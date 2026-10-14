/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
)

const (
	metricsRestartTriggerManual = "manual"
	metricsRestartTriggerAuto   = "auto"

	// metricsListTimeout bounds the cache lists done on every scrape.
	metricsListTimeout = 5 * time.Second
)

const (
	metricsLabelNamespace = "resource_namespace"
	metricsLabelName      = "name"
	metricsLabelCluster   = "cluster"
	metricsStateFailed    = "failed"
	metricsStateRunning   = "running"
	metricsStatusTrue     = "true"
	metricsStatusFalse    = "false"
	metricsStatusUnknown  = "unknown"
)

var (
	connectorRestartsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_connect_operator_connector_restarts_total",
		Help: "Restart actions performed by the operator, by trigger (manual annotation or automatic restart of failed connectors and tasks). " +
			"One automatic action may restart the connector and several tasks.",
	}, []string{metricsLabelNamespace, metricsLabelName, metricsLabelCluster, "trigger"})

	kafkaConnectRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_connect_operator_kafka_connect_requests_total",
		Help: "Requests sent by the operator to the Kafka Connect REST API. The code is \"error\" when no response was received.",
	}, []string{metricsLabelNamespace, metricsLabelCluster, "method", "code"})

	kafkaConnectRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_connect_operator_kafka_connect_request_duration_seconds",
		Help:    "Latency of the requests sent by the operator to the Kafka Connect REST API.",
		Buckets: prometheus.DefBuckets,
		// Native histogram settings, the classic buckets are kept for older Prometheus servers.
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{metricsLabelNamespace, metricsLabelCluster, "method"})

	connectorReadyDesc = prometheus.NewDesc(
		"kafka_connect_operator_connector_ready",
		"1 for the status (true, false, unknown) matching the Connector Ready condition, 0 for the others.",
		[]string{metricsLabelNamespace, metricsLabelName, metricsLabelCluster, "status"}, nil)
	connectorStateDesc = prometheus.NewDesc(
		"kafka_connect_operator_connector_state",
		"1 for the connector state reported by Kafka Connect, 0 for the other states.",
		[]string{metricsLabelNamespace, metricsLabelName, metricsLabelCluster, "state"}, nil)
	connectorTasksDesc = prometheus.NewDesc(
		"kafka_connect_operator_connector_tasks",
		"Number of connector tasks by state.",
		[]string{metricsLabelNamespace, metricsLabelName, metricsLabelCluster, "state"}, nil)
	clusterAvailableDesc = prometheus.NewDesc(
		"kafka_connect_operator_cluster_available",
		"1 if the Cluster Available condition is True, 0 otherwise.",
		[]string{metricsLabelNamespace, metricsLabelName}, nil)
)

// Known states, always exported (0 when not matching) so series do not appear and
// disappear as the states change. Kafka Connect task states have no stopped state.
var (
	metricsConnectorStates = []string{metricsStateRunning, "paused", "stopped", metricsStateFailed, "unassigned", "restarting"}
	metricsTaskStates      = []string{metricsStateRunning, "paused", metricsStateFailed, "unassigned", "restarting"}
	metricsReadyStatuses   = []string{metricsStatusTrue, metricsStatusFalse, metricsStatusUnknown}
)

func init() {
	metrics.Registry.MustRegister(connectorRestartsTotal, kafkaConnectRequestsTotal, kafkaConnectRequestDuration)
}

// connectorRestarts returns the restart counter of the connector, creating it at 0 if missing.
func connectorRestarts(connector *kcv1alpha1.Connector, trigger string) prometheus.Counter {
	return connectorRestartsTotal.WithLabelValues(connector.Namespace, connector.Name, connector.Spec.ClusterRef.Name, trigger)
}

// instrumentedTransport records metrics for every request sent to a Kafka Connect cluster.
type instrumentedTransport struct {
	namespace, cluster string
	next               http.RoundTripper
}

func (t *instrumentedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}

	start := time.Now()
	res, err := next.RoundTrip(req)

	// The response code is "error" when the cluster could not be reached at all.
	code := "error"
	if err == nil {
		code = strconv.Itoa(res.StatusCode)
	}
	kafkaConnectRequestsTotal.WithLabelValues(t.namespace, t.cluster, req.Method, code).Inc()
	kafkaConnectRequestDuration.WithLabelValues(t.namespace, t.cluster, req.Method).Observe(time.Since(start).Seconds())

	return res, err
}

// stateCollector exposes the status of the Cluster and Connector resources.
// The metrics are built from the cache on every scrape, so nothing has to be
// cleaned up when a resource is deleted.
type stateCollector struct {
	reader client.Reader
}

// NewStateCollector returns a collector to register on the controller-runtime metrics registry.
func NewStateCollector(reader client.Reader) prometheus.Collector {
	return &stateCollector{reader: reader}
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- connectorReadyDesc
	ch <- connectorStateDesc
	ch <- connectorTasksDesc
	ch <- clusterAvailableDesc
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), metricsListTimeout)
	defer cancel()
	log := logf.Log.WithName("metrics")

	clusters := &kcv1alpha1.ClusterList{}
	if err := c.reader.List(ctx, clusters); err != nil {
		log.Error(err, "Failed to list Clusters")
	}
	for _, cluster := range clusters.Items {
		ch <- prometheus.MustNewConstMetric(clusterAvailableDesc, prometheus.GaugeValue,
			conditionValue(cluster.Status.Conditions, typeAvailableCluster), cluster.Namespace, cluster.Name)
	}

	connectors := &kcv1alpha1.ConnectorList{}
	if err := c.reader.List(ctx, connectors); err != nil {
		log.Error(err, "Failed to list Connectors")
	}
	for _, connector := range connectors.Items {
		ns, name, cluster := connector.Namespace, connector.Name, connector.Spec.ClusterRef.Name

		// A missing Ready condition is reported as unknown
		ready := metricsStatusUnknown
		if cond := meta.FindStatusCondition(connector.Status.Conditions, typeReadyConnector); cond != nil {
			ready = strings.ToLower(string(cond.Status))
		}
		for _, status := range metricsReadyStatuses {
			ch <- prometheus.MustNewConstMetric(connectorReadyDesc, prometheus.GaugeValue,
				boolValue(status == ready), ns, name, cluster, status)
		}

		// status.connector is nil until the first status sync: every state is 0
		actual := ""
		if connector.Status.Connector != nil {
			actual = strings.ToLower(connector.Status.Connector.State)
		}
		for _, state := range metricsConnectorStates {
			ch <- prometheus.MustNewConstMetric(connectorStateDesc, prometheus.GaugeValue,
				boolValue(state == actual), ns, name, cluster, state)
		}

		tasks := map[string]float64{}
		for _, task := range connector.Status.Tasks {
			tasks[strings.ToLower(task.State)]++
		}
		for _, state := range metricsTaskStates {
			ch <- prometheus.MustNewConstMetric(connectorTasksDesc, prometheus.GaugeValue, tasks[state], ns, name, cluster, state)
		}
	}
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// conditionValue returns 1 if the condition is True, 0 if it is missing or not True.
func conditionValue(conditions []metav1.Condition, conditionType string) float64 {
	return boolValue(meta.IsStatusConditionTrue(conditions, conditionType))
}
