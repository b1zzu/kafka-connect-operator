package controller

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
)

func TestInstrumentedTransport(t *testing.T) {
	kafkaConnectRequestsTotal.Reset()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	rt := &instrumentedTransport{namespace: "ns", cluster: "c"}

	res, err := (&http.Client{Transport: rt}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	server.Close()
	if _, err := (&http.Client{Transport: rt}).Get(server.URL); err == nil {
		t.Fatal("expected an error from the closed server")
	}

	if got := testutil.ToFloat64(kafkaConnectRequestsTotal.WithLabelValues("ns", "c", "GET", "500")); got != 1 {
		t.Errorf("code 500 requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(kafkaConnectRequestsTotal.WithLabelValues("ns", "c", "GET", "error")); got != 1 {
		t.Errorf("transport error requests = %v, want 1", got)
	}
}

func TestStateCollector(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := kcv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	connector := &kcv1alpha1.Connector{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "conn"},
		Spec:       kcv1alpha1.ConnectorSpec{State: kcv1alpha1.ConnectorStateRunning},
	}
	connector.Spec.ClusterRef.Name = "c"
	connector.Status.Conditions = []metav1.Condition{{Type: typeReadyConnector, Status: metav1.ConditionFalse, Reason: "FailedTasks"}}
	connector.Status.Connector = &kcv1alpha1.ConnectorStateStatus{State: connectorStatusRunning}
	connector.Status.Tasks = []kcv1alpha1.TaskStateStatus{{ID: 0, State: "RUNNING"}, {ID: 1, State: "FAILED"}, {ID: 2, State: "RUNNING"}}
	cluster := &kcv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c"},
		Status:     kcv1alpha1.ClusterStatus{Conditions: []metav1.Condition{{Type: typeAvailableCluster, Status: metav1.ConditionTrue}}},
	}

	// A connector that was just created: no conditions, no status
	bare := &kcv1alpha1.Connector{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "bare"}}
	bare.Spec.ClusterRef.Name = "c"

	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(connector, bare, cluster).Build()

	// Every known state is always exported, matching or not
	var want strings.Builder
	want.WriteString(`
# HELP kafka_connect_operator_cluster_available 1 if the Cluster Available condition is True, 0 otherwise.
# TYPE kafka_connect_operator_cluster_available gauge
kafka_connect_operator_cluster_available{name="c",resource_namespace="ns"} 1
`)
	series := func(metric, name, label string, values map[string]float64, all []string) {
		for _, v := range all {
			fmt.Fprintf(&want, "%s{cluster=\"c\",name=\"%s\",resource_namespace=\"ns\",%s=\"%s\"} %v\n", metric, name, label, v, values[v])
		}
	}
	want.WriteString("# HELP kafka_connect_operator_connector_ready 1 for the status (true, false, unknown) matching the Connector Ready condition, 0 for the others.\n")
	want.WriteString("# TYPE kafka_connect_operator_connector_ready gauge\n")
	series("kafka_connect_operator_connector_ready", "bare", "status", map[string]float64{"unknown": 1}, metricsReadyStatuses)
	series("kafka_connect_operator_connector_ready", "conn", "status", map[string]float64{"false": 1}, metricsReadyStatuses)
	want.WriteString("# HELP kafka_connect_operator_connector_state 1 for the connector state reported by Kafka Connect, 0 for the other states.\n")
	want.WriteString("# TYPE kafka_connect_operator_connector_state gauge\n")
	series("kafka_connect_operator_connector_state", "bare", "state", nil, metricsConnectorStates)
	series("kafka_connect_operator_connector_state", "conn", "state", map[string]float64{"running": 1}, metricsConnectorStates)
	want.WriteString("# HELP kafka_connect_operator_connector_tasks Number of connector tasks by state.\n")
	want.WriteString("# TYPE kafka_connect_operator_connector_tasks gauge\n")
	series("kafka_connect_operator_connector_tasks", "bare", "state", nil, metricsTaskStates)
	series("kafka_connect_operator_connector_tasks", "conn", "state", map[string]float64{"running": 2, "failed": 1}, metricsTaskStates)

	if err := testutil.CollectAndCompare(NewStateCollector(reader), strings.NewReader(want.String()),
		"kafka_connect_operator_cluster_available", "kafka_connect_operator_connector_ready",
		"kafka_connect_operator_connector_state", "kafka_connect_operator_connector_tasks"); err != nil {
		t.Error(err)
	}
}

func TestConnectorRestartsExportedAtZero(t *testing.T) {
	connectorRestartsTotal.Reset()
	connector := &kcv1alpha1.Connector{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "conn"}}
	connector.Spec.ClusterRef.Name = "c"

	connectorRestarts(connector, metricsRestartTriggerManual)
	connectorRestarts(connector, metricsRestartTriggerAuto).Inc()

	if got := testutil.CollectAndCount(connectorRestartsTotal); got != 2 {
		t.Errorf("exported series = %d, want 2", got)
	}
	if got := testutil.ToFloat64(connectorRestarts(connector, metricsRestartTriggerManual)); got != 0 {
		t.Errorf("manual restarts = %v, want 0", got)
	}
	if got := testutil.ToFloat64(connectorRestarts(connector, metricsRestartTriggerAuto)); got != 1 {
		t.Errorf("auto restarts = %v, want 1", got)
	}
}
