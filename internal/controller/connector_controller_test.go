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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafkaconnectv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	kafkaconnect "github.com/b1zzu/kafka-connect-operator/pkg/kafka-connect"
)

// mockKafkaConnectServer provides a stateful mock Kafka Connect REST API.
type mockKafkaConnectServer struct {
	mu                  sync.Mutex
	connectors          map[string]*kafkaconnect.Connector
	restartedConnectors []string
	server              *httptest.Server
}

func newMockKafkaConnectServer() *mockKafkaConnectServer {
	m := &mockKafkaConnectServer{
		connectors: make(map[string]*kafkaconnect.Connector),
	}

	mux := http.NewServeMux()

	const connectorsPath = "connectors"

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()

		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

		// POST /connectors
		if len(parts) == 1 && parts[0] == connectorsPath && r.Method == http.MethodPost {
			var c kafkaconnect.Connector
			if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			m.connectors[c.Name] = &c
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(c)
			return
		}

		// GET /connectors/{name}
		if len(parts) == 2 && parts[0] == connectorsPath && r.Method == http.MethodGet {
			name := parts[1]
			c, ok := m.connectors[name]
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(c)
			return
		}

		// DELETE /connectors/{name}
		if len(parts) == 2 && parts[0] == connectorsPath && r.Method == http.MethodDelete {
			name := parts[1]
			delete(m.connectors, name)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// POST /connectors/{name}/restart
		if len(parts) == 3 && parts[0] == connectorsPath && parts[2] == "restart" && r.Method == http.MethodPost {
			name := parts[1]
			if _, ok := m.connectors[name]; !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			m.restartedConnectors = append(m.restartedConnectors, name)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// GET /connectors/{name}/status
		if len(parts) == 3 && parts[0] == connectorsPath && parts[2] == "status" && r.Method == http.MethodGet {
			name := parts[1]
			if _, ok := m.connectors[name]; !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			status := kafkaconnect.ConnectorStatus{
				Name: name,
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "RUNNING",
					WorkerID: "worker-1",
				},
				Tasks: []kafkaconnect.ConnectorStatusTask{
					{ID: 0, State: "RUNNING", WorkerID: "worker-1"},
				},
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(status)
			return
		}

		http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
	})

	m.server = httptest.NewServer(mux)
	return m
}

func (m *mockKafkaConnectServer) Close() {
	m.server.Close()
}

var _ = Describe("Connector Controller", func() {

	newReconciler := func(mock *mockKafkaConnectServer) *ConnectorReconciler {
		r := &ConnectorReconciler{
			Client:                    k8sClient,
			Scheme:                    k8sClient.Scheme(),
			NewKafkaConnectClientFunc: NewDefaultKafkaConnectClientFunc,
		}
		if mock != nil {
			r.NewKafkaConnectClientFunc = func(connector *kafkaconnectv1alpha1.Connector) *kafkaconnect.Client {
				return kafkaconnect.NewClient(mock.server.URL)
			}
		}
		return r
	}

	nameFor := func(name string) types.NamespacedName {
		return types.NamespacedName{Name: name, Namespace: "default"}
	}

	createConnector := func(ctx context.Context, name string, clusterRef string, config map[string]string, annotations map[string]string) {
		connector := &kafkaconnectv1alpha1.Connector{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   "default",
				Annotations: annotations,
			},
			Spec: kafkaconnectv1alpha1.ConnectorSpec{
				ClusterRef: corev1.LocalObjectReference{Name: clusterRef},
				Config:     config,
			},
		}
		Expect(k8sClient.Create(ctx, connector)).To(Succeed())
	}

	reconcileN := func(ctx context.Context, r *ConnectorReconciler, nn types.NamespacedName, n int) (ctrl.Result, error) {
		var result ctrl.Result
		var err error
		for range n {
			result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			if err != nil {
				return result, err
			}
		}
		return result, nil
	}

	getConnector := func(ctx context.Context, nn types.NamespacedName) *kafkaconnectv1alpha1.Connector {
		connector := &kafkaconnectv1alpha1.Connector{}
		err := k8sClient.Get(ctx, nn, connector)
		if errors.IsNotFound(err) {
			return nil
		}
		Expect(err).NotTo(HaveOccurred())
		return connector
	}

	Context("Reconciliation loop", func() {

		It("should return no error and no requeue for a deleted resource", func() {
			ctx := context.Background()
			r := newReconciler(nil)

			result, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: nameFor("nonexistent-connector"),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})

		It("should initialize status conditions on first reconcile", func() {
			ctx := context.Background()
			name := "init-conditions"
			nn := nameFor(name)

			createConnector(ctx, name, "my-cluster", map[string]string{"connector.class": "FileStreamSource"}, nil)
			DeferCleanup(func() {
				c := getConnector(ctx, nn)
				if c != nil {
					c.Finalizers = nil
					_ = k8sClient.Update(ctx, c)
					_ = k8sClient.Delete(ctx, c)
				}
			})

			r := newReconciler(nil)
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Status.Conditions).To(HaveLen(1))

			cond := meta.FindStatusCondition(connector.Status.Conditions, typeRunningConnector)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal("Reconciling"))
			Expect(cond.ObservedGeneration).To(Equal(connector.Generation))

			// No finalizer yet after first reconcile
			Expect(connector.Finalizers).To(BeEmpty())
		})

		It("should complete the full happy path with connector created and running", func() {
			ctx := context.Background()
			name := "happy-path"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			createConnector(ctx, name, "my-cluster", map[string]string{"connector.class": "FileStreamSource"}, nil)
			DeferCleanup(func() {
				c := getConnector(ctx, nn)
				if c != nil {
					c.Finalizers = nil
					_ = k8sClient.Update(ctx, c)
					_ = k8sClient.Delete(ctx, c)
				}
			})

			r := newReconciler(mock)

			// Reconcile 5 times: init conditions, add finalizer, create connector, update status, full pass
			result, err := reconcileN(ctx, r, nn, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())

			// Finalizer should be present
			Expect(connector.Finalizers).To(ContainElement(connectorFinalizer))

			// Running condition should be True
			cond := meta.FindStatusCondition(connector.Status.Conditions, typeRunningConnector)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal("Running"))
			Expect(cond.ObservedGeneration).To(Equal(connector.Generation))
		})

		It("should delete the connector and remove the finalizer", func() {
			ctx := context.Background()
			name := "delete-connector"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			createConnector(ctx, name, "my-cluster", map[string]string{"connector.class": "FileStreamSource"}, nil)

			r := newReconciler(mock)

			// Reconcile 2x to get conditions initialized and finalizer added
			_, err := reconcileN(ctx, r, nn, 2)
			Expect(err).NotTo(HaveOccurred())

			// Verify finalizer is present
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Finalizers).To(ContainElement(connectorFinalizer))

			// Delete the connector (DeletionTimestamp is set because finalizer blocks)
			Expect(k8sClient.Delete(ctx, connector)).To(Succeed())

			// Reconcile once more to process the deletion
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Connector should be fully deleted
			Expect(getConnector(ctx, nn)).To(BeNil())
		})

		It("should restart the connector when restart annotation is set and remove it", func() {
			ctx := context.Background()
			name := "restart-connector"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			createConnector(ctx, name, "my-cluster", map[string]string{"connector.class": "FileStreamSource"}, map[string]string{
				"kafka-connect.b1zzu.net/restart": "true",
			})
			DeferCleanup(func() {
				c := getConnector(ctx, nn)
				if c != nil {
					c.Finalizers = nil
					_ = k8sClient.Update(ctx, c)
					_ = k8sClient.Delete(ctx, c)
				}
			})

			r := newReconciler(mock)

			// Reconcile to completion: init conditions, add finalizer, create connector,
			// restart (removes annotation + restarts loop), then full pass through status
			result, err := reconcileN(ctx, r, nn, 7)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			// Verify the restart annotation has been removed
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).NotTo(HaveKey("kafka-connect.b1zzu.net/restart"))

			// Verify the connector was restarted via the mock
			mock.mu.Lock()
			defer mock.mu.Unlock()
			Expect(mock.restartedConnectors).To(ContainElement(name))
		})

	})

	Context("getDesiredConnectorState", func() {
		It("should return running when state is not specified", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
				},
			}
			state := getDesiredConnectorState(connector)
			Expect(state).To(Equal(kafkaconnectv1alpha1.ConnectorStateRunning))
		})

		It("should return running when state is running", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
				},
				Spec: kafkaconnectv1alpha1.ConnectorSpec{
					State: kafkaconnectv1alpha1.ConnectorStateRunning,
				},
			}
			state := getDesiredConnectorState(connector)
			Expect(state).To(Equal(kafkaconnectv1alpha1.ConnectorStateRunning))
		})

		It("should return paused when state is paused", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
				},
				Spec: kafkaconnectv1alpha1.ConnectorSpec{
					State: kafkaconnectv1alpha1.ConnectorStatePaused,
				},
			}
			state := getDesiredConnectorState(connector)
			Expect(state).To(Equal(kafkaconnectv1alpha1.ConnectorStatePaused))
		})

		It("should return stopped when state is stopped", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
				},
				Spec: kafkaconnectv1alpha1.ConnectorSpec{
					State: kafkaconnectv1alpha1.ConnectorStateStopped,
				},
			}
			state := getDesiredConnectorState(connector)
			Expect(state).To(Equal(kafkaconnectv1alpha1.ConnectorStateStopped))
		})
	})

	Context("mapConnectorStatusToCondition", func() {
		It("should map RUNNING state to Running condition", func() {
			status := &kafkaconnect.ConnectorStatus{
				Name: "test",
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "RUNNING",
					WorkerID: "worker-1",
				},
				Tasks: []kafkaconnect.ConnectorStatusTask{
					{ID: 0, State: "RUNNING", WorkerID: "worker-1"},
				},
			}
			condition := mapConnectorStatusToCondition(status)
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal("Running"))
			Expect(condition.Message).To(Equal("Connector is running with 1 task(s)"))
		})

		It("should map RUNNING state with failed tasks to Failed condition", func() {
			status := &kafkaconnect.ConnectorStatus{
				Name: "test",
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "RUNNING",
					WorkerID: "worker-1",
				},
				Tasks: []kafkaconnect.ConnectorStatusTask{
					{ID: 0, State: "RUNNING", WorkerID: "worker-1"},
					{ID: 1, State: "FAILED", WorkerID: "worker-2", Trace: "some error"},
				},
			}
			condition := mapConnectorStatusToCondition(status)
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Failed"))
			Expect(condition.Message).To(ContainSubstring("1 failed task(s) out of 2"))
		})

		It("should map STOPPED state to Stopped condition", func() {
			status := &kafkaconnect.ConnectorStatus{
				Name: "test",
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "STOPPED",
					WorkerID: "worker-1",
				},
			}
			condition := mapConnectorStatusToCondition(status)
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Stopped"))
			Expect(condition.Message).To(Equal("Connector is stopped"))
		})

		It("should map PAUSED state to Paused condition", func() {
			status := &kafkaconnect.ConnectorStatus{
				Name: "test",
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "PAUSED",
					WorkerID: "worker-1",
				},
			}
			condition := mapConnectorStatusToCondition(status)
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Paused"))
			Expect(condition.Message).To(Equal("Connector is paused"))
		})

		It("should map FAILED state to Failed condition with trace", func() {
			status := &kafkaconnect.ConnectorStatus{
				Name: "test",
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "FAILED",
					WorkerID: "worker-1",
					Trace:    "org.apache.kafka.connect.errors.ConnectException: error",
				},
			}
			condition := mapConnectorStatusToCondition(status)
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Failed"))
			Expect(condition.Message).To(ContainSubstring("Connector failed with trace"))
		})

		It("should map unknown state to Unknown condition", func() {
			status := &kafkaconnect.ConnectorStatus{
				Name: "test",
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "UNASSIGNED",
					WorkerID: "",
				},
			}
			condition := mapConnectorStatusToCondition(status)
			Expect(condition.Status).To(Equal(metav1.ConditionUnknown))
			Expect(condition.Reason).To(Equal("Unknown"))
			Expect(condition.Message).To(ContainSubstring("UNASSIGNED"))
		})
	})

	Context("status field population", func() {
		It("should correctly map Kafka Connect status to API status types", func() {
			kcStatus := &kafkaconnect.ConnectorStatus{
				Name: "test-connector",
				Connector: kafkaconnect.ConnectorStatusConnector{
					State:    "RUNNING",
					WorkerID: "worker-1:8083",
				},
				Tasks: []kafkaconnect.ConnectorStatusTask{
					{ID: 0, State: "RUNNING", WorkerID: "worker-1:8083"},
					{ID: 1, State: "FAILED", WorkerID: "worker-2:8083", Trace: "some stack trace"},
				},
			}

			connectorStatus := &kafkaconnectv1alpha1.ConnectorStateStatus{
				State:    kcStatus.Connector.State,
				WorkerID: kcStatus.Connector.WorkerID,
				Trace:    kcStatus.Connector.Trace,
			}

			tasks := make([]kafkaconnectv1alpha1.TaskStateStatus, len(kcStatus.Tasks))
			for i, task := range kcStatus.Tasks {
				tasks[i] = kafkaconnectv1alpha1.TaskStateStatus{
					ID:       task.ID,
					State:    task.State,
					WorkerID: task.WorkerID,
					Trace:    task.Trace,
				}
			}

			Expect(connectorStatus.State).To(Equal("RUNNING"))
			Expect(connectorStatus.WorkerID).To(Equal("worker-1:8083"))
			Expect(connectorStatus.Trace).To(BeEmpty())

			Expect(tasks).To(HaveLen(2))
			Expect(tasks[0].ID).To(Equal(0))
			Expect(tasks[0].State).To(Equal("RUNNING"))
			Expect(tasks[0].WorkerID).To(Equal("worker-1:8083"))
			Expect(tasks[0].Trace).To(BeEmpty())
			Expect(tasks[1].ID).To(Equal(1))
			Expect(tasks[1].State).To(Equal("FAILED"))
			Expect(tasks[1].WorkerID).To(Equal("worker-2:8083"))
			Expect(tasks[1].Trace).To(Equal("some stack trace"))
		})
	})
})
