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
	mu         sync.Mutex
	connectors map[string]*kafkaconnect.Connector
	server     *httptest.Server
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

		It("should return an error for an invalid state annotation", func() {
			ctx := context.Background()
			name := "invalid-state"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			createConnector(ctx, name, "my-cluster",
				map[string]string{"connector.class": "FileStreamSource"},
				map[string]string{connectorStateAnnotation: "invalid"},
			)
			DeferCleanup(func() {
				c := getConnector(ctx, nn)
				if c != nil {
					c.Finalizers = nil
					_ = k8sClient.Update(ctx, c)
					_ = k8sClient.Delete(ctx, c)
				}
			})

			r := newReconciler(mock)

			// Reconcile 2x: init conditions, add finalizer
			_, err := reconcileN(ctx, r, nn, 2)
			Expect(err).NotTo(HaveOccurred())

			// Third reconcile: GET /connectors/{name} returns 404, then tries to create
			// with invalid state -> should return an error
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid connector state"))

			// Verify the status condition reflects the error
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			cond := meta.FindStatusCondition(connector.Status.Conditions, typeRunningConnector)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("Error"))
			Expect(cond.Message).To(ContainSubstring("invalid connector state"))
		})
	})

	Context("getDesiredConnectorState", func() {
		It("should return running when annotation is absent", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
				},
			}
			state, err := getDesiredConnectorState(connector)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal("running"))
		})

		It("should return running when annotations map exists but state annotation is absent", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
					Annotations: map[string]string{
						"other-annotation": "value",
					},
				},
			}
			state, err := getDesiredConnectorState(connector)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal("running"))
		})

		It("should return paused when annotation is paused", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
					Annotations: map[string]string{
						connectorStateAnnotation: "paused",
					},
				},
			}
			state, err := getDesiredConnectorState(connector)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal("paused"))
		})

		It("should return stopped when annotation is stopped", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
					Annotations: map[string]string{
						connectorStateAnnotation: "stopped",
					},
				},
			}
			state, err := getDesiredConnectorState(connector)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal("stopped"))
		})

		It("should return running when annotation is running", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
					Annotations: map[string]string{
						connectorStateAnnotation: "running",
					},
				},
			}
			state, err := getDesiredConnectorState(connector)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(Equal("running"))
		})

		It("should return error for invalid annotation value", func() {
			connector := &kafkaconnectv1alpha1.Connector{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
					Annotations: map[string]string{
						connectorStateAnnotation: "invalid",
					},
				},
			}
			_, err := getDesiredConnectorState(connector)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid connector state annotation value"))
		})
	})

	Context("mapConnectorStatusToCondition", func() {
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
	})
})
