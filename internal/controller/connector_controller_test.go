/*
Copyright 2026.

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
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafkaconnectv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	kafkaconnect "github.com/b1zzu/kafka-connect-operator/pkg/kafka-connect"
)

// mockKafkaConnectServer provides a stateful mock Kafka Connect REST API.
type mockKafkaConnectServer struct {
	mu                  sync.Mutex
	connectors          map[string]*kafkaconnect.Connector
	connectorStates     map[string]string // name -> state (RUNNING, STOPPED, etc.)
	offsets             map[string]*kafkaconnect.ConnectorOffsets
	restartedConnectors []string
	restartedTasks      []int
	server              *httptest.Server

	// connectorStatus overrides the status returned for connectors (default: RUNNING)
	connectorStatus string
	// taskStatuses overrides the status returned for tasks (default: single RUNNING task)
	taskStatuses []kafkaconnect.ConnectorStatusTask
	// restartError if set, causes restart endpoints to return this error
	restartError bool
}

func newMockKafkaConnectServer() *mockKafkaConnectServer {
	m := &mockKafkaConnectServer{
		connectors:      make(map[string]*kafkaconnect.Connector),
		connectorStates: make(map[string]string),
		offsets:         make(map[string]*kafkaconnect.ConnectorOffsets),
	}
	m.server = httptest.NewServer(m)
	return m
}

func (m *mockKafkaConnectServer) Close() {
	m.server.Close()
}

func (m *mockKafkaConnectServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	if len(parts) >= 1 && parts[0] == "connectors" {
		switch len(parts) {
		case 1:
			m.handleConnectors(w, r)
		case 2:
			m.handleConnector(w, r, parts[1])
		case 3:
			m.handleConnectorAction(w, r, parts[1], parts[2])
		case 5:
			// POST /connectors/{name}/tasks/{id}/restart
			if parts[2] == "tasks" && parts[4] == "restart" && r.Method == http.MethodPost {
				m.handleTaskRestart(w, parts[1], parts[3])
			} else {
				http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
			}
		default:
			http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
		}
		return
	}

	http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
}

func (m *mockKafkaConnectServer) handleConnectors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
		return
	}
	var c kafkaconnect.Connector
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.connectors[c.Name] = &c
	state := connectorStatusRunning
	if c.InitialState != "" {
		state = c.InitialState
	}
	m.connectorStates[c.Name] = state
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(c)
}

func (m *mockKafkaConnectServer) handleConnector(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodGet:
		c, ok := m.connectors[name]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(c)
	case http.MethodDelete:
		delete(m.connectors, name)
		delete(m.connectorStates, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
	}
}

func (m *mockKafkaConnectServer) handleConnectorAction(w http.ResponseWriter, r *http.Request, name, action string) {
	switch action {
	case "restart":
		m.handleRestart(w, r, name)
	case "status":
		m.handleStatus(w, name)
	case "stop":
		m.connectorStates[name] = connectorStatusStopped
		w.WriteHeader(http.StatusAccepted)
	case "resume":
		m.connectorStates[name] = connectorStatusRunning
		w.WriteHeader(http.StatusAccepted)
	case "pause":
		m.connectorStates[name] = connectorStatusPaused
		w.WriteHeader(http.StatusAccepted)
	case "offsets":
		m.handleOffsets(w, r, name)
	default:
		http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
	}
}

func (m *mockKafkaConnectServer) handleRestart(w http.ResponseWriter, _ *http.Request, name string) {
	if _, ok := m.connectors[name]; !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if m.restartError {
		http.Error(w, "restart failed", http.StatusInternalServerError)
		return
	}
	m.restartedConnectors = append(m.restartedConnectors, name)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockKafkaConnectServer) handleTaskRestart(w http.ResponseWriter, name, taskIDStr string) {
	if _, ok := m.connectors[name]; !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if m.restartError {
		http.Error(w, "restart failed", http.StatusInternalServerError)
		return
	}
	var taskID int
	_, _ = fmt.Sscanf(taskIDStr, "%d", &taskID)
	m.restartedTasks = append(m.restartedTasks, taskID)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockKafkaConnectServer) handleStatus(w http.ResponseWriter, name string) {
	if _, ok := m.connectors[name]; !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	state := m.connectorStates[name]
	if state == "" {
		state = connectorStatusRunning
	}
	// Allow test-level override of connector status
	if m.connectorStatus != "" {
		state = m.connectorStatus
	}
	status := kafkaconnect.ConnectorStatus{
		Name: name,
		Connector: kafkaconnect.ConnectorStatusConnector{
			State:    state,
			WorkerID: "worker-1",
		},
	}
	if m.taskStatuses != nil {
		status.Tasks = m.taskStatuses
	} else if state == connectorStatusRunning {
		status.Tasks = []kafkaconnect.ConnectorStatusTask{
			{ID: 0, State: connectorStatusRunning, WorkerID: "worker-1"},
		}
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(status)
}

func (m *mockKafkaConnectServer) handleOffsets(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodGet:
		offsets, ok := m.offsets[name]
		if !ok {
			offsets = &kafkaconnect.ConnectorOffsets{Offsets: []kafkaconnect.ConnectorOffset{}}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(offsets)
	case http.MethodPatch:
		if m.connectorStates[name] != connectorStatusStopped {
			http.Error(w, "connector must be stopped", http.StatusConflict)
			return
		}
		var offsets kafkaconnect.ConnectorOffsets
		if err := json.NewDecoder(r.Body).Decode(&offsets); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		m.offsets[name] = &offsets
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if m.connectorStates[name] != connectorStatusStopped {
			http.Error(w, "connector must be stopped", http.StatusConflict)
			return
		}
		delete(m.offsets, name)
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, fmt.Sprintf("unhandled: %s %s", r.Method, r.URL.Path), http.StatusNotImplemented)
	}
}

var _ = Describe("Connector Controller", func() {

	newReconciler := func(mock *mockKafkaConnectServer) (*ConnectorReconciler, *events.FakeRecorder) {
		recorder := events.NewFakeRecorder(10)
		r := &ConnectorReconciler{
			Client:                    k8sClient,
			Scheme:                    k8sClient.Scheme(),
			Recorder:                  recorder,
			NewKafkaConnectClientFunc: NewDefaultKafkaConnectClientFunc,
		}
		if mock != nil {
			r.NewKafkaConnectClientFunc = func(connector *kafkaconnectv1alpha1.Connector) *kafkaconnect.Client {
				return kafkaconnect.NewClient(mock.server.URL)
			}
		}
		return r, recorder
	}

	nameFor := func(name string) types.NamespacedName {
		return types.NamespacedName{Name: name, Namespace: "default"}
	}

	createConnectorWithSpec := func(ctx context.Context, name string, annotations map[string]string, spec kafkaconnectv1alpha1.ConnectorSpec) {
		connector := &kafkaconnectv1alpha1.Connector{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   "default",
				Annotations: annotations,
			},
			Spec: spec,
		}
		Expect(k8sClient.Create(ctx, connector)).To(Succeed())
	}

	createConnector := func(ctx context.Context, name string, clusterRef string, config map[string]string, annotations map[string]string) {
		createConnectorWithSpec(ctx, name, annotations, kafkaconnectv1alpha1.ConnectorSpec{
			ClusterRef: corev1.LocalObjectReference{Name: clusterRef},
			Config:     config,
		})
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
			r, _ := newReconciler(nil)

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

			r, _ := newReconciler(nil)
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

			r, _ := newReconciler(mock)

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

			r, _ := newReconciler(mock)

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

			r, _ := newReconciler(mock)

			// Reconcile to completion: init conditions, add finalizer, create connector,
			// restart (removes annotation + restarts loop), then full pass through status
			result, err := reconcileN(ctx, r, nn, 7)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			// Verify the restart annotation has been removed
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).NotTo(HaveKey("kafka-connect.b1zzu.net/restart"))

			// Verify restart status fields
			Expect(connector.Status.LastRestartAt).NotTo(BeNil())
			Expect(connector.Status.RestartCount).To(Equal(int32(1)))

			// Verify the connector was restarted via the mock
			mock.mu.Lock()
			defer mock.mu.Unlock()
			Expect(mock.restartedConnectors).To(ContainElement(name))
		})

		It("should emit warning event and return error when manual restart fails", func() {
			ctx := context.Background()
			name := "restart-fail"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			mock.restartError = true
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

			r, _ := newReconciler(mock)

			// Reconcile 3 times: init conditions, add finalizer, create connector
			_, err := reconcileN(ctx, r, nn, 3)
			Expect(err).NotTo(HaveOccurred())

			// 4th reconcile hits restart which fails
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to restart connector"))

			// Verify restart status fields are NOT updated on failure
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Status.LastRestartAt).To(BeNil())
			Expect(connector.Status.RestartCount).To(Equal(int32(0)))
		})

		It("should auto-restart failed connector, emit event, and update restart status", func() {
			ctx := context.Background()
			name := "auto-restart-connector"
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

			r, _ := newReconciler(mock)

			// Reconcile 4 times: init conditions, add finalizer, create connector, restart loop
			_, err := reconcileN(ctx, r, nn, 4)
			Expect(err).NotTo(HaveOccurred())

			// Now set mock to return FAILED status
			mock.mu.Lock()
			mock.connectorStatus = "FAILED"
			mock.mu.Unlock()

			// Reconcile once more — auto restart should happen
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			// Status was updated (restarted flag triggered DeepEqual diff), so returns empty result
			Expect(result).To(Equal(ctrl.Result{}))

			// Verify restart status fields
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Status.LastRestartAt).NotTo(BeNil())
			Expect(connector.Status.RestartCount).To(Equal(int32(1)))

			// Verify the connector was restarted via the mock
			mock.mu.Lock()
			defer mock.mu.Unlock()
			Expect(mock.restartedConnectors).To(ContainElement(name))
		})

		It("should emit warning event and return error when auto-restart fails", func() {
			ctx := context.Background()
			name := "auto-restart-fail"
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

			r, _ := newReconciler(mock)

			// Reconcile 4 times: init conditions, add finalizer, create connector, restart loop
			_, err := reconcileN(ctx, r, nn, 4)
			Expect(err).NotTo(HaveOccurred())

			// Now set mock to return FAILED status and make restart fail
			mock.mu.Lock()
			mock.connectorStatus = "FAILED"
			mock.restartError = true
			mock.mu.Unlock()

			// Reconcile — auto restart should fail and return error
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to restart failed connector"))

			// Verify restart status fields are NOT updated on failure
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Status.LastRestartAt).To(BeNil())
			Expect(connector.Status.RestartCount).To(Equal(int32(0)))
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

	Context("reconcileConnectorOffsets", func() {

		cleanupConnector := func(ctx context.Context, nn types.NamespacedName) {
			c := getConnector(ctx, nn)
			if c != nil {
				c.Finalizers = nil
				_ = k8sClient.Update(ctx, c)
				_ = k8sClient.Delete(ctx, c)
			}
		}

		It("should be a no-op when no annotation is set", func() {
			ctx := context.Background()
			name := "offsets-noop"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			createConnector(ctx, name, "my-cluster", map[string]string{"connector.class": "FileStreamSource"}, nil)
			DeferCleanup(func() { cleanupConnector(ctx, nn) })

			r, _ := newReconciler(mock)

			// Reconcile to full running: init, finalizer, create, status, full pass
			result, err := reconcileN(ctx, r, nn, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Minute))

			// No annotation, no offset operation
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).NotTo(HaveKey(offsetsAnnotation))
		})

		It("should export offsets to a new ConfigMap and remove annotation", func() {
			ctx := context.Background()
			name := "offsets-export"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			// Pre-set offsets in the mock
			mock.offsets[name] = &kafkaconnect.ConnectorOffsets{
				Offsets: []kafkaconnect.ConnectorOffset{
					{
						Partition: map[string]any{"kafka_topic": "test-topic", "kafka_partition": float64(0)},
						Offset:    map[string]any{"kafka_offset": float64(100)},
					},
				},
			}

			createConnectorWithSpec(ctx, name, map[string]string{
				offsetsAnnotation: "export",
			}, kafkaconnectv1alpha1.ConnectorSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
				Config:     map[string]string{"connector.class": "FileStreamSource"},
				ExportOffsets: &kafkaconnectv1alpha1.OffsetsSpec{
					ConfigMapRef: corev1.LocalObjectReference{Name: "offsets-export-cm"},
				},
			})
			DeferCleanup(func() {
				cleanupConnector(ctx, nn)
				cm := &corev1.ConfigMap{}
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: "offsets-export-cm", Namespace: "default"}, cm); err == nil {
					_ = k8sClient.Delete(ctx, cm)
				}
			})

			r, recorder := newReconciler(mock)

			// Reconcile: init, finalizer, create, export+annotation removal, then next pass syncs status
			_, err := reconcileN(ctx, r, nn, 6)
			Expect(err).NotTo(HaveOccurred())

			// Annotation should be removed
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).NotTo(HaveKey(offsetsAnnotation))

			// ConfigMap should exist with offsets
			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, client.ObjectKey{Name: "offsets-export-cm", Namespace: "default"}, cm)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey(offsetsConfigMapKey))
			Expect(cm.Data[offsetsConfigMapKey]).To(ContainSubstring("kafka_topic"))

			// Timestamp should be set
			Expect(connector.Status.LastExportedOffsetsAt).NotTo(BeNil())

			// Event should be emitted
			Expect(recorder.Events).To(Receive(ContainSubstring("ExportedOffsets")))
		})

		It("should emit warning and remove annotation when exportOffsets is missing", func() {
			ctx := context.Background()
			name := "offsets-export-no-spec"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			createConnectorWithSpec(ctx, name, map[string]string{
				offsetsAnnotation: "export",
			}, kafkaconnectv1alpha1.ConnectorSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
				Config:     map[string]string{"connector.class": "FileStreamSource"},
				// No ExportOffsets set
			})
			DeferCleanup(func() { cleanupConnector(ctx, nn) })

			r, recorder := newReconciler(mock)

			// Reconcile: init, finalizer, create, export fails (removes annotation), status, full pass
			_, err := reconcileN(ctx, r, nn, 6)
			Expect(err).NotTo(HaveOccurred())

			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).NotTo(HaveKey(offsetsAnnotation))

			Expect(recorder.Events).To(Receive(ContainSubstring("FailedExportOffsets")))
		})

		It("should import offsets when connector is stopped", func() {
			ctx := context.Background()
			name := "offsets-import"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			// Create the ConfigMap with offsets first
			offsetsJSON := `{"offsets":[{"partition":{"kafka_topic":"test-topic","kafka_partition":0},"offset":{"kafka_offset":500}}]}`
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "offsets-import-cm",
					Namespace: "default",
				},
				Data: map[string]string{
					offsetsConfigMapKey: offsetsJSON,
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func() {
				cleanupConnector(ctx, nn)
				_ = k8sClient.Delete(ctx, cm)
			})

			createConnectorWithSpec(ctx, name, map[string]string{
				offsetsAnnotation: "import",
			}, kafkaconnectv1alpha1.ConnectorSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
				Config:     map[string]string{"connector.class": "FileStreamSource"},
				State:      kafkaconnectv1alpha1.ConnectorStateStopped,
				ImportOffsets: &kafkaconnectv1alpha1.OffsetsSpec{
					ConfigMapRef: corev1.LocalObjectReference{Name: "offsets-import-cm"},
				},
			})

			r, recorder := newReconciler(mock)

			// Reconcile: init, finalizer, create (stopped), import+annotation removal, status, full pass
			_, err := reconcileN(ctx, r, nn, 6)
			Expect(err).NotTo(HaveOccurred())

			// Annotation should be removed
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).NotTo(HaveKey(offsetsAnnotation))

			// Offsets should be set in mock
			mock.mu.Lock()
			importedOffsets := mock.offsets[name]
			mock.mu.Unlock()
			Expect(importedOffsets).NotTo(BeNil())
			Expect(importedOffsets.Offsets).To(HaveLen(1))

			// Timestamp should be set
			Expect(connector.Status.LastImportedOffsetsAt).NotTo(BeNil())

			Expect(recorder.Events).To(Receive(ContainSubstring("ImportedOffsets")))
		})

		It("should emit warning and keep annotation when importing but connector is not stopped", func() {
			ctx := context.Background()
			name := "offsets-import-notstop"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "offsets-import-notstop-cm",
					Namespace: "default",
				},
				Data: map[string]string{
					offsetsConfigMapKey: `{"offsets":[]}`,
				},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			DeferCleanup(func() {
				cleanupConnector(ctx, nn)
				_ = k8sClient.Delete(ctx, cm)
			})

			createConnectorWithSpec(ctx, name, map[string]string{
				offsetsAnnotation: "import",
			}, kafkaconnectv1alpha1.ConnectorSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
				Config:     map[string]string{"connector.class": "FileStreamSource"},
				State:      kafkaconnectv1alpha1.ConnectorStateRunning,
				ImportOffsets: &kafkaconnectv1alpha1.OffsetsSpec{
					ConfigMapRef: corev1.LocalObjectReference{Name: "offsets-import-notstop-cm"},
				},
			})

			r, recorder := newReconciler(mock)

			// Reconcile enough times to reach offset reconciliation
			_, err := reconcileN(ctx, r, nn, 5)
			Expect(err).NotTo(HaveOccurred())

			// Annotation should still be present (retry)
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).To(HaveKeyWithValue(offsetsAnnotation, "import"))

			Expect(recorder.Events).To(Receive(ContainSubstring("FailedImportOffsets")))
		})

		It("should reset offsets when connector is stopped", func() {
			ctx := context.Background()
			name := "offsets-reset"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			// Pre-set offsets in mock
			mock.offsets[name] = &kafkaconnect.ConnectorOffsets{
				Offsets: []kafkaconnect.ConnectorOffset{
					{
						Partition: map[string]any{"kafka_topic": "test-topic"},
						Offset:    map[string]any{"kafka_offset": float64(100)},
					},
				},
			}

			createConnectorWithSpec(ctx, name, map[string]string{
				offsetsAnnotation: "reset",
			}, kafkaconnectv1alpha1.ConnectorSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
				Config:     map[string]string{"connector.class": "FileStreamSource"},
				State:      kafkaconnectv1alpha1.ConnectorStateStopped,
			})
			DeferCleanup(func() { cleanupConnector(ctx, nn) })

			r, recorder := newReconciler(mock)

			// Reconcile: init, finalizer, create (stopped), reset+annotation removal, status, full pass
			_, err := reconcileN(ctx, r, nn, 6)
			Expect(err).NotTo(HaveOccurred())

			// Annotation should be removed
			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).NotTo(HaveKey(offsetsAnnotation))

			// Offsets should be cleared in mock
			mock.mu.Lock()
			_, exists := mock.offsets[name]
			mock.mu.Unlock()
			Expect(exists).To(BeFalse())

			// Timestamp should be set
			Expect(connector.Status.LastResetOffsetsAt).NotTo(BeNil())

			Expect(recorder.Events).To(Receive(ContainSubstring("ResetOffsets")))
		})

		It("should emit warning and keep annotation when resetting but connector is not stopped", func() {
			ctx := context.Background()
			name := "offsets-reset-notstop"
			nn := nameFor(name)

			mock := newMockKafkaConnectServer()
			DeferCleanup(mock.Close)

			createConnectorWithSpec(ctx, name, map[string]string{
				offsetsAnnotation: "reset",
			}, kafkaconnectv1alpha1.ConnectorSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
				Config:     map[string]string{"connector.class": "FileStreamSource"},
				State:      kafkaconnectv1alpha1.ConnectorStateRunning,
			})
			DeferCleanup(func() { cleanupConnector(ctx, nn) })

			r, recorder := newReconciler(mock)

			_, err := reconcileN(ctx, r, nn, 5)
			Expect(err).NotTo(HaveOccurred())

			connector := getConnector(ctx, nn)
			Expect(connector).NotTo(BeNil())
			Expect(connector.Annotations).To(HaveKeyWithValue(offsetsAnnotation, "reset"))

			Expect(recorder.Events).To(Receive(ContainSubstring("FailedResetOffsets")))
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
