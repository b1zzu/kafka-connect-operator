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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafkaconnectv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	kafkaconnect "github.com/b1zzu/kafka-connect-operator/pkg/kafka-connect"
)

var _ = Describe("Connector Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		connector := &kafkaconnectv1alpha1.Connector{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Connector")
			err := k8sClient.Get(ctx, typeNamespacedName, connector)
			if err != nil && errors.IsNotFound(err) {
				resource := &kafkaconnectv1alpha1.Connector{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &kafkaconnectv1alpha1.Connector{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Connector")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ConnectorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
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
