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
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kafkaconnectv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	kafkaconnect "github.com/b1zzu/kafka-connect-operator/pkg/kafka-connect"
)

// gvkFixClient wraps a client.Client to populate TypeMeta after Get calls.
// The controller-runtime typed client strips TypeMeta during deserialization
// of typed objects. In production, the manager's cached client preserves GVK
// through informer watch events, but the plain client.New used in envtest
// does not. This wrapper restores GVK from the scheme after every Get so
// that ownerReferenceForCluster (which reads GVK from the object) works.
type gvkFixClient struct {
	client.Client
	scheme *runtime.Scheme
}

func (c *gvkFixClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	gvks, _, err := c.scheme.ObjectKinds(obj)
	if err == nil && len(gvks) > 0 {
		obj.GetObjectKind().SetGroupVersionKind(gvks[0])
	}
	return nil
}

var _ = Describe("Cluster Controller", func() {
	newReconciler := func() *ClusterReconciler {
		return &ClusterReconciler{
			Client: &gvkFixClient{Client: k8sClient, scheme: k8sClient.Scheme()},
			Scheme: k8sClient.Scheme(),
		}
	}

	createCluster := func(ctx context.Context, name string, spec kafkaconnectv1alpha1.ClusterSpec) {
		cluster := &kafkaconnectv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
			},
			Spec: spec,
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
	}

	nameFor := func(name string) types.NamespacedName {
		return types.NamespacedName{Name: name, Namespace: "default"}
	}

	reconcileN := func(ctx context.Context, r *ClusterReconciler, nn types.NamespacedName, n int) {
		for range n {
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		}
	}

	Context("When the Cluster resource does not exist", func() {
		It("should return no error and no requeue", func() {
			r := newReconciler()
			nn := nameFor("nonexistent-cluster")

			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))
		})
	})

	Context("When reconciling for the first time", func() {
		const name = "cluster-init"

		BeforeEach(func() {
			createCluster(ctx, name, kafkaconnectv1alpha1.ClusterSpec{
				Config: map[string]string{
					"bootstrap.servers": "kafka:9092",
					"group.id":          "test-group",
				},
			})
		})

		AfterEach(func() {
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nameFor(name), cluster)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})

		It("should initialize status conditions", func() {
			r := newReconciler()
			nn := nameFor(name)

			// First reconcile: initializes conditions and returns nil cluster to trigger re-reconcile
			reconcileN(ctx, r, nn, 1)

			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())

			Expect(cluster.Status.Conditions).To(HaveLen(1))
			cond := meta.FindStatusCondition(cluster.Status.Conditions, "Available")
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionUnknown))
			Expect(cond.Reason).To(Equal("Reconciling"))
			Expect(cond.ObservedGeneration).To(Equal(cluster.Generation))

			// Service should not exist yet (reconciliation stopped after condition init)
			svc := &corev1.Service{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, svc)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("When reconciling a valid Cluster", func() {
		const name = "cluster-happy"

		BeforeEach(func() {
			createCluster(ctx, name, kafkaconnectv1alpha1.ClusterSpec{
				Config: map[string]string{
					"bootstrap.servers": "kafka:9092",
					"group.id":          "happy-group",
				},
			})
		})

		AfterEach(func() {
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nameFor(name), cluster)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})

		It("should create all sub-resources", func() {
			r := newReconciler()
			nn := nameFor(name)

			// 4 reconcile calls: 1) init conditions, 2) url update, 3) configHash update, 4) full pass
			reconcileN(ctx, r, nn, 4)

			By("checking the Service")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, svc)).To(Succeed())
			Expect(svc.Spec.Ports).To(HaveLen(1))
			Expect(svc.Spec.Ports[0].Port).To(Equal(int32(8083)))

			By("checking the NetworkPolicy")
			np := &netv1.NetworkPolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, np)).To(Succeed())

			By("checking the ServiceAccount")
			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sa)).To(Succeed())

			By("checking the ConfigMap")
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-config", Namespace: "default"}, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKey("connect.properties"))
			Expect(cm.Data["connect.properties"]).To(ContainSubstring("bootstrap.servers"))
			Expect(cm.Data).To(HaveKey("connect-log4j2.properties"))
			Expect(cm.Data["connect-log4j2.properties"]).To(ContainSubstring("PatternLayout"))

			By("checking the Deployment")
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, dep)).To(Succeed())

			By("checking the Cluster status configHash")
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.ConfigHash).NotTo(BeNil())

			By("checking the Cluster status url")
			Expect(cluster.Status.URL).To(Equal("http://cluster-happy.default.svc:8083"))
		})
	})

	Context("When the Deployment rollout is complete", func() {
		const name = "cluster-plugins"

		var (
			mu      sync.Mutex
			plugins []kafkaconnect.ConnectorPlugin
			calls   int
			server  *httptest.Server
		)

		setPlugins := func(p ...kafkaconnect.ConnectorPlugin) {
			mu.Lock()
			defer mu.Unlock()
			plugins = p
		}

		getCalls := func() int {
			mu.Lock()
			defer mu.Unlock()
			return calls
		}

		completeRollout := func(ctx context.Context) {
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, nameFor(name), dep)).To(Succeed())
			replicas := *dep.Spec.Replicas
			dep.Status = appsv1.DeploymentStatus{
				ObservedGeneration: dep.Generation,
				Replicas:           replicas,
				UpdatedReplicas:    replicas,
				ReadyReplicas:      replicas,
				AvailableReplicas:  replicas,
				Conditions: []appsv1.DeploymentCondition{{
					Type:   appsv1.DeploymentAvailable,
					Status: corev1.ConditionTrue,
					Reason: "MinimumReplicasAvailable",
				}},
			}
			Expect(k8sClient.Status().Update(ctx, dep)).To(Succeed())
		}

		BeforeEach(func() {
			calls = 0
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Path != "/connector-plugins" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				calls++
				_ = json.NewEncoder(w).Encode(plugins)
			}))
			setPlugins(kafkaconnect.ConnectorPlugin{Class: "com.example.SinkConnector", Type: "sink", Version: "1.0.0"})

			createCluster(ctx, name, kafkaconnectv1alpha1.ClusterSpec{
				Config: map[string]string{
					"bootstrap.servers": "kafka:9092",
					"group.id":          "plugins-group",
				},
			})
		})

		AfterEach(func() {
			server.Close()
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nameFor(name), cluster)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})

		It("should list the connector plugins once per Deployment generation", func() {
			r := newReconciler()
			r.NewKafkaConnectClientFunc = func(_, _ string) (*kafkaconnect.Client, error) {
				return kafkaconnect.NewClient(server.URL), nil
			}
			nn := nameFor(name)
			cluster := &kafkaconnectv1alpha1.Cluster{}

			By("not listing the plugins before the rollout is complete")
			reconcileN(ctx, r, nn, 4)
			Expect(getCalls()).To(Equal(0))
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.ConnectorPlugins).To(BeEmpty())

			By("listing the plugins after the rollout is complete")
			completeRollout(ctx)
			// 1) Available condition update, 2) connectorPlugins update, 3) full pass
			reconcileN(ctx, r, nn, 3)
			Expect(getCalls()).To(Equal(1))
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.ConnectorPlugins).To(Equal([]kafkaconnectv1alpha1.ConnectorPlugin{
				{Class: "com.example.SinkConnector", Type: "sink", Version: "1.0.0"},
			}))

			By("not listing the plugins again for the same Deployment generation")
			reconcileN(ctx, r, nn, 2)
			Expect(getCalls()).To(Equal(1))

			By("listing the plugins again after a new rollout")
			setPlugins(
				kafkaconnect.ConnectorPlugin{Class: "com.example.SinkConnector", Type: "sink", Version: "1.0.0"},
				kafkaconnect.ConnectorPlugin{Class: "com.example.SourceConnector", Type: "source", Version: "2.0.0"},
			)
			cluster.Spec.Plugins = []kafkaconnectv1alpha1.Plugin{{Name: "source", Image: "example.com/source:2.0.0"}}
			Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
			// 1) configHash update, 2) Deployment update, 3) full pass
			reconcileN(ctx, r, nn, 3)
			Expect(getCalls()).To(Equal(1))
			completeRollout(ctx)
			// 1) connectorPlugins update, 2) full pass
			reconcileN(ctx, r, nn, 2)
			Expect(getCalls()).To(Equal(2))
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.ConnectorPlugins).To(HaveLen(2))
			Expect(cluster.Status.ConnectorPlugins[1].Class).To(Equal("com.example.SourceConnector"))
		})

		It("should not list the connector plugins again when Kafka Connect has none", func() {
			r := newReconciler()
			r.NewKafkaConnectClientFunc = func(_, _ string) (*kafkaconnect.Client, error) {
				return kafkaconnect.NewClient(server.URL), nil
			}
			nn := nameFor(name)
			setPlugins()

			reconcileN(ctx, r, nn, 4)
			completeRollout(ctx)
			reconcileN(ctx, r, nn, 4)
			Expect(getCalls()).To(Equal(1))

			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.ConnectorPlugins).To(BeEmpty())
		})
	})

	Context("When the Cluster config contains a managed key", func() {
		const name = "cluster-invalid"

		BeforeEach(func() {
			createCluster(ctx, name, kafkaconnectv1alpha1.ClusterSpec{
				Config: map[string]string{
					"bootstrap.servers": "kafka:9092",
					"group.id":          "invalid-group",
					"listeners":         "http://:9999",
				},
			})
		})

		AfterEach(func() {
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nameFor(name), cluster)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})

		It("should set Available=False with UserError reason and not create a Deployment", func() {
			r := newReconciler()
			nn := nameFor(name)

			// 1) init conditions, 2) url update, 3) hits config validation error
			reconcileN(ctx, r, nn, 3)

			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())

			cond := meta.FindStatusCondition(cluster.Status.Conditions, "Available")
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("UserError"))
			Expect(cond.Message).To(ContainSubstring("listeners"))
			Expect(cond.ObservedGeneration).To(Equal(cluster.Generation))

			// Deployment should not have been created
			dep := &appsv1.Deployment{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, dep)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("When NetworkPolicy is disabled", func() {
		const name = "cluster-no-np"

		BeforeEach(func() {
			enabled := false
			createCluster(ctx, name, kafkaconnectv1alpha1.ClusterSpec{
				Config: map[string]string{
					"bootstrap.servers": "kafka:9092",
					"group.id":          "no-np-group",
				},
				NetworkPolicy: &kafkaconnectv1alpha1.NetworkPolicyConfig{
					Enabled: &enabled,
				},
			})
		})

		AfterEach(func() {
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nameFor(name), cluster)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})

		It("should skip NetworkPolicy but create all other sub-resources", func() {
			r := newReconciler()
			nn := nameFor(name)

			reconcileN(ctx, r, nn, 4)

			By("checking that NetworkPolicy does NOT exist")
			np := &netv1.NetworkPolicy{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, np)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			By("checking the Service exists")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, svc)).To(Succeed())

			By("checking the ServiceAccount exists")
			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, sa)).To(Succeed())

			By("checking the ConfigMap exists")
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name + "-config", Namespace: "default"}, cm)).To(Succeed())

			By("checking the Deployment exists")
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, dep)).To(Succeed())
		})
	})

	Describe("handleReconciliationError", func() {
		newReconciler := func() *ClusterReconciler {
			return &ClusterReconciler{
				Client: &gvkFixClient{Client: k8sClient, scheme: k8sClient.Scheme()},
				Scheme: k8sClient.Scheme(),
			}
		}

		It("should not panic when ClusterReconciliationError.cluster is nil", func() {
			r := newReconciler()
			nn := types.NamespacedName{Name: "test-cluster", Namespace: "default"}

			createCluster(ctx, "test-cluster", kafkaconnectv1alpha1.ClusterSpec{
				Config: map[string]string{
					"bootstrap.servers": "kafka:9092",
					"group.id":          "test-group",
				},
			})
			defer func() {
				cluster := &kafkaconnectv1alpha1.Cluster{}
				_ = k8sClient.Get(ctx, nn, cluster)
				_ = k8sClient.Delete(ctx, cluster)
			}()

			reconcileN(ctx, r, nn, 1)

			err := &ClusterReconciliationError{
				err:     nil,
				msg:     "test user error",
				cluster: nil,
			}

			result := r.handleReconciliationError(ctx, err)
			Expect(result).To(HaveOccurred())
			Expect(result.Error()).To(ContainSubstring("unexpected nil cluster"))
		})

		It("should return nil when error is nil", func() {
			r := newReconciler()
			result := r.handleReconciliationError(ctx, nil)
			Expect(result).ToNot(HaveOccurred())
		})

		It("should return error as-is when error is not ClusterReconciliationError", func() {
			r := newReconciler()
			originalErr := fmt.Errorf("some other error")
			result := r.handleReconciliationError(ctx, originalErr)
			Expect(result).To(Equal(originalErr))
		})
	})

	Describe("serverSideApply", func() {
		It("should not panic and return cluster when Apply succeeds", func() {
			cluster := &kafkaconnectv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ssa-test-cluster",
					Namespace: "default",
				},
				Spec: kafkaconnectv1alpha1.ClusterSpec{
					Config: map[string]string{
						"bootstrap.servers": "kafka:9092",
					},
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
			defer func() { Expect(k8sClient.Delete(ctx, cluster)).To(Succeed()) }()

			r := &ClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			result, err := r.serverSideApply(ctx, cluster, serviceForCluster(cluster))
			Expect(err).To(Succeed())
			Expect(result).NotTo(BeNil())
		})
	})

	Describe("hashConfigMap", func() {
		baseConfigMap := func() *corev1.ConfigMap {
			return &corev1.ConfigMap{
				Data: map[string]string{
					"connect.properties":        "bootstrap.servers=kafka:9092\n",
					"connect-log4j2.properties": "rootLogger.level=INFO\n",
				},
			}
		}

		It("should change when connect.properties changes", func() {
			cm1 := baseConfigMap()
			hash1 := hashConfigMap(cm1)

			cm2 := baseConfigMap()
			cm2.Data["connect.properties"] = "bootstrap.servers=kafka:9093\n"
			hash2 := hashConfigMap(cm2)

			Expect(hash1).NotTo(Equal(hash2))
		})

		It("should change when jmx-exporter-config.yaml changes", func() {
			cm1 := baseConfigMap()
			cm1.Data["jmx-exporter-config.yaml"] = "rules:\n- pattern: \".*\"\n"
			hash1 := hashConfigMap(cm1)

			cm2 := baseConfigMap()
			cm2.Data["jmx-exporter-config.yaml"] = "rules:\n- pattern: \"foo\"\n"
			hash2 := hashConfigMap(cm2)

			Expect(hash1).NotTo(Equal(hash2))
		})

		It("should NOT change when connect-log4j2.properties changes", func() {
			cm1 := baseConfigMap()
			hash1 := hashConfigMap(cm1)

			cm2 := baseConfigMap()
			cm2.Data["connect-log4j2.properties"] = "rootLogger.level=DEBUG\n"
			hash2 := hashConfigMap(cm2)

			Expect(hash1).To(Equal(hash2))
		})

		It("should NOT change when an unknown key is added", func() {
			cm1 := baseConfigMap()
			hash1 := hashConfigMap(cm1)

			cm2 := baseConfigMap()
			cm2.Data["unknown-key"] = "some-value"
			hash2 := hashConfigMap(cm2)

			Expect(hash1).To(Equal(hash2))
		})
	})

	Context("When the pause-reconciliation annotation is set", func() {
		const name = "cluster-paused"

		BeforeEach(func() {
			cluster := &kafkaconnectv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
					Annotations: map[string]string{
						"kafka-connect.b1zzu.net/pause-reconciliation": "true",
					},
				},
				Spec: kafkaconnectv1alpha1.ClusterSpec{
					Config: map[string]string{
						"bootstrap.servers": "kafka:9092",
						"group.id":          "paused-group",
					},
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		})

		AfterEach(func() {
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nameFor(name), cluster)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cluster)).To(Succeed())
		})

		It("should skip reconciliation and not initialize status conditions", func() {
			r := newReconciler()
			nn := nameFor(name)

			// Reconcile should skip due to pause annotation
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Status conditions should not be initialized since reconciliation is skipped
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.Conditions).To(BeEmpty())

			// Sub-resources should not be created
			svc := &corev1.Service{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, svc)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			dep := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, dep)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("should resume reconciliation when pause annotation is removed", func() {
			r := newReconciler()
			nn := nameFor(name)

			// Reconcile should skip due to pause annotation
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			// Remove the pause annotation
			cluster := &kafkaconnectv1alpha1.Cluster{}
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			delete(cluster.Annotations, "kafka-connect.b1zzu.net/pause-reconciliation")
			Expect(k8sClient.Update(ctx, cluster)).To(Succeed())

			// Now reconcile should proceed normally
			// 4 reconcile calls: 1) init conditions, 2) url update, 3) configHash update, 4) full pass
			reconcileN(ctx, r, nn, 4)

			// Status should be initialized
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.Conditions).To(HaveLen(1))
			cond := meta.FindStatusCondition(cluster.Status.Conditions, "Available")
			Expect(cond).NotTo(BeNil())

			// Sub-resources should be created
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, svc)).To(Succeed())

			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, dep)).To(Succeed())
		})

		It("should not skip reconciliation when pause annotation is set to false", func() {
			// Update the annotation to false
			cluster := &kafkaconnectv1alpha1.Cluster{}
			nn := nameFor(name)
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			cluster.Annotations["kafka-connect.b1zzu.net/pause-reconciliation"] = "false"
			Expect(k8sClient.Update(ctx, cluster)).To(Succeed())

			r := newReconciler()

			// Reconcile should proceed normally since annotation is not "true"
			// 4 reconcile calls: 1) init conditions, 2) url update, 3) configHash update, 4) full pass
			reconcileN(ctx, r, nn, 4)

			// Status should be initialized
			Expect(k8sClient.Get(ctx, nn, cluster)).To(Succeed())
			Expect(cluster.Status.Conditions).To(HaveLen(1))
			cond := meta.FindStatusCondition(cluster.Status.Conditions, "Available")
			Expect(cond).NotTo(BeNil())

			// Sub-resources should be created
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, svc)).To(Succeed())

			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, dep)).To(Succeed())
		})
	})
})
