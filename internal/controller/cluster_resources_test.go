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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
)

const testClusterName = "my-cluster"
const jmxExporterVolName = "jmx-exporter"

var _ = Describe("Cluster Resources", func() {

	newCluster := func(plugins []kcv1alpha1.Plugin) *kcv1alpha1.Cluster {
		return &kcv1alpha1.Cluster{
			Spec: kcv1alpha1.ClusterSpec{
				Config:  map[string]string{"bootstrap.servers": "localhost:9092"},
				Plugins: plugins,
			},
		}
	}

	newClusterWithSecrets := func(plugins []kcv1alpha1.Plugin, secrets []kcv1alpha1.SecretMount) *kcv1alpha1.Cluster {
		return &kcv1alpha1.Cluster{
			Spec: kcv1alpha1.ClusterSpec{
				Config:  map[string]string{"bootstrap.servers": "localhost:9092"},
				Plugins: plugins,
				Secrets: secrets,
			},
		}
	}

	newClusterWithMetrics := func(metrics *kcv1alpha1.MetricsConfig) *kcv1alpha1.Cluster {
		return &kcv1alpha1.Cluster{
			Spec: kcv1alpha1.ClusterSpec{
				Config:  map[string]string{"bootstrap.servers": "localhost:9092"},
				Metrics: metrics,
			},
		}
	}

	Describe("deploymentForCluster", func() {
		It("should have only the config volume when no plugins are specified", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(1))
			Expect(*volumes[0].Name).To(Equal("config"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(1))
			Expect(*mounts[0].Name).To(Equal("config"))
		})

		It("should add plugin volumes and mounts for multiple plugins", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "plugin-a", Image: "registry.example.com/plugin-a:1.0"},
				{Name: "plugin-b", Image: "registry.example.com/plugin-b:2.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3)) // config + 2 plugins

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal("plugin-plugin-a"))
			Expect(*volumes[1].Image.Reference).To(Equal("registry.example.com/plugin-a:1.0"))
			Expect(*volumes[2].Name).To(Equal("plugin-plugin-b"))
			Expect(*volumes[2].Image.Reference).To(Equal("registry.example.com/plugin-b:2.0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal("plugin-plugin-a"))
			Expect(*mounts[1].MountPath).To(Equal("/plugins/plugin-a"))
			Expect(*mounts[1].ReadOnly).To(BeTrue())
			Expect(*mounts[2].Name).To(Equal("plugin-plugin-b"))
			Expect(*mounts[2].MountPath).To(Equal("/plugins/plugin-b"))
			Expect(*mounts[2].ReadOnly).To(BeTrue())
		})

		It("should propagate pullPolicy when specified", func() {
			policy := corev1.PullAlways
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "plugin", Image: "registry.example.com/plugin:latest", PullPolicy: &policy},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(2))
			Expect(*volumes[1].Image.PullPolicy).To(Equal(corev1.PullAlways))
		})

		It("should not set pullPolicy when not specified", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "plugin", Image: "registry.example.com/plugin:1.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(2))
			Expect(volumes[1].Image.PullPolicy).To(BeNil())
		})

		It("should add secret volumes and mounts with correct paths", func() {
			cluster := newClusterWithSecrets(nil, []kcv1alpha1.SecretMount{
				{Name: "my-keystore", SecretRef: corev1.LocalObjectReference{Name: "my-keystore-secret"}},
				{Name: "my-truststore", SecretRef: corev1.LocalObjectReference{Name: "my-truststore-secret"}},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3)) // config + 2 secrets

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal("secret-0"))
			Expect(*volumes[1].Secret.SecretName).To(Equal("my-keystore-secret"))
			Expect(*volumes[2].Name).To(Equal("secret-1"))
			Expect(*volumes[2].Secret.SecretName).To(Equal("my-truststore-secret"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal("secret-0"))
			Expect(*mounts[1].MountPath).To(Equal("/secrets/my-keystore"))
			Expect(*mounts[1].ReadOnly).To(BeTrue())
			Expect(*mounts[2].Name).To(Equal("secret-1"))
			Expect(*mounts[2].MountPath).To(Equal("/secrets/my-truststore"))
			Expect(*mounts[2].ReadOnly).To(BeTrue())
		})

		It("should add both plugin and secret volumes together", func() {
			cluster := newClusterWithSecrets(
				[]kcv1alpha1.Plugin{
					{Name: "plugin-a", Image: "registry.example.com/plugin-a:1.0"},
				},
				[]kcv1alpha1.SecretMount{
					{Name: "my-keystore", SecretRef: corev1.LocalObjectReference{Name: "my-keystore-secret"}},
				},
			)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3)) // config + 1 plugin + 1 secret

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal("plugin-plugin-a"))
			Expect(*volumes[2].Name).To(Equal("secret-0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal("plugin-plugin-a"))
			Expect(*mounts[1].MountPath).To(Equal("/plugins/plugin-a"))
			Expect(*mounts[2].Name).To(Equal("secret-0"))
			Expect(*mounts[2].MountPath).To(Equal("/secrets/my-keystore"))
		})

		It("should have only the config volume when no plugins or secrets are specified", func() {
			cluster := newClusterWithSecrets(nil, nil)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(1))
			Expect(*volumes[0].Name).To(Equal("config"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(1))
			Expect(*mounts[0].Name).To(Equal("config"))
		})

		It("should have only config/hash annotation when no user pod annotations", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			annotations := dep.Spec.Template.Annotations
			Expect(annotations).To(HaveLen(1))
			Expect(annotations).To(HaveKeyWithValue("config/hash", ""))
		})

		It("should merge user pod annotations with config/hash", func() {
			cluster := newCluster(nil)
			cluster.Spec.PodAnnotations = map[string]string{
				"vault.hashicorp.com/agent-inject": "true",
			}
			dep := deploymentForCluster(cluster)

			annotations := dep.Spec.Template.Annotations
			Expect(annotations).To(HaveLen(2))
			Expect(annotations).To(HaveKeyWithValue("vault.hashicorp.com/agent-inject", "true"))
			Expect(annotations).To(HaveKeyWithValue("config/hash", ""))
		})

		It("should let internal config/hash take precedence over user collision", func() {
			hash := "abc123"
			cluster := newCluster(nil)
			cluster.Status.ConfigHash = &hash
			cluster.Spec.PodAnnotations = map[string]string{
				"config/hash": "user-value",
			}
			dep := deploymentForCluster(cluster)

			annotations := dep.Spec.Template.Annotations
			Expect(annotations).To(HaveKeyWithValue("config/hash", "abc123"))
		})

		It("should have only internal labels when no user pod labels", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			dep := deploymentForCluster(cluster)

			labels := dep.Spec.Template.Labels
			Expect(labels).To(HaveLen(2))
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "kafka-connect"))
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", testClusterName))
		})

		It("should merge user pod labels with internal labels", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			cluster.Spec.PodLabels = map[string]string{
				"team": "platform",
			}
			dep := deploymentForCluster(cluster)

			labels := dep.Spec.Template.Labels
			Expect(labels).To(HaveLen(3))
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "kafka-connect"))
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", testClusterName))
			Expect(labels).To(HaveKeyWithValue("team", "platform"))
		})

		It("should let internal labels take precedence over user collision", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			cluster.Spec.PodLabels = map[string]string{
				"app.kubernetes.io/name": "custom-name",
			}
			dep := deploymentForCluster(cluster)

			labels := dep.Spec.Template.Labels
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "kafka-connect"))
		})

		It("should use only internal labels for selector (not user labels)", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			cluster.Spec.PodLabels = map[string]string{
				"team": "platform",
			}
			dep := deploymentForCluster(cluster)

			selectorLabels := dep.Spec.Selector.MatchLabels
			Expect(selectorLabels).To(HaveLen(2))
			Expect(selectorLabels).To(HaveKeyWithValue("app.kubernetes.io/name", "kafka-connect"))
			Expect(selectorLabels).To(HaveKeyWithValue("app.kubernetes.io/instance", testClusterName))
			Expect(selectorLabels).NotTo(HaveKey("team"))
		})

		It("should apply deployment annotations when specified", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			cluster.Spec.DeploymentAnnotations = map[string]string{
				"prometheus.io/scrape": "true",
			}
			dep := deploymentForCluster(cluster)

			Expect(dep.Annotations).To(HaveKeyWithValue("prometheus.io/scrape", "true"))
		})

		It("should not have deployment annotations when none specified", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			Expect(dep.Annotations).To(BeEmpty())
		})

		It("should set serviceAccountName on PodSpec", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			dep := deploymentForCluster(cluster)

			Expect(dep.Spec.Template.Spec.ServiceAccountName).NotTo(BeNil())
			Expect(*dep.Spec.Template.Spec.ServiceAccountName).To(Equal("my-cluster-connect"))
		})

		It("should not have JMX exporter volume/mount/env/port when metrics is nil", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			for _, v := range volumes {
				Expect(*v.Name).NotTo(Equal(jmxExporterVolName))
			}

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			for _, m := range mounts {
				Expect(*m.Name).NotTo(Equal(jmxExporterVolName))
			}

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			for _, e := range envVars {
				Expect(*e.Name).NotTo(Equal("KAFKA_OPTS"))
			}

			ports := dep.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(1))
			Expect(*ports[0].Name).To(Equal("http"))
		})

		It("should add JMX exporter volume, mount, env, and port when jmxExporter is set", func() {
			cluster := newClusterWithMetrics(&kcv1alpha1.MetricsConfig{
				JMXExporter: &kcv1alpha1.JMXExporterConfig{},
			})
			dep := deploymentForCluster(cluster)

			// Volume
			volumes := dep.Spec.Template.Spec.Volumes
			var jmxVol *corev1ac.VolumeApplyConfiguration
			for i := range volumes {
				if *volumes[i].Name == jmxExporterVolName {
					jmxVol = &volumes[i]
				}
			}
			Expect(jmxVol).NotTo(BeNil())
			Expect(*jmxVol.Image.Reference).To(Equal("ghcr.io/b1zzu/kafka-connect-operator/jmx-exporter:1.5.0"))

			// Mount
			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			var jmxMount *corev1ac.VolumeMountApplyConfiguration
			for i := range mounts {
				if *mounts[i].Name == jmxExporterVolName {
					jmxMount = &mounts[i]
				}
			}
			Expect(jmxMount).NotTo(BeNil())
			Expect(*jmxMount.MountPath).To(Equal("/opt/jmx-exporter"))
			Expect(*jmxMount.ReadOnly).To(BeTrue())

			// Env
			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var kafkaOpts *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == "KAFKA_OPTS" {
					kafkaOpts = &envVars[i]
				}
			}
			Expect(kafkaOpts).NotTo(BeNil())
			Expect(*kafkaOpts.Value).To(ContainSubstring("jmx_prometheus_javaagent.jar=9404"))

			// Port
			ports := dep.Spec.Template.Spec.Containers[0].Ports
			Expect(ports).To(HaveLen(2))
			Expect(*ports[1].Name).To(Equal("metrics"))
			Expect(*ports[1].ContainerPort).To(Equal(int32(9404)))
		})

		It("should use custom JMX exporter image when overridden", func() {
			customImage := "custom-registry/jmx-exporter:1.5.0"
			cluster := newClusterWithMetrics(&kcv1alpha1.MetricsConfig{
				JMXExporter: &kcv1alpha1.JMXExporterConfig{
					Image: &customImage,
				},
			})
			dep := deploymentForCluster(cluster)

			var jmxVol *corev1ac.VolumeApplyConfiguration
			for i := range dep.Spec.Template.Spec.Volumes {
				if *dep.Spec.Template.Spec.Volumes[i].Name == jmxExporterVolName {
					jmxVol = &dep.Spec.Template.Spec.Volumes[i]
				}
			}
			Expect(jmxVol).NotTo(BeNil())
			Expect(*jmxVol.Image.Reference).To(Equal("custom-registry/jmx-exporter:1.5.0"))
		})

		It("should propagate JMX exporter pullPolicy when specified", func() {
			policy := corev1.PullAlways
			cluster := newClusterWithMetrics(&kcv1alpha1.MetricsConfig{
				JMXExporter: &kcv1alpha1.JMXExporterConfig{
					PullPolicy: &policy,
				},
			})
			dep := deploymentForCluster(cluster)

			var jmxVol *corev1ac.VolumeApplyConfiguration
			for i := range dep.Spec.Template.Spec.Volumes {
				if *dep.Spec.Template.Spec.Volumes[i].Name == jmxExporterVolName {
					jmxVol = &dep.Spec.Template.Spec.Volumes[i]
				}
			}
			Expect(jmxVol).NotTo(BeNil())
			Expect(*jmxVol.Image.PullPolicy).To(Equal(corev1.PullAlways))
		})
	})

	Describe("serviceAccountForCluster", func() {
		It("should create SA with correct name", func() {
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			sa := serviceAccountForCluster(cluster)

			Expect(*sa.Name).To(Equal("my-cluster-connect"))
			Expect(*sa.Namespace).To(Equal("default"))
		})

		It("should have user-defined annotations when specified", func() {
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
					ServiceAccountAnnotations: map[string]string{
						"eks.amazonaws.com/role-arn": "arn:aws:iam::123456789012:role/my-role",
					},
				},
			}
			sa := serviceAccountForCluster(cluster)

			Expect(sa.Annotations).To(HaveKeyWithValue("eks.amazonaws.com/role-arn", "arn:aws:iam::123456789012:role/my-role"))
		})

		It("should have no annotations when none specified", func() {
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			sa := serviceAccountForCluster(cluster)

			Expect(sa.Annotations).To(BeEmpty())
		})
	})

	Describe("serviceForCluster", func() {
		It("should apply service annotations when specified", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			cluster.Spec.ServiceAnnotations = map[string]string{
				"service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
			}
			svc := serviceForCluster(cluster)

			Expect(svc.Annotations).To(HaveKeyWithValue("service.beta.kubernetes.io/aws-load-balancer-type", "nlb"))
		})

		It("should not have annotations when none specified", func() {
			cluster := newCluster(nil)
			svc := serviceForCluster(cluster)

			Expect(svc.Annotations).To(BeEmpty())
		})
	})

	Describe("kafkaConnectConfigsForCluster", func() {
		It("should not set plugin.path when no plugins are specified", func() {
			cluster := newCluster(nil)
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).NotTo(HaveKey("plugin.path"))
		})

		It("should set plugin.path for a single plugin", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "my-plugin", Image: "registry.example.com/plugin:1.0"},
			})
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKeyWithValue("plugin.path", "/plugins/my-plugin"))
		})

		It("should set plugin.path with comma-separated paths for multiple plugins", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "plugin-a", Image: "registry.example.com/plugin-a:1.0"},
				{Name: "plugin-b", Image: "registry.example.com/plugin-b:2.0"},
				{Name: "plugin-c", Image: "registry.example.com/plugin-c:3.0"},
			})
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKeyWithValue("plugin.path", "/plugins/plugin-a,/plugins/plugin-b,/plugins/plugin-c"))
		})

		It("should return no error for valid user config", func() {
			cluster := newCluster(nil)
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKey("bootstrap.servers"))
			Expect(configs).To(HaveKey("listeners"))
		})

		It("should return error when a single managed key is set by user", func() {
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{
						"bootstrap.servers": "localhost:9092",
						"listeners":         "http://:9999",
					},
				},
			}
			_, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("listeners"))
		})

		It("should return error listing all conflicting keys when multiple managed keys are set", func() {
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{
						"bootstrap.servers":    "localhost:9092",
						"listeners":            "http://:9999",
						"rest.advertised.port": "9999",
					},
				},
			}
			_, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("listeners"))
			Expect(err.Error()).To(ContainSubstring("rest.advertised.port"))
		})

		It("should allow plugin.path in user config when no plugins are specified", func() {
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{
						"bootstrap.servers": "localhost:9092",
						"plugin.path":       "/custom/plugins",
					},
				},
			}
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKeyWithValue("plugin.path", "/custom/plugins"))
		})

		It("should reject plugin.path in user config when plugins are present", func() {
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{
						"bootstrap.servers": "localhost:9092",
						"plugin.path":       "/custom/plugins",
					},
					Plugins: []kcv1alpha1.Plugin{
						{Name: "my-plugin", Image: "registry.example.com/plugin:1.0"},
					},
				},
			}
			_, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("plugin.path"))
		})

		It("should always include file config provider", func() {
			cluster := newCluster(nil)
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKeyWithValue("config.providers", "env,file"))
			Expect(configs).To(HaveKeyWithValue("config.providers.file.class", "org.apache.kafka.common.config.provider.FileConfigProvider"))
		})
	})

	Describe("configMapForCluster", func() {
		It("should not have jmx-exporter-config.yaml key when metrics is nil", func() {
			cluster := newCluster(nil)
			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).NotTo(HaveKey("jmx-exporter-config.yaml"))
			Expect(cm.Data).To(HaveKey("connect.properties"))
		})

		It("should have jmx-exporter-config.yaml key when jmxExporter is configured", func() {
			cluster := newClusterWithMetrics(&kcv1alpha1.MetricsConfig{
				JMXExporter: &kcv1alpha1.JMXExporterConfig{},
			})
			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey("jmx-exporter-config.yaml"))
			Expect(cm.Data["jmx-exporter-config.yaml"]).To(ContainSubstring("rules:"))
			Expect(cm.Data).To(HaveKey("connect.properties"))
		})
	})

	Describe("networkPolicyForCluster", func() {
		It("should have 2 ingress rules when metrics is nil", func() {
			cluster := newCluster(nil)
			cluster.Name = testClusterName
			np := networkPolicyForCluster(cluster, "kafka-connect-operator")

			Expect(np.Spec.Ingress).To(HaveLen(2))
		})

		It("should have 3 ingress rules with open TCP 9404 when jmxExporter is configured", func() {
			cluster := newClusterWithMetrics(&kcv1alpha1.MetricsConfig{
				JMXExporter: &kcv1alpha1.JMXExporterConfig{},
			})
			cluster.Name = testClusterName
			np := networkPolicyForCluster(cluster, "kafka-connect-operator")

			Expect(np.Spec.Ingress).To(HaveLen(3))

			// Third rule should have no From selector (open) and port 9404
			metricsRule := np.Spec.Ingress[2]
			Expect(metricsRule.From).To(BeEmpty())
			Expect(metricsRule.Ports).To(HaveLen(1))
			Expect(*metricsRule.Ports[0].Port).To(Equal(intstr.FromInt(9404)))
		})
	})
})
