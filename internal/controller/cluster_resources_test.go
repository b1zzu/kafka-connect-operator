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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
)

const testClusterName = "my-cluster"
const jmxExporterVolName = "jmx-exporter"
const log4jLayoutVolName = "log4j-layout-template-json"
const classpathEnvName = "CLASSPATH"

var _ = Describe("Cluster Resources", func() {

	jsonLayoutLogging := func() *kcv1alpha1.LoggingConfig {
		return &kcv1alpha1.LoggingConfig{Log4jJsonLayout: &kcv1alpha1.Log4jJsonLayoutConfig{}}
	}

	newCluster := func(plugins []kcv1alpha1.Plugin) *kcv1alpha1.Cluster {
		return &kcv1alpha1.Cluster{
			Spec: kcv1alpha1.ClusterSpec{
				Config:  map[string]string{"bootstrap.servers": "localhost:9092"},
				Plugins: plugins,
				Logging: jsonLayoutLogging(),
			},
		}
	}

	newClusterWithSecrets := func(plugins []kcv1alpha1.Plugin, secrets []kcv1alpha1.SecretMount) *kcv1alpha1.Cluster {
		return &kcv1alpha1.Cluster{
			Spec: kcv1alpha1.ClusterSpec{
				Config:  map[string]string{"bootstrap.servers": "localhost:9092"},
				Plugins: plugins,
				Secrets: secrets,
				Logging: jsonLayoutLogging(),
			},
		}
	}

	newClusterWithMetrics := func(metrics *kcv1alpha1.MetricsConfig) *kcv1alpha1.Cluster {
		return &kcv1alpha1.Cluster{
			Spec: kcv1alpha1.ClusterSpec{
				Config:  map[string]string{"bootstrap.servers": "localhost:9092"},
				Metrics: metrics,
				Logging: jsonLayoutLogging(),
			},
		}
	}

	newClusterWithLibraries := func(libraries []kcv1alpha1.Library) *kcv1alpha1.Cluster {
		return &kcv1alpha1.Cluster{
			Spec: kcv1alpha1.ClusterSpec{
				Config:    map[string]string{"bootstrap.servers": "localhost:9092"},
				Libraries: libraries,
				Logging:   jsonLayoutLogging(),
			},
		}
	}

	Describe("deploymentForCluster", func() {
		It("should have config and log4j-layout-template-json volumes when no plugins are specified", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3))
			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*volumes[2].Name).To(Equal("logs"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*mounts[2].Name).To(Equal("logs"))
		})

		It("should add plugin volumes and mounts for multiple plugins", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "plugin-a", Image: "registry.example.com/plugin-a:1.0"},
				{Name: "plugin-b", Image: "registry.example.com/plugin-b:2.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(5)) // config + log4j-layout + logs + 2 plugins

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*volumes[2].Name).To(Equal("logs"))
			Expect(*volumes[3].Name).To(Equal("plugin-plugin-a"))
			Expect(*volumes[3].Image.Reference).To(Equal("registry.example.com/plugin-a:1.0"))
			Expect(*volumes[4].Name).To(Equal("plugin-plugin-b"))
			Expect(*volumes[4].Image.Reference).To(Equal("registry.example.com/plugin-b:2.0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(5))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*mounts[2].Name).To(Equal("logs"))
			Expect(*mounts[3].Name).To(Equal("plugin-plugin-a"))
			Expect(*mounts[3].MountPath).To(Equal("/plugins/plugin-a"))
			Expect(*mounts[3].ReadOnly).To(BeTrue())
			Expect(*mounts[4].Name).To(Equal("plugin-plugin-b"))
			Expect(*mounts[4].MountPath).To(Equal("/plugins/plugin-b"))
			Expect(*mounts[4].ReadOnly).To(BeTrue())
		})

		It("should propagate pullPolicy when specified", func() {
			policy := corev1.PullAlways
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "plugin", Image: "registry.example.com/plugin:latest", PullPolicy: &policy},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(4)) // config + log4j-layout + logs + 1 plugin
			Expect(*volumes[3].Image.PullPolicy).To(Equal(corev1.PullAlways))
		})

		It("should not set pullPolicy when not specified", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Name: "plugin", Image: "registry.example.com/plugin:1.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(4)) // config + log4j-layout + logs + 1 plugin
			Expect(volumes[3].Image.PullPolicy).To(BeNil())
		})

		It("should add secret volumes and mounts with correct paths", func() {
			cluster := newClusterWithSecrets(nil, []kcv1alpha1.SecretMount{
				{Name: "my-keystore", SecretRef: corev1.LocalObjectReference{Name: "my-keystore-secret"}},
				{Name: "my-truststore", SecretRef: corev1.LocalObjectReference{Name: "my-truststore-secret"}},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(5)) // config + log4j-layout + logs + 2 secrets

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*volumes[2].Name).To(Equal("logs"))
			Expect(*volumes[3].Name).To(Equal("secret-0"))
			Expect(*volumes[3].Secret.SecretName).To(Equal("my-keystore-secret"))
			Expect(*volumes[4].Name).To(Equal("secret-1"))
			Expect(*volumes[4].Secret.SecretName).To(Equal("my-truststore-secret"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(5))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*mounts[2].Name).To(Equal("logs"))
			Expect(*mounts[3].Name).To(Equal("secret-0"))
			Expect(*mounts[3].MountPath).To(Equal("/secrets/my-keystore"))
			Expect(*mounts[3].ReadOnly).To(BeTrue())
			Expect(*mounts[4].Name).To(Equal("secret-1"))
			Expect(*mounts[4].MountPath).To(Equal("/secrets/my-truststore"))
			Expect(*mounts[4].ReadOnly).To(BeTrue())
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
			Expect(volumes).To(HaveLen(5)) // config + log4j-layout + logs + 1 plugin + 1 secret

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*volumes[2].Name).To(Equal("logs"))
			Expect(*volumes[3].Name).To(Equal("plugin-plugin-a"))
			Expect(*volumes[4].Name).To(Equal("secret-0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(5))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*mounts[2].Name).To(Equal("logs"))
			Expect(*mounts[3].Name).To(Equal("plugin-plugin-a"))
			Expect(*mounts[3].MountPath).To(Equal("/plugins/plugin-a"))
			Expect(*mounts[4].Name).To(Equal("secret-0"))
			Expect(*mounts[4].MountPath).To(Equal("/secrets/my-keystore"))
		})

		It("should have config and log4j-layout-template-json volumes when no plugins or secrets are specified", func() {
			cluster := newClusterWithSecrets(nil, nil)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3))
			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*volumes[2].Name).To(Equal("logs"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*mounts[2].Name).To(Equal("logs"))
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
			Expect(*dep.Spec.Template.Spec.ServiceAccountName).To(Equal(testClusterName))
		})

		It("should always set KAFKA_LOG4J_OPTS pointing to the custom Log4j2 config", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var log4jOpts *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == "KAFKA_LOG4J_OPTS" {
					log4jOpts = &envVars[i]
				}
			}
			Expect(log4jOpts).NotTo(BeNil())
			Expect(*log4jOpts.Value).To(Equal("-Dlog4j2.configurationFile=/config/connect-log4j2.properties"))
		})

		It("should set KAFKA_HEAP_OPTS with MaxRAMPercentage", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var heapOpts *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == "KAFKA_HEAP_OPTS" {
					heapOpts = &envVars[i]
				}
			}
			Expect(heapOpts).NotTo(BeNil())
			Expect(*heapOpts.Value).To(Equal("-XX:MaxRAMPercentage=75.0"))
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
			Expect(*jmxVol.Image.Reference).To(Equal("ghcr.io/b1zzu/kafka-connect-operator/jmx-exporter:1.6.0"))

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

		It("should have log4j-layout-template-json in CLASSPATH when no libraries are specified", func() {
			cluster := newClusterWithLibraries(nil)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3)) // config + log4j-layout + logs
			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*volumes[2].Name).To(Equal("logs"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*mounts[2].Name).To(Equal("logs"))

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var classpathEnv *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == classpathEnvName {
					classpathEnv = &envVars[i]
					break
				}
			}
			Expect(classpathEnv).NotTo(BeNil())
			Expect(*classpathEnv.Value).To(Equal("/opt/log4j-layout-template-json/*"))
		})

		It("should add a single library volume, mount, and CLASSPATH env", func() {
			cluster := newClusterWithLibraries([]kcv1alpha1.Library{
				{Name: "msk-iam-auth", Image: "ghcr.io/example/aws-msk-iam-auth:2.3.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(4)) // config + log4j-layout + logs + 1 library
			Expect(*volumes[3].Name).To(Equal("library-msk-iam-auth"))
			Expect(*volumes[3].Image.Reference).To(Equal("ghcr.io/example/aws-msk-iam-auth:2.3.0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(4))
			Expect(*mounts[3].Name).To(Equal("library-msk-iam-auth"))
			Expect(*mounts[3].MountPath).To(Equal("/libraries/msk-iam-auth"))
			Expect(*mounts[3].ReadOnly).To(BeTrue())

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var classpathEnv *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == classpathEnvName {
					classpathEnv = &envVars[i]
					break
				}
			}
			Expect(classpathEnv).NotTo(BeNil())
			Expect(*classpathEnv.Value).To(Equal("/opt/log4j-layout-template-json/*:/libraries/msk-iam-auth/*"))
		})

		It("should build colon-separated CLASSPATH for multiple libraries", func() {
			cluster := newClusterWithLibraries([]kcv1alpha1.Library{
				{Name: "lib-a", Image: "registry.example.com/lib-a:1.0"},
				{Name: "lib-b", Image: "registry.example.com/lib-b:2.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(5)) // config + log4j-layout + logs + 2 libraries
			Expect(*volumes[3].Name).To(Equal("library-lib-a"))
			Expect(*volumes[4].Name).To(Equal("library-lib-b"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(5))
			Expect(*mounts[3].MountPath).To(Equal("/libraries/lib-a"))
			Expect(*mounts[4].MountPath).To(Equal("/libraries/lib-b"))

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var classpathEnv *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == classpathEnvName {
					classpathEnv = &envVars[i]
					break
				}
			}
			Expect(classpathEnv).NotTo(BeNil())
			Expect(*classpathEnv.Value).To(Equal("/opt/log4j-layout-template-json/*:/libraries/lib-a/*:/libraries/lib-b/*"))
		})

		It("should propagate library pullPolicy when specified", func() {
			policy := corev1.PullAlways
			cluster := newClusterWithLibraries([]kcv1alpha1.Library{
				{Name: "lib", Image: "registry.example.com/lib:latest", PullPolicy: &policy},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(4)) // config + log4j-layout + logs + 1 library
			Expect(*volumes[3].Image.PullPolicy).To(Equal(corev1.PullAlways))
		})

		It("should not set library pullPolicy when not specified", func() {
			cluster := newClusterWithLibraries([]kcv1alpha1.Library{
				{Name: "lib", Image: "registry.example.com/lib:1.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(4)) // config + log4j-layout + logs + 1 library
			Expect(volumes[3].Image.PullPolicy).To(BeNil())
		})

		It("should add libraries together with plugins and secrets", func() {
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
					Plugins: []kcv1alpha1.Plugin{
						{Name: "plugin-a", Image: "registry.example.com/plugin-a:1.0"},
					},
					Libraries: []kcv1alpha1.Library{
						{Name: "lib-a", Image: "registry.example.com/lib-a:1.0"},
					},
					Logging: jsonLayoutLogging(),
					Secrets: []kcv1alpha1.SecretMount{
						{Name: "my-keystore", SecretRef: corev1.LocalObjectReference{Name: "my-keystore-secret"}},
					},
				},
			}
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(6)) // config + log4j-layout + logs + 1 plugin + 1 library + 1 secret
			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*volumes[2].Name).To(Equal("logs"))
			Expect(*volumes[3].Name).To(Equal("plugin-plugin-a"))
			Expect(*volumes[4].Name).To(Equal("library-lib-a"))
			Expect(*volumes[5].Name).To(Equal("secret-0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(6))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal(log4jLayoutVolName))
			Expect(*mounts[2].Name).To(Equal("logs"))
			Expect(*mounts[3].Name).To(Equal("plugin-plugin-a"))
			Expect(*mounts[3].MountPath).To(Equal("/plugins/plugin-a"))
			Expect(*mounts[4].Name).To(Equal("library-lib-a"))
			Expect(*mounts[4].MountPath).To(Equal("/libraries/lib-a"))
			Expect(*mounts[5].Name).To(Equal("secret-0"))
			Expect(*mounts[5].MountPath).To(Equal("/secrets/my-keystore"))

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var classpathEnv *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == classpathEnvName {
					classpathEnv = &envVars[i]
					break
				}
			}
			Expect(classpathEnv).NotTo(BeNil())
			Expect(*classpathEnv.Value).To(Equal("/opt/log4j-layout-template-json/*:/libraries/lib-a/*"))
		})

		It("should use default resources when spec.resources is nil", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			res := dep.Spec.Template.Spec.Containers[0].Resources
			Expect(res).NotTo(BeNil())
			Expect(*res.Requests).To(HaveKeyWithValue(corev1.ResourceCPU, resource.MustParse("250m")))
			Expect(*res.Requests).To(HaveKeyWithValue(corev1.ResourceMemory, resource.MustParse("1Gi")))
			Expect(*res.Limits).To(HaveKeyWithValue(corev1.ResourceCPU, resource.MustParse("1000m")))
			Expect(*res.Limits).To(HaveKeyWithValue(corev1.ResourceMemory, resource.MustParse("4Gi")))
		})

		It("should use custom resources when spec.resources is set", func() {
			cluster := newCluster(nil)
			cluster.Spec.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2000m"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			}
			dep := deploymentForCluster(cluster)

			res := dep.Spec.Template.Spec.Containers[0].Resources
			Expect(res).NotTo(BeNil())
			Expect(*res.Requests).To(HaveKeyWithValue(corev1.ResourceCPU, resource.MustParse("500m")))
			Expect(*res.Requests).To(HaveKeyWithValue(corev1.ResourceMemory, resource.MustParse("2Gi")))
			Expect(*res.Limits).To(HaveKeyWithValue(corev1.ResourceCPU, resource.MustParse("2000m")))
			Expect(*res.Limits).To(HaveKeyWithValue(corev1.ResourceMemory, resource.MustParse("8Gi")))
		})

		It("should handle partial resources (requests only, no limits)", func() {
			cluster := newCluster(nil)
			cluster.Spec.Resources = &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			}
			dep := deploymentForCluster(cluster)

			res := dep.Spec.Template.Spec.Containers[0].Resources
			Expect(res).NotTo(BeNil())
			Expect(*res.Requests).To(HaveKeyWithValue(corev1.ResourceCPU, resource.MustParse("100m")))
			Expect(*res.Requests).To(HaveKeyWithValue(corev1.ResourceMemory, resource.MustParse("512Mi")))
			Expect(res.Limits).To(BeNil())
		})

		It("should not have topology spread constraints when not specified", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			Expect(dep.Spec.Template.Spec.TopologySpreadConstraints).To(BeEmpty())
		})

		It("should apply a single topology spread constraint", func() {
			cluster := newCluster(nil)
			cluster.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
				{
					MaxSkew:           1,
					TopologyKey:       "topology.kubernetes.io/zone",
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/name": "kafka-connect",
						},
					},
				},
			}
			dep := deploymentForCluster(cluster)

			tscs := dep.Spec.Template.Spec.TopologySpreadConstraints
			Expect(tscs).To(HaveLen(1))
			Expect(*tscs[0].MaxSkew).To(Equal(int32(1)))
			Expect(*tscs[0].TopologyKey).To(Equal("topology.kubernetes.io/zone"))
			Expect(*tscs[0].WhenUnsatisfiable).To(Equal(corev1.DoNotSchedule))
			Expect(tscs[0].LabelSelector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/name", "kafka-connect"))
		})

		It("should apply multiple topology spread constraints", func() {
			cluster := newCluster(nil)
			cluster.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
				{
					MaxSkew:           1,
					TopologyKey:       "topology.kubernetes.io/zone",
					WhenUnsatisfiable: corev1.DoNotSchedule,
				},
				{
					MaxSkew:           2,
					TopologyKey:       "kubernetes.io/hostname",
					WhenUnsatisfiable: corev1.ScheduleAnyway,
				},
			}
			dep := deploymentForCluster(cluster)

			tscs := dep.Spec.Template.Spec.TopologySpreadConstraints
			Expect(tscs).To(HaveLen(2))
			Expect(*tscs[0].MaxSkew).To(Equal(int32(1)))
			Expect(*tscs[0].TopologyKey).To(Equal("topology.kubernetes.io/zone"))
			Expect(*tscs[0].WhenUnsatisfiable).To(Equal(corev1.DoNotSchedule))
			Expect(*tscs[1].MaxSkew).To(Equal(int32(2)))
			Expect(*tscs[1].TopologyKey).To(Equal("kubernetes.io/hostname"))
			Expect(*tscs[1].WhenUnsatisfiable).To(Equal(corev1.ScheduleAnyway))
		})

		It("should not have affinity when not specified", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			Expect(dep.Spec.Template.Spec.Affinity).To(BeNil())
		})

		It("should apply node affinity with required scheduling", func() {
			cluster := newCluster(nil)
			cluster.Spec.Affinity = &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "topology.kubernetes.io/zone",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"us-east-1a", "us-east-1b"},
									},
								},
							},
						},
					},
				},
			}
			dep := deploymentForCluster(cluster)

			affinity := dep.Spec.Template.Spec.Affinity
			Expect(affinity).NotTo(BeNil())
			Expect(affinity.NodeAffinity).NotTo(BeNil())
			req := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			Expect(req).NotTo(BeNil())
			Expect(req.NodeSelectorTerms).To(HaveLen(1))
			Expect(req.NodeSelectorTerms[0].MatchExpressions).To(HaveLen(1))
			expr := req.NodeSelectorTerms[0].MatchExpressions[0]
			Expect(*expr.Key).To(Equal("topology.kubernetes.io/zone"))
			Expect(*expr.Operator).To(Equal(corev1.NodeSelectorOpIn))
			Expect(expr.Values).To(ConsistOf("us-east-1a", "us-east-1b"))
		})

		It("should apply preferred pod anti-affinity", func() {
			cluster := newCluster(nil)
			cluster.Spec.Affinity = &corev1.Affinity{
				PodAntiAffinity: &corev1.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
						{
							Weight: 100,
							PodAffinityTerm: corev1.PodAffinityTerm{
								TopologyKey: "kubernetes.io/hostname",
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"app.kubernetes.io/name": "kafka-connect",
									},
								},
							},
						},
					},
				},
			}
			dep := deploymentForCluster(cluster)

			affinity := dep.Spec.Template.Spec.Affinity
			Expect(affinity).NotTo(BeNil())
			Expect(affinity.PodAntiAffinity).NotTo(BeNil())
			prefs := affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
			Expect(prefs).To(HaveLen(1))
			Expect(*prefs[0].Weight).To(Equal(int32(100)))
			Expect(*prefs[0].PodAffinityTerm.TopologyKey).To(Equal("kubernetes.io/hostname"))
			Expect(prefs[0].PodAffinityTerm.LabelSelector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/name", "kafka-connect"))
		})

		It("should not have tolerations when not specified", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			Expect(dep.Spec.Template.Spec.Tolerations).To(BeEmpty())
		})

		It("should apply a single toleration", func() {
			cluster := newCluster(nil)
			cluster.Spec.Tolerations = []corev1.Toleration{
				{
					Key:      "dedicated",
					Operator: corev1.TolerationOpEqual,
					Value:    "kafka-connect",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			}
			dep := deploymentForCluster(cluster)

			tols := dep.Spec.Template.Spec.Tolerations
			Expect(tols).To(HaveLen(1))
			Expect(*tols[0].Key).To(Equal("dedicated"))
			Expect(*tols[0].Operator).To(Equal(corev1.TolerationOpEqual))
			Expect(*tols[0].Value).To(Equal("kafka-connect"))
			Expect(*tols[0].Effect).To(Equal(corev1.TaintEffectNoSchedule))
		})

		It("should apply multiple tolerations", func() {
			cluster := newCluster(nil)
			cluster.Spec.Tolerations = []corev1.Toleration{
				{
					Key:      "dedicated",
					Operator: corev1.TolerationOpEqual,
					Value:    "kafka-connect",
					Effect:   corev1.TaintEffectNoSchedule,
				},
				{
					Key:      "node.kubernetes.io/not-ready",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoExecute,
				},
			}
			dep := deploymentForCluster(cluster)

			tols := dep.Spec.Template.Spec.Tolerations
			Expect(tols).To(HaveLen(2))
			Expect(*tols[0].Key).To(Equal("dedicated"))
			Expect(*tols[0].Operator).To(Equal(corev1.TolerationOpEqual))
			Expect(*tols[0].Value).To(Equal("kafka-connect"))
			Expect(*tols[0].Effect).To(Equal(corev1.TaintEffectNoSchedule))
			Expect(*tols[1].Key).To(Equal("node.kubernetes.io/not-ready"))
			Expect(*tols[1].Operator).To(Equal(corev1.TolerationOpExists))
			Expect(*tols[1].Effect).To(Equal(corev1.TaintEffectNoExecute))
		})

		It("should have log4j-layout-template-json volume and mount when log4jJsonLayout is enabled", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			// Volume
			var log4jVol *corev1ac.VolumeApplyConfiguration
			for i := range dep.Spec.Template.Spec.Volumes {
				if *dep.Spec.Template.Spec.Volumes[i].Name == log4jLayoutVolName {
					log4jVol = &dep.Spec.Template.Spec.Volumes[i]
				}
			}
			Expect(log4jVol).NotTo(BeNil())
			Expect(*log4jVol.Image.Reference).To(Equal("ghcr.io/b1zzu/kafka-connect-operator/log4j-layout-template-json:2.26.1"))

			// Mount
			var log4jMount *corev1ac.VolumeMountApplyConfiguration
			for i := range dep.Spec.Template.Spec.Containers[0].VolumeMounts {
				if *dep.Spec.Template.Spec.Containers[0].VolumeMounts[i].Name == log4jLayoutVolName {
					log4jMount = &dep.Spec.Template.Spec.Containers[0].VolumeMounts[i]
				}
			}
			Expect(log4jMount).NotTo(BeNil())
			Expect(*log4jMount.MountPath).To(Equal("/opt/log4j-layout-template-json"))
			Expect(*log4jMount.ReadOnly).To(BeTrue())
		})

		It("should use custom log4j-layout image when specified", func() {
			customImage := "example.com/custom/log4j-layout:1.0.0"
			pullPolicy := corev1.PullNever
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
					Logging: &kcv1alpha1.LoggingConfig{
						Log4jJsonLayout: &kcv1alpha1.Log4jJsonLayoutConfig{
							Image:      &customImage,
							PullPolicy: &pullPolicy,
						},
					},
				},
			}
			dep := deploymentForCluster(cluster)

			var log4jVol *corev1ac.VolumeApplyConfiguration
			for i := range dep.Spec.Template.Spec.Volumes {
				if *dep.Spec.Template.Spec.Volumes[i].Name == log4jLayoutVolName {
					log4jVol = &dep.Spec.Template.Spec.Volumes[i]
				}
			}
			Expect(log4jVol).NotTo(BeNil())
			Expect(*log4jVol.Image.Reference).To(Equal(customImage))
			Expect(*log4jVol.Image.PullPolicy).To(Equal(corev1.PullNever))
		})

		It("should not have log4j-layout-template-json volume when logging is set without log4jJsonLayout", func() {
			level := "DEBUG"
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
					Logging: &kcv1alpha1.LoggingConfig{
						Level: &level,
					},
				},
			}
			dep := deploymentForCluster(cluster)

			var log4jVol *corev1ac.VolumeApplyConfiguration
			for i := range dep.Spec.Template.Spec.Volumes {
				if *dep.Spec.Template.Spec.Volumes[i].Name == log4jLayoutVolName {
					log4jVol = &dep.Spec.Template.Spec.Volumes[i]
				}
			}
			Expect(log4jVol).To(BeNil())
		})

		It("should not have log4j-layout-template-json volume, mount, or CLASSPATH when logging is unset", func() {
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			dep := deploymentForCluster(cluster)

			for i := range dep.Spec.Template.Spec.Volumes {
				Expect(*dep.Spec.Template.Spec.Volumes[i].Name).NotTo(Equal(log4jLayoutVolName))
			}
			for i := range dep.Spec.Template.Spec.Containers[0].VolumeMounts {
				Expect(*dep.Spec.Template.Spec.Containers[0].VolumeMounts[i].Name).NotTo(Equal(log4jLayoutVolName))
			}

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			for i := range envVars {
				Expect(*envVars[i].Name).NotTo(Equal(classpathEnvName))
			}
		})

		It("should have CLASSPATH with log4j-layout-template-json when log4jJsonLayout is enabled", func() {
			cluster := newCluster(nil)
			dep := deploymentForCluster(cluster)

			envVars := dep.Spec.Template.Spec.Containers[0].Env
			var classpathEnv *corev1ac.EnvVarApplyConfiguration
			for i := range envVars {
				if *envVars[i].Name == classpathEnvName {
					classpathEnv = &envVars[i]
					break
				}
			}
			Expect(classpathEnv).NotTo(BeNil())
			Expect(*classpathEnv.Value).To(ContainSubstring("/opt/log4j-layout-template-json/*"))
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

			Expect(*sa.Name).To(Equal(testClusterName))
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
		It("should always contain connect-log4j2.properties with JSON console logging", func() {
			cluster := newCluster(nil)
			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.Data).To(HaveKey("connect-log4j2.properties"))

			log4jConfig := cm.Data["connect-log4j2.properties"]
			Expect(log4jConfig).To(ContainSubstring("JsonTemplateLayout"))
			Expect(log4jConfig).To(ContainSubstring("CONSOLE"))
			Expect(log4jConfig).NotTo(ContainSubstring("File"))
			Expect(log4jConfig).NotTo(ContainSubstring("RollingFile"))
		})

		It("should use PatternLayout instead of JsonTemplateLayout when log4jJsonLayout is not set", func() {
			cluster := &kcv1alpha1.Cluster{
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())

			log4jConfig := cm.Data["connect-log4j2.properties"]
			Expect(log4jConfig).To(ContainSubstring("PatternLayout"))
			Expect(log4jConfig).NotTo(ContainSubstring("JsonTemplateLayout"))
		})

		It("should include monitorInterval=30 in log4j2 properties for hot-reload", func() {
			cluster := newCluster(nil)
			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())

			log4jConfig := cm.Data["connect-log4j2.properties"]
			Expect(log4jConfig).To(ContainSubstring("monitorInterval=30"))
		})

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

		It("should default rootLogger.level to INFO when logging is nil", func() {
			cluster := newCluster(nil)
			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())

			log4jConfig := cm.Data["connect-log4j2.properties"]
			Expect(log4jConfig).To(ContainSubstring("rootLogger.level=INFO"))
			Expect(log4jConfig).NotTo(MatchRegexp(`logger\.\d+\.name`))
		})

		It("should set custom root logger level", func() {
			level := "DEBUG"
			cluster := newCluster(nil)
			cluster.Spec.Logging = &kcv1alpha1.LoggingConfig{Level: &level}

			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())

			log4jConfig := cm.Data["connect-log4j2.properties"]
			Expect(log4jConfig).To(ContainSubstring("rootLogger.level=DEBUG"))
		})

		It("should add per-logger overrides", func() {
			cluster := newCluster(nil)
			cluster.Spec.Logging = &kcv1alpha1.LoggingConfig{
				Loggers: []kcv1alpha1.LoggingLoggerConfig{
					{Name: "org.apache.kafka.connect", Level: "WARN"},
					{Name: "io.debezium", Level: "TRACE"},
				},
			}

			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())

			log4jConfig := cm.Data["connect-log4j2.properties"]
			Expect(log4jConfig).To(ContainSubstring("rootLogger.level=INFO"))
			Expect(log4jConfig).To(ContainSubstring("logger.0.name=org.apache.kafka.connect"))
			Expect(log4jConfig).To(ContainSubstring("logger.0.level=WARN"))
			Expect(log4jConfig).To(ContainSubstring("logger.1.name=io.debezium"))
			Expect(log4jConfig).To(ContainSubstring("logger.1.level=TRACE"))
		})

		It("should combine custom root level with per-logger overrides", func() {
			level := "ERROR"
			cluster := newCluster(nil)
			cluster.Spec.Logging = &kcv1alpha1.LoggingConfig{
				Level: &level,
				Loggers: []kcv1alpha1.LoggingLoggerConfig{
					{Name: "org.apache.kafka", Level: "DEBUG"},
				},
			}

			cm, err := configMapForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())

			log4jConfig := cm.Data["connect-log4j2.properties"]
			Expect(log4jConfig).To(ContainSubstring("rootLogger.level=ERROR"))
			Expect(log4jConfig).To(ContainSubstring("logger.0.name=org.apache.kafka"))
			Expect(log4jConfig).To(ContainSubstring("logger.0.level=DEBUG"))
		})
	})

	Describe("podDisruptionBudgetForCluster", func() {
		It("should default maxUnavailable to 1 when not specified", func() {
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			pdb := podDisruptionBudgetForCluster(cluster)

			Expect(*pdb.Spec.MaxUnavailable).To(Equal(intstr.FromInt(1)))
		})

		It("should use custom maxUnavailable when specified", func() {
			maxUnavailable := intstr.FromString("25%")
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config:         map[string]string{"bootstrap.servers": "localhost:9092"},
					MaxUnavailable: &maxUnavailable,
				},
			}
			pdb := podDisruptionBudgetForCluster(cluster)

			Expect(*pdb.Spec.MaxUnavailable).To(Equal(intstr.FromString("25%")))
		})

		It("should have correct label selector matching Deployment selector", func() {
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			pdb := podDisruptionBudgetForCluster(cluster)

			Expect(pdb.Spec.Selector.MatchLabels).To(HaveLen(2))
			Expect(pdb.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/name", "kafka-connect"))
			Expect(pdb.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/instance", testClusterName))
		})

		It("should have correct name and namespace", func() {
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			pdb := podDisruptionBudgetForCluster(cluster)

			Expect(*pdb.Name).To(Equal(testClusterName))
			Expect(*pdb.Namespace).To(Equal("default"))
		})

		It("should have owner reference set", func() {
			cluster := &kcv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: testClusterName, Namespace: "default"},
				Spec: kcv1alpha1.ClusterSpec{
					Config: map[string]string{"bootstrap.servers": "localhost:9092"},
				},
			}
			pdb := podDisruptionBudgetForCluster(cluster)

			Expect(pdb.OwnerReferences).To(HaveLen(1))
			Expect(*pdb.OwnerReferences[0].Name).To(Equal(testClusterName))
			Expect(*pdb.OwnerReferences[0].Controller).To(BeTrue())
			Expect(*pdb.OwnerReferences[0].BlockOwnerDeletion).To(BeTrue())
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
