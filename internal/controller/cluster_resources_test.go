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

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
)

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
				{Image: "registry.example.com/plugin-a:1.0"},
				{Image: "registry.example.com/plugin-b:2.0"},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3)) // config + 2 plugins

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal("plugin-0"))
			Expect(*volumes[1].Image.Reference).To(Equal("registry.example.com/plugin-a:1.0"))
			Expect(*volumes[2].Name).To(Equal("plugin-1"))
			Expect(*volumes[2].Image.Reference).To(Equal("registry.example.com/plugin-b:2.0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal("plugin-0"))
			Expect(*mounts[1].MountPath).To(Equal("/plugins/0"))
			Expect(*mounts[1].ReadOnly).To(BeTrue())
			Expect(*mounts[2].Name).To(Equal("plugin-1"))
			Expect(*mounts[2].MountPath).To(Equal("/plugins/1"))
			Expect(*mounts[2].ReadOnly).To(BeTrue())
		})

		It("should propagate pullPolicy when specified", func() {
			policy := corev1.PullAlways
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Image: "registry.example.com/plugin:latest", PullPolicy: &policy},
			})
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(2))
			Expect(*volumes[1].Image.PullPolicy).To(Equal(corev1.PullAlways))
		})

		It("should not set pullPolicy when not specified", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Image: "registry.example.com/plugin:1.0"},
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
					{Image: "registry.example.com/plugin-a:1.0"},
				},
				[]kcv1alpha1.SecretMount{
					{Name: "my-keystore", SecretRef: corev1.LocalObjectReference{Name: "my-keystore-secret"}},
				},
			)
			dep := deploymentForCluster(cluster)

			volumes := dep.Spec.Template.Spec.Volumes
			Expect(volumes).To(HaveLen(3)) // config + 1 plugin + 1 secret

			Expect(*volumes[0].Name).To(Equal("config"))
			Expect(*volumes[1].Name).To(Equal("plugin-0"))
			Expect(*volumes[2].Name).To(Equal("secret-0"))

			mounts := dep.Spec.Template.Spec.Containers[0].VolumeMounts
			Expect(mounts).To(HaveLen(3))
			Expect(*mounts[0].Name).To(Equal("config"))
			Expect(*mounts[1].Name).To(Equal("plugin-0"))
			Expect(*mounts[1].MountPath).To(Equal("/plugins/0"))
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
				{Image: "registry.example.com/plugin:1.0"},
			})
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKeyWithValue("plugin.path", "/plugins/0"))
		})

		It("should set plugin.path with comma-separated paths for multiple plugins", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Image: "registry.example.com/plugin-a:1.0"},
				{Image: "registry.example.com/plugin-b:2.0"},
				{Image: "registry.example.com/plugin-c:3.0"},
			})
			configs, err := kafkaConnectConfigsForCluster(cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(configs).To(HaveKeyWithValue("plugin.path", "/plugins/0,/plugins/1,/plugins/2"))
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
						{Image: "registry.example.com/plugin:1.0"},
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
})
