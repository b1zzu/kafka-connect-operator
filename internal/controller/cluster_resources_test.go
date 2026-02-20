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
	})

	Describe("kafkaConnectConfigsForCluster", func() {
		It("should not set plugin.path when no plugins are specified", func() {
			cluster := newCluster(nil)
			configs := kafkaConnectConfigsForCluster(cluster)
			Expect(configs).NotTo(HaveKey("plugin.path"))
		})

		It("should set plugin.path for a single plugin", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Image: "registry.example.com/plugin:1.0"},
			})
			configs := kafkaConnectConfigsForCluster(cluster)
			Expect(configs).To(HaveKeyWithValue("plugin.path", "/plugins/0"))
		})

		It("should set plugin.path with comma-separated paths for multiple plugins", func() {
			cluster := newCluster([]kcv1alpha1.Plugin{
				{Image: "registry.example.com/plugin-a:1.0"},
				{Image: "registry.example.com/plugin-b:2.0"},
				{Image: "registry.example.com/plugin-c:3.0"},
			})
			configs := kafkaConnectConfigsForCluster(cluster)
			Expect(configs).To(HaveKeyWithValue("plugin.path", "/plugins/0,/plugins/1,/plugins/2"))
		})
	})
})
