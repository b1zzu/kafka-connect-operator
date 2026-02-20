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
	"fmt"
	"maps"
	"sort"
	"strings"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	netv1ac "k8s.io/client-go/applyconfigurations/networking/v1"
)

func deploymentForCluster(cluster *kcv1alpha1.Cluster) *appsv1ac.DeploymentApplyConfiguration {
	image := "docker.io/apache/kafka:4.2.0"
	if cluster.Spec.Image != nil {
		image = *cluster.Spec.Image
	}

	// Build plugin volumes and volume mounts from OCI image references
	pluginVolumes := make([]*corev1ac.VolumeApplyConfiguration, 0, len(cluster.Spec.Plugins))
	pluginMounts := make([]*corev1ac.VolumeMountApplyConfiguration, 0, len(cluster.Spec.Plugins))
	for i, plugin := range cluster.Spec.Plugins {
		volName := fmt.Sprintf("plugin-%d", i)
		mountPath := fmt.Sprintf("/plugins/%d", i)

		imgVolSrc := corev1ac.ImageVolumeSource().WithReference(plugin.Image)
		if plugin.PullPolicy != nil {
			imgVolSrc = imgVolSrc.WithPullPolicy(*plugin.PullPolicy)
		}

		pluginVolumes = append(pluginVolumes, corev1ac.Volume().
			WithName(volName).
			WithImage(imgVolSrc))
		pluginMounts = append(pluginMounts, corev1ac.VolumeMount().
			WithName(volName).
			WithMountPath(mountPath).
			WithReadOnly(true))
	}

	labels := map[string]string{
		"app.kubernetes.io/name":     "kafka-connect",
		"app.kubernetes.io/instance": cluster.Name,
	}

	configHash := ""
	if cluster.Status.ConfigHash != nil {
		configHash = *cluster.Status.ConfigHash
	}

	podAnnotations := map[string]string{
		"config/hash": configHash,
	}

	var replicas int32 = 1
	if cluster.Spec.Replicas != nil {
		replicas = *cluster.Spec.Replicas
	}

	// TODO: Allow configuration of topology spread

	name := fmt.Sprintf("%s-connect", cluster.Name)

	volumes := append([]*corev1ac.VolumeApplyConfiguration{
		corev1ac.Volume().
			WithName("config").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(configMapNameForCluster(cluster))),
	}, pluginVolumes...)

	volumeMounts := append([]*corev1ac.VolumeMountApplyConfiguration{
		corev1ac.VolumeMount().
			WithName("config").
			WithMountPath("/config").
			WithReadOnly(true),
	}, pluginMounts...)

	return appsv1ac.Deployment(name, cluster.Namespace).
		WithOwnerReferences(ownerReferenceForCluster(cluster)).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(replicas).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(labels)).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(labels).
				WithAnnotations(podAnnotations).
				WithSpec(corev1ac.PodSpec().
					WithSecurityContext(corev1ac.PodSecurityContext().
						WithRunAsNonRoot(true)).
					WithContainers(corev1ac.Container().
						WithName("kafka-connect").
						WithImage(image).
						WithImagePullPolicy(corev1.PullIfNotPresent).
						WithCommand("/opt/kafka/bin/connect-distributed.sh", "/config/connect.properties").
						WithEnv(corev1ac.EnvVar().
							WithName("CONNECT_REST_ADVERTISED_HOST_NAME").
							WithValueFrom(corev1ac.EnvVarSource().WithFieldRef(corev1ac.ObjectFieldSelector().WithFieldPath("status.podIP")))).
						WithPorts(corev1ac.ContainerPort().
							WithContainerPort(8083).
							WithName("http")).
						WithResources(corev1ac.ResourceRequirements().
							WithRequests(corev1.ResourceList{ // Request and limits are based on standard cloud ratio 1CPU 4GB
								corev1.ResourceCPU:    resource.MustParse("250m"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							}).
							WithLimits(corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1000m"),
								corev1.ResourceMemory: resource.MustParse("4Gi"),
							})).
						WithLivenessProbe(corev1ac.Probe().
							WithHTTPGet(corev1ac.HTTPGetAction().
								WithPath("/health").
								WithPort(intstr.FromString("http"))).
							WithInitialDelaySeconds(30).
							WithPeriodSeconds(10).
							WithTimeoutSeconds(5).
							WithFailureThreshold(3),
						).
						WithReadinessProbe(corev1ac.Probe().
							WithHTTPGet(corev1ac.HTTPGetAction().
								WithPath("/health").
								WithPort(intstr.FromString("http"))).
							WithInitialDelaySeconds(10).
							WithPeriodSeconds(5).
							WithTimeoutSeconds(3).
							WithFailureThreshold(3),
						).
						WithVolumeMounts(volumeMounts...).
						WithSecurityContext(corev1ac.SecurityContext().
							WithRunAsNonRoot(true).
							WithRunAsUser(65534).
							WithAllowPrivilegeEscalation(false).
							WithCapabilities(corev1ac.Capabilities().WithDrop("ALL"))),
					).
					WithVolumes(volumes...),
				),
			),
		)
}

func kafkaConnectConfigsForCluster(cluster *kcv1alpha1.Cluster) (map[string]string, error) {
	// Operator-managed configs: single source of truth
	managedConfigs := map[string]string{
		"listeners":                                    "http://:8083",
		"rest.advertised.host.name":                    "${env:CONNECT_REST_ADVERTISED_HOST_NAME}",
		"rest.advertised.listener":                     "http",
		"rest.advertised.port":                         "8083",
		"rest.extension.classes":                       "", // cluster is secured using network policies
		"config.providers":                             "env",
		"config.providers.env.class":                   "org.apache.kafka.common.config.provider.EnvVarConfigProvider",
		"config.providers.env.param.allowlist.pattern": "^CONNECT_.*",
	}

	// Auto-configure plugin.path when plugins are present
	if len(cluster.Spec.Plugins) > 0 {
		paths := make([]string, len(cluster.Spec.Plugins))
		for i := range cluster.Spec.Plugins {
			paths[i] = fmt.Sprintf("/plugins/%d", i)
		}
		managedConfigs["plugin.path"] = strings.Join(paths, ",")
	}

	// Check for conflicts between user config and operator-managed keys
	var conflicts []string
	for key := range managedConfigs {
		if _, exists := cluster.Spec.Config[key]; exists {
			conflicts = append(conflicts, key)
		}
	}
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return nil, fmt.Errorf("spec.config contains operator-managed keys that cannot be overridden: %s", strings.Join(conflicts, ", "))
	}

	// Merge: copy user configs, then apply managed configs
	configs := make(map[string]string, len(cluster.Spec.Config)+len(managedConfigs))
	maps.Copy(configs, cluster.Spec.Config)
	maps.Copy(configs, managedConfigs)

	// TODO: File config providers

	return configs, nil
}

func configMapNameForCluster(cluster *kcv1alpha1.Cluster) string {
	return fmt.Sprintf("%s-connect-config", cluster.Name)
}

func configMapForCluster(cluster *kcv1alpha1.Cluster) (*corev1ac.ConfigMapApplyConfiguration, error) {
	configs, err := kafkaConnectConfigsForCluster(cluster)
	if err != nil {
		return nil, err
	}

	configsBuilder := &strings.Builder{}
	configsKeys := make([]string, 0, len(configs))
	for k := range configs {
		configsKeys = append(configsKeys, k)
	}
	sort.Strings(configsKeys)
	for _, k := range configsKeys {
		fmt.Fprintf(configsBuilder, "%s=%s\n", k, configs[k])
	}

	name := configMapNameForCluster(cluster)
	return corev1ac.ConfigMap(name, cluster.Namespace).
		WithData(map[string]string{"connect.properties": configsBuilder.String()}).
		WithOwnerReferences(ownerReferenceForCluster(cluster)), nil
}

func serviceForCluster(cluster *kcv1alpha1.Cluster) *corev1ac.ServiceApplyConfiguration {
	labels := map[string]string{
		"app.kubernetes.io/name":     "kafka-connect",
		"app.kubernetes.io/instance": cluster.Name,
	}

	name := fmt.Sprintf("%s-connect", cluster.Name)

	return corev1ac.Service(name, cluster.Namespace).
		WithSpec(corev1ac.ServiceSpec().
			WithSelector(labels).
			WithPorts(corev1ac.ServicePort().
				WithProtocol(corev1.ProtocolTCP).
				WithPort(8083).
				WithTargetPort(intstr.FromString("http"))))
}

func networkPolicyForCluster(cluster *kcv1alpha1.Cluster, operatorNamespace string) *netv1ac.NetworkPolicyApplyConfiguration {
	podLabels := map[string]string{
		"app.kubernetes.io/name":     "kafka-connect",
		"app.kubernetes.io/instance": cluster.Name,
	}

	operatorPodLabels := map[string]string{
		"control-plane":          "controller-manager",
		"app.kubernetes.io/name": "kafka-connect-operator",
	}

	operatorNamespaceLabels := map[string]string{
		"kubernetes.io/metadata.name": operatorNamespace,
	}

	name := fmt.Sprintf("%s-connect", cluster.Name)

	return netv1ac.NetworkPolicy(name, cluster.Namespace).
		WithOwnerReferences(ownerReferenceForCluster(cluster)).
		WithSpec(netv1ac.NetworkPolicySpec().
			WithPodSelector(metav1ac.LabelSelector().WithMatchLabels(podLabels)).
			WithPolicyTypes("Ingress").
			WithIngress(
				// Rule 1: Allow operator access
				netv1ac.NetworkPolicyIngressRule().
					WithFrom(
						netv1ac.NetworkPolicyPeer().
							WithNamespaceSelector(metav1ac.LabelSelector().WithMatchLabels(operatorNamespaceLabels)).
							WithPodSelector(metav1ac.LabelSelector().WithMatchLabels(operatorPodLabels)),
					).
					WithPorts(
						netv1ac.NetworkPolicyPort().
							WithProtocol(corev1.ProtocolTCP).
							WithPort(intstr.FromInt(8083)),
					),
				// Rule 2: Allow inter-pod communication (distributed mode)
				netv1ac.NetworkPolicyIngressRule().
					WithFrom(
						netv1ac.NetworkPolicyPeer().
							WithPodSelector(metav1ac.LabelSelector().WithMatchLabels(podLabels)),
					).
					WithPorts(
						netv1ac.NetworkPolicyPort().
							WithProtocol(corev1.ProtocolTCP).
							WithPort(intstr.FromInt(8083)),
					),
			),
		)
}

func ownerReferenceForCluster(cluster *kcv1alpha1.Cluster) *metav1ac.OwnerReferenceApplyConfiguration {
	return metav1ac.OwnerReference().
		WithAPIVersion(cluster.GetObjectKind().GroupVersionKind().GroupVersion().String()).
		WithKind(cluster.GetObjectKind().GroupVersionKind().Kind).
		WithName(cluster.GetName()).
		WithUID(cluster.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true)
}
