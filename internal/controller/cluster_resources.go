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
	"slices"
	"strconv"
	"strings"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	"github.com/b1zzu/kafka-connect-operator/pkg/applycfg"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	netv1ac "k8s.io/client-go/applyconfigurations/networking/v1"
	policyv1ac "k8s.io/client-go/applyconfigurations/policy/v1"
)

func marshalProperties(props map[string]string) string {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	b := &strings.Builder{}
	for _, k := range keys {
		fmt.Fprintf(b, "%s=%s\n", k, props[k])
	}
	return b.String()
}

func log4j2ConfigForCluster(cluster *kcv1alpha1.Cluster) map[string]string {
	rootLevel := "INFO"
	if cluster.Spec.Logging != nil && cluster.Spec.Logging.Level != nil {
		rootLevel = *cluster.Spec.Logging.Level
	}

	props := map[string]string{
		"monitorInterval":              "30",
		"appender.0.type":              "Console",
		"appender.0.name":              "CONSOLE",
		"appender.0.direct":            "true", //nolint:goconst
		"appender.0.layout.type":       "JsonTemplateLayout",
		"rootLogger.level":             rootLevel,
		"rootLogger.appenderRef.0.ref": "CONSOLE",
	}

	if cluster.Spec.Logging != nil {
		for i, l := range cluster.Spec.Logging.Loggers {
			key := strconv.Itoa(i)
			props["logger."+key+".name"] = l.Name
			props["logger."+key+".level"] = l.Level
		}
	}

	return props
}

func deploymentForCluster(cluster *kcv1alpha1.Cluster) *appsv1ac.DeploymentApplyConfiguration {
	image := "docker.io/apache/kafka:4.3.1"
	if cluster.Spec.Image != nil {
		image = *cluster.Spec.Image
	}

	// Build plugin volumes and volume mounts from OCI image references
	pluginVolumes := make([]*corev1ac.VolumeApplyConfiguration, 0, len(cluster.Spec.Plugins))
	pluginMounts := make([]*corev1ac.VolumeMountApplyConfiguration, 0, len(cluster.Spec.Plugins))
	for _, plugin := range cluster.Spec.Plugins {
		volName := fmt.Sprintf("plugin-%s", plugin.Name)
		mountPath := fmt.Sprintf("/plugins/%s", plugin.Name)

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

	// Build library volumes and volume mounts from OCI image references
	libraryVolumes := make([]*corev1ac.VolumeApplyConfiguration, 0, len(cluster.Spec.Libraries))
	libraryMounts := make([]*corev1ac.VolumeMountApplyConfiguration, 0, len(cluster.Spec.Libraries))
	for _, library := range cluster.Spec.Libraries {
		volName := fmt.Sprintf("library-%s", library.Name)
		mountPath := fmt.Sprintf("/libraries/%s", library.Name)

		imgVolSrc := corev1ac.ImageVolumeSource().WithReference(library.Image)
		if library.PullPolicy != nil {
			imgVolSrc = imgVolSrc.WithPullPolicy(*library.PullPolicy)
		}

		libraryVolumes = append(libraryVolumes, corev1ac.Volume().
			WithName(volName).
			WithImage(imgVolSrc))
		libraryMounts = append(libraryMounts, corev1ac.VolumeMount().
			WithName(volName).
			WithMountPath(mountPath).
			WithReadOnly(true))
	}

	// Build secret volumes and volume mounts
	secretVolumes := make([]*corev1ac.VolumeApplyConfiguration, 0, len(cluster.Spec.Secrets))
	secretMounts := make([]*corev1ac.VolumeMountApplyConfiguration, 0, len(cluster.Spec.Secrets))
	for i, secret := range cluster.Spec.Secrets {
		volName := fmt.Sprintf("secret-%d", i)
		mountPath := fmt.Sprintf("/secrets/%s", secret.Name)

		secretVolumes = append(secretVolumes, corev1ac.Volume().
			WithName(volName).
			WithSecret(corev1ac.SecretVolumeSource().
				WithSecretName(secret.SecretRef.Name)))
		secretMounts = append(secretMounts, corev1ac.VolumeMount().
			WithName(volName).
			WithMountPath(mountPath).
			WithReadOnly(true))
	}

	// Build log4j-layout-template-json volume and mount (always present)
	log4jLayoutImage := "ghcr.io/b1zzu/kafka-connect-operator/log4j-layout-template-json:2.26.1"
	if cluster.Spec.Logging != nil && cluster.Spec.Logging.Log4jJsonLayout != nil &&
		cluster.Spec.Logging.Log4jJsonLayout.Image != nil {
		log4jLayoutImage = *cluster.Spec.Logging.Log4jJsonLayout.Image
	}

	imgVolSrc := corev1ac.ImageVolumeSource().WithReference(log4jLayoutImage)
	if cluster.Spec.Logging != nil && cluster.Spec.Logging.Log4jJsonLayout != nil &&
		cluster.Spec.Logging.Log4jJsonLayout.PullPolicy != nil {
		imgVolSrc = imgVolSrc.WithPullPolicy(*cluster.Spec.Logging.Log4jJsonLayout.PullPolicy)
	}

	log4jLayoutVolume := corev1ac.Volume().
		WithName("log4j-layout-template-json").
		WithImage(imgVolSrc)
	log4jLayoutMount := corev1ac.VolumeMount().
		WithName("log4j-layout-template-json").
		WithMountPath("/opt/log4j-layout-template-json").
		WithReadOnly(true)

	// Build JMX exporter volume and mount (conditional)
	var jmxVolumes []*corev1ac.VolumeApplyConfiguration
	var jmxMounts []*corev1ac.VolumeMountApplyConfiguration
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.JMXExporter != nil {
		jmxImage := "ghcr.io/b1zzu/kafka-connect-operator/jmx-exporter:1.5.0"
		if cluster.Spec.Metrics.JMXExporter.Image != nil {
			jmxImage = *cluster.Spec.Metrics.JMXExporter.Image
		}

		imgVolSrc := corev1ac.ImageVolumeSource().WithReference(jmxImage)
		if cluster.Spec.Metrics.JMXExporter.PullPolicy != nil {
			imgVolSrc = imgVolSrc.WithPullPolicy(*cluster.Spec.Metrics.JMXExporter.PullPolicy)
		}

		jmxVolumes = append(jmxVolumes, corev1ac.Volume().
			WithName("jmx-exporter").
			WithImage(imgVolSrc))
		jmxMounts = append(jmxMounts, corev1ac.VolumeMount().
			WithName("jmx-exporter").
			WithMountPath("/opt/jmx-exporter").
			WithReadOnly(true))
	}

	labels := selectorLabelsForCluster(cluster)

	// Pod labels: merge user-defined labels with internal labels (internal wins on conflict)
	podLabels := make(map[string]string, len(cluster.Spec.PodLabels)+len(labels))
	maps.Copy(podLabels, cluster.Spec.PodLabels)
	maps.Copy(podLabels, labels)

	configHash := ""
	if cluster.Status.ConfigHash != nil {
		configHash = *cluster.Status.ConfigHash
	}

	// Pod annotations: merge user-defined annotations with internal annotations (internal wins on conflict)
	podAnnotations := make(map[string]string, len(cluster.Spec.PodAnnotations)+1)
	maps.Copy(podAnnotations, cluster.Spec.PodAnnotations)
	podAnnotations["config/hash"] = configHash

	deploymentAnnotation := cluster.Spec.DeploymentAnnotations
	deploymentLabels := cluster.Spec.DeploymentLabels

	var replicas int32 = 1
	if cluster.Spec.Replicas != nil {
		replicas = *cluster.Spec.Replicas
	}

	name := cluster.Name

	volumes := append([]*corev1ac.VolumeApplyConfiguration{
		corev1ac.Volume().
			WithName("config").
			WithConfigMap(corev1ac.ConfigMapVolumeSource().
				WithName(configMapNameForCluster(cluster))),
		log4jLayoutVolume,
		corev1ac.Volume().
			WithName("logs").
			WithEmptyDir(corev1ac.EmptyDirVolumeSource()),
	}, pluginVolumes...)
	volumes = append(volumes, libraryVolumes...)
	volumes = append(volumes, secretVolumes...)
	volumes = append(volumes, jmxVolumes...)

	volumeMounts := append([]*corev1ac.VolumeMountApplyConfiguration{
		corev1ac.VolumeMount().
			WithName("config").
			WithMountPath("/config").
			WithReadOnly(true),
		log4jLayoutMount,
		corev1ac.VolumeMount().
			WithName("logs").
			WithMountPath("/opt/kafka/logs"),
	}, pluginMounts...)
	volumeMounts = append(volumeMounts, libraryMounts...)
	volumeMounts = append(volumeMounts, secretMounts...)
	volumeMounts = append(volumeMounts, jmxMounts...)

	// Build env vars
	envVars := []*corev1ac.EnvVarApplyConfiguration{
		corev1ac.EnvVar().
			WithName("CONNECT_REST_ADVERTISED_HOST_NAME").
			WithValueFrom(corev1ac.EnvVarSource().WithFieldRef(corev1ac.ObjectFieldSelector().WithFieldPath("status.podIP"))),
		corev1ac.EnvVar().
			WithName("KAFKA_HEAP_OPTS").
			WithValue("-XX:MaxRAMPercentage=75.0"),
		corev1ac.EnvVar().
			WithName("KAFKA_LOG4J_OPTS").
			WithValue("-Dlog4j2.configurationFile=/config/connect-log4j2.properties"),
	}
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.JMXExporter != nil {
		envVars = append(envVars, corev1ac.EnvVar().
			WithName("KAFKA_OPTS").
			WithValue("-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/config/jmx-exporter-config.yaml"))
	}
	classpathEntries := make([]string, 0, 1+len(cluster.Spec.Libraries))
	classpathEntries = append(classpathEntries, "/opt/log4j-layout-template-json/*")
	for _, library := range cluster.Spec.Libraries {
		classpathEntries = append(classpathEntries, fmt.Sprintf("/libraries/%s/*", library.Name))
	}
	envVars = append(envVars, corev1ac.EnvVar().
		WithName("CLASSPATH").
		WithValue(strings.Join(classpathEntries, ":")))

	// Build ports
	ports := []*corev1ac.ContainerPortApplyConfiguration{
		corev1ac.ContainerPort().
			WithContainerPort(8083).
			WithName("http"),
	}
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.JMXExporter != nil {
		ports = append(ports, corev1ac.ContainerPort().
			WithContainerPort(9404).
			WithName("metrics"))
	}

	// Build resource requirements
	resources := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1000m"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	if cluster.Spec.Resources != nil {
		resources = cluster.Spec.Resources
	}

	podSpec := corev1ac.PodSpec().
		WithServiceAccountName(serviceAccountNameForCluster(cluster)).
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsNonRoot(true)).
		WithContainers(corev1ac.Container().
			WithName("kafka-connect").
			WithImage(image).
			WithImagePullPolicy(corev1.PullIfNotPresent).
			WithCommand("/opt/kafka/bin/connect-distributed.sh", "/config/connect.properties").
			WithEnv(envVars...).
			WithPorts(ports...).
			WithResources(applycfg.ResourceRequirements(resources)).
			WithStartupProbe(corev1ac.Probe().
				WithHTTPGet(corev1ac.HTTPGetAction().
					WithPath("/health").
					WithPort(intstr.FromString("http"))).
				WithPeriodSeconds(10).
				WithTimeoutSeconds(5).
				WithFailureThreshold(30),
			).
			WithLivenessProbe(corev1ac.Probe().
				WithHTTPGet(corev1ac.HTTPGetAction().
					WithPath("/health").
					WithPort(intstr.FromString("http"))).
				WithPeriodSeconds(10).
				WithTimeoutSeconds(5).
				WithFailureThreshold(3),
			).
			WithReadinessProbe(corev1ac.Probe().
				WithHTTPGet(corev1ac.HTTPGetAction().
					WithPath("/health").
					WithPort(intstr.FromString("http"))).
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
		WithVolumes(volumes...).
		WithTopologySpreadConstraints(applycfg.TopologySpreadConstraints(cluster.Spec.TopologySpreadConstraints)...).
		WithTolerations(applycfg.Tolerations(cluster.Spec.Tolerations)...).
		WithAffinity(applycfg.Affinity(cluster.Spec.Affinity))

	return appsv1ac.Deployment(name, cluster.Namespace).
		WithOwnerReferences(ownerReferenceForCluster(cluster)).
		WithAnnotations(deploymentAnnotation).
		WithLabels(deploymentLabels).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(replicas).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(labels)).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(podLabels).
				WithAnnotations(podAnnotations).
				WithSpec(podSpec),
			),
		)
}

func kafkaConnectConfigsForCluster(cluster *kcv1alpha1.Cluster) (map[string]string, error) {
	// Operator-managed configs: single source of truth
	managedConfigs := map[string]string{
		"listeners":                                    "http://:8083", //nolint:goconst
		"rest.advertised.host.name":                    "${env:CONNECT_REST_ADVERTISED_HOST_NAME}",
		"rest.advertised.listener":                     "http",
		"rest.advertised.port":                         "8083",
		"rest.extension.classes":                       "", // cluster is secured using network policies
		"config.providers":                             "env,file",
		"config.providers.env.class":                   "org.apache.kafka.common.config.provider.EnvVarConfigProvider",
		"config.providers.env.param.allowlist.pattern": "^CONNECT_.*",
		"config.providers.file.class":                  "org.apache.kafka.common.config.provider.FileConfigProvider",
	}

	// Auto-configure plugin.path when plugins are present
	if len(cluster.Spec.Plugins) > 0 {
		paths := make([]string, len(cluster.Spec.Plugins))
		for i, plugin := range cluster.Spec.Plugins {
			paths[i] = fmt.Sprintf("/plugins/%s", plugin.Name)
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
		slices.Sort(conflicts)
		return nil, fmt.Errorf("spec.config contains operator-managed keys that cannot be overridden: %s", strings.Join(conflicts, ", "))
	}

	// Merge: copy user configs, then apply managed configs
	configs := make(map[string]string, len(cluster.Spec.Config)+len(managedConfigs))
	maps.Copy(configs, cluster.Spec.Config)
	maps.Copy(configs, managedConfigs)

	return configs, nil
}

func configMapNameForCluster(cluster *kcv1alpha1.Cluster) string {
	return fmt.Sprintf("%s-config", cluster.Name)
}

func configMapForCluster(cluster *kcv1alpha1.Cluster) (*corev1ac.ConfigMapApplyConfiguration, error) {
	configs, err := kafkaConnectConfigsForCluster(cluster)
	if err != nil {
		return nil, err
	}

	data := map[string]string{
		"connect.properties":        marshalProperties(configs), //nolint:goconst
		"connect-log4j2.properties": marshalProperties(log4j2ConfigForCluster(cluster)),
	}
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.JMXExporter != nil {
		data["jmx-exporter-config.yaml"] = "rules:\n- pattern: \".*\"\n"
	}

	name := configMapNameForCluster(cluster)
	return corev1ac.ConfigMap(name, cluster.Namespace).
		WithData(data).
		WithOwnerReferences(ownerReferenceForCluster(cluster)), nil
}

func serviceForCluster(cluster *kcv1alpha1.Cluster) *corev1ac.ServiceApplyConfiguration {
	labels := selectorLabelsForCluster(cluster)

	name := cluster.Name

	serviceAnnotations := cluster.Spec.ServiceAnnotations

	return corev1ac.Service(name, cluster.Namespace).
		WithAnnotations(serviceAnnotations).
		WithSpec(corev1ac.ServiceSpec().
			WithSelector(labels).
			WithPorts(corev1ac.ServicePort().
				WithProtocol(corev1.ProtocolTCP).
				WithPort(8083).
				WithTargetPort(intstr.FromString("http"))))
}

func networkPolicyForCluster(cluster *kcv1alpha1.Cluster, operatorNamespace string) *netv1ac.NetworkPolicyApplyConfiguration {
	podLabels := selectorLabelsForCluster(cluster)

	operatorPodLabels := map[string]string{
		"control-plane":          "controller-manager",
		"app.kubernetes.io/name": "kafka-connect-operator", //nolint:goconst
	}

	operatorNamespaceLabels := map[string]string{
		"kubernetes.io/metadata.name": operatorNamespace,
	}

	name := cluster.Name

	ingressRules := []*netv1ac.NetworkPolicyIngressRuleApplyConfiguration{
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
	}

	// Rule 3: Allow Prometheus scraping on metrics port (open to all)
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.JMXExporter != nil {
		ingressRules = append(ingressRules, netv1ac.NetworkPolicyIngressRule().
			WithPorts(
				netv1ac.NetworkPolicyPort().
					WithProtocol(corev1.ProtocolTCP).
					WithPort(intstr.FromInt(9404)),
			))
	}

	return netv1ac.NetworkPolicy(name, cluster.Namespace).
		WithOwnerReferences(ownerReferenceForCluster(cluster)).
		WithSpec(netv1ac.NetworkPolicySpec().
			WithPodSelector(metav1ac.LabelSelector().WithMatchLabels(podLabels)).
			WithPolicyTypes("Ingress").
			WithIngress(ingressRules...),
		)
}

func serviceAccountNameForCluster(cluster *kcv1alpha1.Cluster) string {
	return cluster.Name
}

func serviceAccountForCluster(cluster *kcv1alpha1.Cluster) *corev1ac.ServiceAccountApplyConfiguration {
	name := serviceAccountNameForCluster(cluster)

	serviceAccountAnnotations := cluster.Spec.ServiceAccountAnnotations

	return corev1ac.ServiceAccount(name, cluster.Namespace).
		WithAnnotations(serviceAccountAnnotations).
		WithOwnerReferences(ownerReferenceForCluster(cluster))
}

func podDisruptionBudgetForCluster(cluster *kcv1alpha1.Cluster) *policyv1ac.PodDisruptionBudgetApplyConfiguration {
	labels := selectorLabelsForCluster(cluster)

	maxUnavailable := intstr.FromInt(1)
	if cluster.Spec.MaxUnavailable != nil {
		maxUnavailable = *cluster.Spec.MaxUnavailable
	}

	name := cluster.Name

	return policyv1ac.PodDisruptionBudget(name, cluster.Namespace).
		WithOwnerReferences(ownerReferenceForCluster(cluster)).
		WithSpec(policyv1ac.PodDisruptionBudgetSpec().
			WithMaxUnavailable(maxUnavailable).
			WithSelector(metav1ac.LabelSelector().WithMatchLabels(labels)))
}

func selectorLabelsForCluster(cluster *kcv1alpha1.Cluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "kafka-connect", //nolint:goconst
		"app.kubernetes.io/instance": cluster.Name,
	}
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
