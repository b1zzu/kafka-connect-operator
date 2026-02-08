package controller

import (
	"fmt"
	"sort"
	"strings"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
)

// TODO: Network policies

func deploymentForCluster(cluster *kcv1alpha1.Cluster) *appsv1ac.DeploymentApplyConfiguration {
	// TODO: Allow configuring different image
	image := "apache/kafka:4.1.1"

	// TODO: Allow to configure mount volumes for plugins

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

	// TODO: Configure liveness and readines prope
	// TODO: Configure Resources R: 250m/1G L: 1000m/4G
	// TODO: Allow configuration of topology spread

	name := fmt.Sprintf("%s-connect", cluster.Name)

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
						WithVolumeMounts(corev1ac.VolumeMount().
							WithName("config").
							WithMountPath("/config").
							WithReadOnly(true)).
						WithSecurityContext(corev1ac.SecurityContext().
							WithRunAsNonRoot(true).
							WithRunAsUser(65534).
							WithAllowPrivilegeEscalation(false).
							WithCapabilities(corev1ac.Capabilities().WithDrop("ALL"))),
					).
					WithVolumes(corev1ac.Volume().
						WithName("config").
						WithConfigMap(corev1ac.ConfigMapVolumeSource().
							WithName(configMapNameForCluster(cluster)))),
				),
			),
		)
}

func kafkaConnectPropertiesForCluster(cluster *kcv1alpha1.Cluster) map[string]string {
	properties := cluster.Spec.Properties

	// Hardcoded mandatory properties
	properties["listeners"] = "http://:8083"
	properties["rest.advertised.host.name"] = "${env:CONNECT_REST_ADVERTISED_HOST_NAME}"
	properties["rest.advertised.listener"] = "http"
	properties["rest.advertised.port"] = "8083"
	properties["rest.extension.classes"] = "" // cluster is secured using network policies

	// Env config provider
	// Allow to define additional properties as CONNECT_* envs
	// See: https://kafka.apache.org/41/configuration/configuration-providers/#envvarconfigprovider
	properties["config.providers"] = "env"
	properties["config.providers.env.class"] = "org.apache.kafka.common.config.provider.EnvVarConfigProvider"
	properties["config.providers.env.param.allowlist.pattern"] = "^CONNECT_.*"

	// TODO: File config providers

	return properties
}

func configMapNameForCluster(cluster *kcv1alpha1.Cluster) string {
	return fmt.Sprintf("%s-connect-config", cluster.Name)
}

func configMapForCluster(cluster *kcv1alpha1.Cluster) *corev1ac.ConfigMapApplyConfiguration {
	propertiesBuilder := &strings.Builder{}

	properties := kafkaConnectPropertiesForCluster(cluster)
	propertiesKeys := make([]string, 0, len(cluster.Spec.Properties))
	for k := range properties {
		propertiesKeys = append(propertiesKeys, k)
	}
	sort.Strings(propertiesKeys)
	for _, k := range propertiesKeys {
		fmt.Fprintf(propertiesBuilder, "%s=%s\n", k, properties[k])
	}

	name := configMapNameForCluster(cluster)
	return corev1ac.ConfigMap(name, cluster.Namespace).
		WithData(map[string]string{"connect.properties": propertiesBuilder.String()}).
		WithOwnerReferences(ownerReferenceForCluster(cluster))
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
