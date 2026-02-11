package controller

import (
	"fmt"
	"sort"
	"strings"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
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

func kafkaConnectConfigsForCluster(cluster *kcv1alpha1.Cluster) map[string]string {
	configs := cluster.Spec.Config

	// Hardcoded mandatory configs
	configs["listeners"] = "http://:8083"
	configs["rest.advertised.host.name"] = "${env:CONNECT_REST_ADVERTISED_HOST_NAME}"
	configs["rest.advertised.listener"] = "http"
	configs["rest.advertised.port"] = "8083"
	configs["rest.extension.classes"] = "" // cluster is secured using network policies

	// Env config provider
	// Allow to use CONNECT_* envs in config
	// See: https://kafka.apache.org/41/configuration/configuration-providers/#envvarconfigprovider
	configs["config.providers"] = "env"
	configs["config.providers.env.class"] = "org.apache.kafka.common.config.provider.EnvVarConfigProvider"
	configs["config.providers.env.param.allowlist.pattern"] = "^CONNECT_.*"

	// TODO: File config providers

	return configs
}

func configMapNameForCluster(cluster *kcv1alpha1.Cluster) string {
	return fmt.Sprintf("%s-connect-config", cluster.Name)
}

func configMapForCluster(cluster *kcv1alpha1.Cluster) *corev1ac.ConfigMapApplyConfiguration {
	configsBuilder := &strings.Builder{}

	configs := kafkaConnectConfigsForCluster(cluster)
	configsKeys := make([]string, 0, len(cluster.Spec.Config))
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
		WithOwnerReferences(ownerReferenceForCluster(cluster))
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

func ownerReferenceForCluster(cluster *kcv1alpha1.Cluster) *metav1ac.OwnerReferenceApplyConfiguration {
	return metav1ac.OwnerReference().
		WithAPIVersion(cluster.GetObjectKind().GroupVersionKind().GroupVersion().String()).
		WithKind(cluster.GetObjectKind().GroupVersionKind().Kind).
		WithName(cluster.GetName()).
		WithUID(cluster.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true)
}
