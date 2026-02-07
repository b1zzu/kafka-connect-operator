package controller

import (
	"fmt"
	"sort"
	"strings"

	kafkaconnectv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
)

func deploymentForCluster(cluster *kafkaconnectv1alpha1.Cluster) *appsv1ac.DeploymentApplyConfiguration {
	image := "apache/kafka:4.1.1"

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

	return appsv1ac.Deployment(cluster.Name, cluster.Namespace).
		WithOwnerReferences(ownerReferenceForCluster(cluster)).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(1).
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
							WithName(cluster.Name))),
				),
			),
		)
}

func configMapForCluster(cluster *kafkaconnectv1alpha1.Cluster) *corev1ac.ConfigMapApplyConfiguration {
	propertiesBuilder := &strings.Builder{}

	properties := cluster.Spec.Properties
	propertiesKeys := make([]string, 0, len(cluster.Spec.Properties))
	for k := range properties {
		propertiesKeys = append(propertiesKeys, k)
	}
	sort.Strings(propertiesKeys)
	for _, k := range propertiesKeys {
		fmt.Fprintf(propertiesBuilder, "%s=%s\n", k, properties[k])
	}

	return corev1ac.ConfigMap(cluster.Name, cluster.Namespace).
		WithData(map[string]string{"connect.properties": propertiesBuilder.String()}).
		WithOwnerReferences(ownerReferenceForCluster(cluster))
}

func ownerReferenceForCluster(cluster *kafkaconnectv1alpha1.Cluster) *metav1ac.OwnerReferenceApplyConfiguration {
	return metav1ac.OwnerReference().
		WithAPIVersion(cluster.GetObjectKind().GroupVersionKind().GroupVersion().String()).
		WithKind(cluster.GetObjectKind().GroupVersionKind().Kind).
		WithName(cluster.GetName()).
		WithUID(cluster.GetUID()).
		WithBlockOwnerDeletion(true).
		WithController(true)
}
