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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ClusterSpec defines the desired state of Cluster
type ClusterSpec struct {
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// Number of Kafka Connect replicas to run
	// +optional
	// +kubebuilder:validation:Minimum:=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Image used to deploy Kafka Connect, it should be based on docker.io/apache/kafka
	// +optional
	// +kubebuilder:default="docker.io/apache/kafka:4.2.0"
	Image *string `json:"image,omitempty"`

	// Kafka Connect configs: https://kafka.apache.org/41/configuration/kafka-connect-configs/
	// +optional
	Config map[string]string `json:"config"`

	// Plugins is a list of Kafka Connect plugins to mount from OCI images.
	// Each plugin image is mounted as a read-only volume in the Kafka Connect container.
	// The operator automatically configures plugin.path to include all plugin mount directories.
	// +optional
	// +listType=map
	// +listMapKey=name
	Plugins []Plugin `json:"plugins,omitempty"`

	// Libraries is a list of shared classpath libraries to mount from OCI images.
	// Each library image is mounted as a read-only volume at /libraries/{name} and
	// added to the CLASSPATH environment variable. Unlike plugins (which use isolated
	// classloaders via plugin.path), libraries are loaded on the shared classpath.
	// +optional
	// +listType=map
	// +listMapKey=name
	Libraries []Library `json:"libraries,omitempty"`

	// Secrets is a list of Kubernetes Secrets to mount into the Kafka Connect pods.
	// Each Secret is mounted read-only at /secrets/{name}.
	// Secret content changes do not trigger pod restarts because the operator does not
	// control secret content. Kubernetes handles secret volume updates automatically
	// via the kubelet's periodic sync.
	// +optional
	// +listType=map
	// +listMapKey=name
	Secrets []SecretMount `json:"secrets,omitempty"`

	// NetworkPolicy configuration
	// +optional
	NetworkPolicy *NetworkPolicyConfig `json:"networkPolicy,omitempty"`

	// Metrics configuration for enabling Prometheus metrics collection.
	// When not set, metrics collection is disabled.
	// +optional
	Metrics *MetricsConfig `json:"metrics,omitempty"`

	// ServiceAnnotations defines custom annotations for the Service
	// +optional
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`

	// ServiceAccountAnnotations defines custom annotations for the ServiceAccount
	// +optional
	ServiceAccountAnnotations map[string]string `json:"serviceAccountAnnotations,omitempty"`

	// DeploymentAnnotations defines custom annotations for the Deployment
	// +optional
	DeploymentAnnotations map[string]string `json:"deploymentAnnotations,omitempty"`

	// PodAnnotations defines custom annotations for the Pods.
	// Merged with internal annotations; internal keys take precedence.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// PodLabels defines custom labels for the Pods.
	// Applied only to the pod template metadata, NOT to the Deployment selector.
	// Merged with internal labels; internal keys take precedence.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`
}

// Plugin defines a Kafka Connect plugin to be mounted from an OCI image.
type Plugin struct {
	// Name is the identifier for this plugin mount.
	// The plugin will be mounted at /plugins/{name}.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// OCI image reference containing the plugin artifacts.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Pull policy for the OCI image.
	// Defaults to Always if :latest tag is specified, or IfNotPresent otherwise.
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	PullPolicy *corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// Library defines a shared classpath library to be mounted from an OCI image.
type Library struct {
	// Name is the identifier for this library mount.
	// The library will be mounted at /libraries/{name}.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// OCI image reference containing the library artifacts.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Pull policy for the OCI image.
	// Defaults to Always if :latest tag is specified, or IfNotPresent otherwise.
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	PullPolicy *corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// SecretMount defines a Kubernetes Secret to mount into Kafka Connect pods.
type SecretMount struct {
	// Name is the identifier for this secret mount.
	// The secret will be mounted at /secrets/{name}.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// SecretRef is a reference to a Kubernetes Secret in the same namespace.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// NetworkPolicyConfig defines the NetworkPolicy configuration
type NetworkPolicyConfig struct {
	// Enabled controls whether NetworkPolicies should be created
	// Default: true
	// +optional
	// +kubebuilder:default:=true
	Enabled *bool `json:"enabled,omitempty"`
}

// MetricsConfig defines the metrics configuration for the Kafka Connect cluster.
type MetricsConfig struct {
	// JMXExporter configures Prometheus metrics using the JMX Exporter Java agent.
	// +optional
	JMXExporter *JMXExporterConfig `json:"jmxExporter,omitempty"`
	// Future: StrimziReporter *StrimziReporterConfig `json:"strimziReporter,omitempty"`
}

// JMXExporterConfig configures the Prometheus JMX Exporter Java agent.
type JMXExporterConfig struct {
	// Image is the OCI image containing the JMX Exporter Java agent JAR.
	// +optional
	// +kubebuilder:default="ghcr.io/b1zzu/kafka-connect-operator/jmx-exporter:1.5.0"
	Image *string `json:"image,omitempty"`

	// Pull policy for the JMX Exporter image.
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	PullPolicy *corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// ClusterStatus defines the observed state of Cluster.
type ClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the Cluster resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// The hash of the Cluster ConfigMap app
	// If no config is applied yet, this will be null.
	// +optional
	ConfigHash *string `json:"configHash,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Cluster is the Schema for the clusters API
type Cluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Cluster
	// +required
	Spec ClusterSpec `json:"spec"`

	// status defines the observed state of Cluster
	// +optional
	Status ClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ClusterList contains a list of Cluster
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Cluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
}
