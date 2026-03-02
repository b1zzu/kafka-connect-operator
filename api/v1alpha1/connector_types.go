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

// ConnectorState represents the desired state of a connector.
// +kubebuilder:validation:Enum=running;paused;stopped
type ConnectorState string

const (
	ConnectorStateRunning ConnectorState = "running"
	ConnectorStatePaused  ConnectorState = "paused"
	ConnectorStateStopped ConnectorState = "stopped"
)

// OffsetsSpec configures the ConfigMap reference for offset operations (export or import).
// This field alone has no effect. To trigger an operation, set the annotation
// kafka-connect.b1zzu.net/offsets to "export", "import", or "reset".
type OffsetsSpec struct {
	// Reference to the ConfigMap used for the offset operation.
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
}

// ConnectorSpec defines the desired state of Connector
type ConnectorSpec struct {
	// The name of the Kafka Connect cluster hosting the connector.
	ClusterRef corev1.LocalObjectReference `json:"cluster"`

	// Connector configs:
	// Source: https://kafka.apache.org/41/configuration/kafka-connect-configs/#source-connector-configs
	// Sink: https://kafka.apache.org/41/configuration/kafka-connect-configs/#sink-connector-configs
	// MirrorMaker: https://kafka.apache.org/41/configuration/mirrormaker-configs/
	// +optional
	Config map[string]string `json:"config"`

	// The desired state of the connector: running, paused, or stopped.
	// +kubebuilder:default=running
	// +optional
	State ConnectorState `json:"state,omitempty"`

	// Configuration for exporting offsets to a ConfigMap.
	// This field alone has no effect. To trigger an export,
	// set the annotation kafka-connect.b1zzu.net/offsets: export
	// +optional
	ExportOffsets *OffsetsSpec `json:"exportOffsets,omitempty"`

	// Configuration for importing offsets from a ConfigMap.
	// This field alone has no effect. To trigger an import,
	// set the annotation kafka-connect.b1zzu.net/offsets: import
	// The connector must be in the stopped state before importing.
	// +optional
	ImportOffsets *OffsetsSpec `json:"importOffsets,omitempty"`
}

// ConnectorStatus defines the observed state of Connector.
type ConnectorStatus struct {
	// conditions represent the current state of the Connector resource.
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

	// connector represents the state of the connector as reported by Kafka Connect.
	// +optional
	Connector *ConnectorStateStatus `json:"connector,omitempty"`

	// tasks represents the state of individual connector tasks as reported by Kafka Connect.
	// +optional
	Tasks []TaskStateStatus `json:"tasks,omitempty"`

	// lastRestartAt is the timestamp of the last connector or task restart.
	// +optional
	LastRestartAt *metav1.Time `json:"lastRestartAt,omitempty"`

	// restartCount is the total number of restarts performed on this connector.
	// +optional
	RestartCount int32 `json:"restartCount,omitempty"`

	// +optional
	LastExportedOffsetsAt *metav1.Time `json:"lastExportedOffsetsAt,omitempty"`
	// +optional
	LastImportedOffsetsAt *metav1.Time `json:"lastImportedOffsetsAt,omitempty"`
	// +optional
	LastResetOffsetsAt *metav1.Time `json:"lastResetOffsetsAt,omitempty"`
}

// ConnectorStateStatus represents the state of the connector as reported by Kafka Connect.
type ConnectorStateStatus struct {
	// state is the current state of the connector (e.g., RUNNING, PAUSED, FAILED, STOPPED, UNASSIGNED).
	State string `json:"state"`
	// workerID is the Kafka Connect worker that the connector is assigned to.
	WorkerID string `json:"workerID"`
	// trace contains the error trace if the connector is in FAILED state.
	// +optional
	Trace string `json:"trace,omitempty"`
}

// TaskStateStatus represents the state of a connector task as reported by Kafka Connect.
type TaskStateStatus struct {
	// id is the task identifier.
	ID int `json:"id"`
	// state is the current state of the task (e.g., RUNNING, FAILED).
	State string `json:"state"`
	// workerID is the Kafka Connect worker that the task is assigned to.
	WorkerID string `json:"workerID"`
	// trace contains the error trace if the task is in FAILED state.
	// +optional
	Trace string `json:"trace,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=".status.connector.state"

// Connector is the Schema for the connectors API
type Connector struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Connector
	// +required
	Spec ConnectorSpec `json:"spec"`

	// status defines the observed state of Connector
	// +optional
	Status ConnectorStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ConnectorList contains a list of Connector
type ConnectorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Connector `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Connector{}, &ConnectorList{})
}
