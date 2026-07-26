/*
Copyright 2026.

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
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os/exec"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	kafkaconnect "github.com/b1zzu/kafka-connect-operator/pkg/kafka-connect"
)

const (
	typeReadyConnector         = "Ready"
	connectorFinalizer         = "kafka-connect.b1zzu.net/connector"
	connectorRestartAnnotation = "kafka-connect.b1zzu.net/restart"

	connectorStatusRunning    = "RUNNING"
	connectorStatusPaused     = "PAUSED"
	connectorStatusStopped    = "STOPPED"
	connectorStatusUnassigned = "UNASSIGNED"
	connectorStatusFailed     = "FAILED"

	offsetsAnnotation   = "kafka-connect.b1zzu.net/offsets"
	offsetsConfigMapKey = "offsets.json"
)

type connectorReconcileFunc func(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error)

// ConnectorReconciler reconciles a Connector object
type ConnectorReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// ReconcileInterval is the interval between each reconcile loop
	// to monitor the status of the connector.
	ReconcileInterval time.Duration

	// RestartFailedConnectorBackoff is the time to wait before attempting
	// to restart a failed connector after it was updated, or already restarted.
	RestartFailedConnectorBackoff time.Duration

	// ConnectorStateTransitionBackoff is the time to wait before attempting
	// to change the connector state while it's already changing.
	ConnectorStateTransitionBackoff time.Duration

	// NewKafkaConnectClientFunc creates a Kafka Connect client for the given connector.
	NewKafkaConnectClientFunc func(connector *kcv1alpha1.Connector) (*kafkaconnect.Client, error)
}

// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=connectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=connectors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=connectors/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *ConnectorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Start reconcile")

	connector, err := r.getConnector(ctx, req.NamespacedName)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
	}

	// Skip reconciliation loop if paused
	if pause, ok := connector.GetAnnotations()[pauseReconciliationAnnotation]; ok {
		if pause == pauseReconciliationTrue {
			log.Info(fmt.Sprintf(
				"Skip reconciliation, because reconciliation is paused. Remove the '%s' annotation to unpause it.",
				pauseReconciliationAnnotation))
			return ctrl.Result{}, nil
		}
	}

	// Initialize status conditions
	connector, err = r.initializeStatusConditions(ctx, connector)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
	}

	for _, reconcile := range []connectorReconcileFunc{
		r.reconcileConnectorFinalizer,
		r.reconcileConnector,
		r.reconcileConnectorState,
		r.reconcileConnectorRestart,
		r.reconcileConnectorOffsets,
		r.reconcileConnectorStatus,
	} {
		connector, err = reconcile(ctx, connector)
		if err != nil {
			return ctrl.Result{}, err
		}

		if connector == nil {
			// When a reconcile function return err nil and connector nil, it means that
			// the reconcile loop should be restarted triggered by a stuatus change, if
			// not the RequeueAfter 1s will ensure that the loop is quickly restarted
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	requeueAfter := r.controllerRequeueAfter(&connector.Status)

	log.Info("Done reconcile", "requeueAfter", requeueAfter)

	// Monitor the connector status every minute
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConnectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kcv1alpha1.Connector{}).
		Named("connector").
		Complete(r)
}

func getDesiredConnectorState(connector *kcv1alpha1.Connector) kcv1alpha1.ConnectorState {
	if connector.Spec.State == "" {
		return kcv1alpha1.ConnectorStateRunning
	}
	return connector.Spec.State
}

func (r *ConnectorReconciler) getConnector(ctx context.Context, key client.ObjectKey) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	connector := &kcv1alpha1.Connector{}
	err := r.Get(ctx, key, connector)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Resource has been deleted")
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get Connector: %w", err)
	}
	return connector, nil
}

func (r *ConnectorReconciler) initializeStatusConditions(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	if len(connector.Status.Conditions) == 0 {
		log.Info("Setting connector initial condition")

		err := r.updateStatusCondition(ctx, connector, metav1.Condition{
			Type:   typeReadyConnector,
			Status: metav1.ConditionUnknown,
			Reason: "Reconciling",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize condition: %w", err)
		}

		// The Connector resource was updated, it must be refetched or the reconciliation loop restarted
		return nil, nil
	}

	return connector, nil
}

func (r *ConnectorReconciler) reconcileConnector(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Create Kafka Connect client
	kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to create the client: %w", err)
	}

	// Get existing connector from Kafka Connect
	actualConnector, err := kafkaConnect.GetConnector(ctx, connector.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get the connector: %w", err)
	}

	// Connector doesn't exist, create it
	if actualConnector == nil {
		log.Info("Creating connector")

		desiredState := getDesiredConnectorState(connector)

		// Create the connector
		desiredConnector := &kafkaconnect.Connector{
			Name:   connector.Name,
			Config: connector.Spec.Config,
		}

		switch desiredState {
		case connectorStatusPaused:
			desiredConnector.InitialState = connectorStatusPaused
		case connectorStatusStopped:
			desiredConnector.InitialState = connectorStatusStopped
		}

		err = kafkaConnect.CreateConnector(ctx, desiredConnector)
		if err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
				"Failed", "Create", "Failed to create the connector: %v", err)

			return nil, fmt.Errorf("failed to create the connector: %w", err)
		}

		r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal,
			"Created", "Create", "Connector created")

		now := metav1.Now()
		connector.Status.LastUpdatedAt = &now
		err = r.updateStatusCondition(ctx, connector, metav1.Condition{
			Type:   typeReadyConnector,
			Status: metav1.ConditionUnknown,
			Reason: "Starting",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update status: %w", err)
		}

		log.Info("Connector created successfully, starting now")

		// Return nil to restart reconciliation and proceed to status sync
		return nil, nil
	}

	actualConfig := actualConnector.Config
	desiredConfig := connector.Spec.Config

	// Remove the name from the actualConfig otherwise it will
	// enter in an infinite update loop because the desired config
	// is always going to be different from the actual config.
	delete(actualConfig, "name")

	// Connector exists, compare configs
	if !connectorConfigsEqual(actualConfig, desiredConfig) {
		log.Info("Updating connector")

		// Update the connector config
		err = kafkaConnect.UpdateConnectorConfig(ctx, connector.Name, desiredConfig)
		if err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
				"Failed", "UpdateConfig", "Failed to update the connector config: %v", err)

			return nil, fmt.Errorf("failed to update the connector config: %w", err)
		}

		r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal,
			"Updated", "UpdateConfig", "Connector config updated")

		now := metav1.Now()
		connector.Status.LastUpdatedAt = &now
		err := r.updateStatusCondition(ctx, connector, metav1.Condition{
			Type:   typeReadyConnector,
			Status: metav1.ConditionUnknown,
			Reason: "Updating",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update status: %w", err)
		}

		log.Info("Connector config updated successfully")

		// Return nil to restart reconciliation and proceed to status sync
		return nil, nil
	}

	// Config matches, continue to status sync
	return connector, nil
}

// Reconcile the state of the connector, the state is weather the connector should be running, paused or stopped
// and it's controlled using the state property.
//
// The controller will keep track of weather the connector is already transitioning from one state to another,
// and backoff from retrying again.
func (r *ConnectorReconciler) reconcileConnectorState(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	desiredState := getDesiredConnectorState(connector)

	kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to create the client: %w", err)
	}

	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get the connector status: %w", err)
	}

	if status == nil {
		// Connector not registered yet (e.g. just created), retry shortly
		log.Info("Connector status not found")
		return nil, nil
	}

	actualState := kcv1alpha1.ConnectorState(strings.ToLower(status.Connector.State))

	// Remove the stateTransitionTo status if the connector has reached the desired state
	stateTransitionTo := connector.Status.StateTransitionTo
	if stateTransitionTo != nil && *stateTransitionTo == actualState {
		log.Info("Connector state transition completed", "state", actualState)

		switch actualState {
		case kcv1alpha1.ConnectorStateRunning:
			r.recordNormalEvent(connector, "Resume", "Resumed", "Connector resumed")
		case kcv1alpha1.ConnectorStatePaused:
			r.recordNormalEvent(connector, "Pause", "Paused", "Connector paused")
		case kcv1alpha1.ConnectorStateStopped:
			r.recordNormalEvent(connector, "Stop", "Stopped", "Connector stopped")
		}

		connector.Status.StateTransitionTo = nil
		if err := r.Status().Update(ctx, connector); err != nil {
			return nil, fmt.Errorf("failed to update status after state change completed: %w", err)
		}
	}

	if actualState == desiredState {
		return connector, nil
	}

	// Skip state reconciliation if the connector is in an unrecoverable or unknown state
	switch actualState {
	case kcv1alpha1.ConnectorStateRunning, kcv1alpha1.ConnectorStatePaused, kcv1alpha1.ConnectorStateStopped:
		// known states, proceed with reconciliation
	default:
		log.Info("Skip connector state reconciliation, connector is in an unreconcilable state",
			"actual", actualState, "desired", desiredState)
		return connector, nil
	}

	// Skip state reconcile if the connector is transitioning and the backoff timeout is not yet expired
	if stateTransitionTo != nil && r.stateTransitionBackoff(connector) {
		return connector, nil
	}

	log.Info("Reconciling connector state", "actual", actualState, "desired", desiredState)

	switch desiredState {
	case kcv1alpha1.ConnectorStateRunning:
		log.Info("Resuming connector")
		err = kafkaConnect.ResumeConnector(ctx, connector.Name)
		if err != nil {
			r.recordWarningEvent(connector, "Resume", "FailedResuming", "Failed to resume connector: %v", err)
			return nil, fmt.Errorf("failed to resume connector: %w", err)
		}
		r.recordNormalEvent(connector, "Resume", "Resuming", "Resuming connector")

	case kcv1alpha1.ConnectorStatePaused:
		if actualState == kcv1alpha1.ConnectorStateStopped {
			r.recordWarningEvent(connector, "Pause", "CannotPause", "Cannot pause the connector because it is already stopped")
			log.Info("Cannot pause the connector because it is stopped")
			// Ignore the desired state
			return connector, nil

		} else {
			log.Info("Pausing connector")
			err = kafkaConnect.PauseConnector(ctx, connector.Name)
			if err != nil {
				r.recordWarningEvent(connector, "Pause", "FailedPausing", "Failed to pause connector: %v", err)
				return nil, fmt.Errorf("failed to pause the connector: %w", err)
			}
			r.recordNormalEvent(connector, "Pause", "Pausing", "Pausing connector")

		}
	case kcv1alpha1.ConnectorStateStopped:
		log.Info("Stopping connector")
		err = kafkaConnect.StopConnector(ctx, connector.Name)
		if err != nil {
			r.recordWarningEvent(connector, "Stop", "FailedStopping", "Failed to stop connector: %v", err)
			return nil, fmt.Errorf("failed to stop the connector: %w", err)
		}
		r.recordNormalEvent(connector, "Stop", "Stopping", "Stopping connector")
	}

	now := metav1.Now()
	connector.Status.StateTransitionTo = &desiredState
	connector.Status.LastStateTransitionAt = &now
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update status after state change: %w", err)
	}

	return nil, nil
}

func (r *ConnectorReconciler) reconcileConnectorRestart(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	if connector.Annotations[connectorRestartAnnotation] != "true" {
		return connector, nil
	}

	log.Info("Restarting connector (manual restart requested)")

	kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to create the client: %w", err)
	}

	if err := kafkaConnect.RestartConnector(ctx, connector.Name); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"FailedRestart", "Restart", "Failed to restart connector: %v", err)
		return nil, fmt.Errorf("failed to restart connector: %w", err)
	}

	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal,
		"Restarted", "Restart", "Connector restarted")

	now := metav1.Now()
	connector.Status.LastRestartAt = &now
	connector.Status.RestartCount++
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update connector status after restart: %w", err)
	}

	// Remove the restart annotation (even on failure to prevent retry loops)
	delete(connector.Annotations, connectorRestartAnnotation)
	if err := r.Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to remove restart annotation: %w", err)
	}

	// Return nil to restart reconciliation
	return nil, nil
}

func (r *ConnectorReconciler) reconcileConnectorOffsets(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	annotation := connector.Annotations[offsetsAnnotation]
	if annotation == "" {
		return connector, nil
	}

	log.Info("Reconciling connector offsets", "operation", annotation)

	switch annotation {
	case "export":
		return r.reconcileExportOffsets(ctx, connector)
	case "import":
		return r.reconcileImportOffsets(ctx, connector)
	case "reset":
		return r.reconcileResetOffsets(ctx, connector)
	default:
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorOffsets", "ManagingOffsets", "Unknown offsets annotation value: %s", annotation)
		return r.removeOffsetsAnnotation(ctx, connector)
	}
}

func (r *ConnectorReconciler) reconcileExportOffsets(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	if connector.Spec.ExportOffsets == nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorExportOffsets", "ExportingOffsets", "spec.exportOffsets.configMapRef is required for export")
		return r.removeOffsetsAnnotation(ctx, connector)
	}

	kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to create the client: %w", err)
	}

	offsets, err := kafkaConnect.GetConnectorOffsets(ctx, connector.Name)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"FailedExportOffsets", "ExportingOffsets", "Failed to get offsets: %v", err)
		return nil, fmt.Errorf("failed to get connector offsets: %w", err)
	}

	offsetsJSON, err := json.Marshal(offsets)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"FailedExportOffsets", "ExportingOffsets", "Failed to marshal offsets: %v", err)
		return nil, fmt.Errorf("failed to marshal offsets: %w", err)
	}

	cmName := connector.Spec.ExportOffsets.ConfigMapRef.Name
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, client.ObjectKey{Name: cmName, Namespace: connector.Namespace}, cm)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
				"FailedExportOffsets", "ExportingOffsets", "Failed to get ConfigMap: %v", err)
			return nil, fmt.Errorf("failed to get ConfigMap: %w", err)
		}

		// Create ConfigMap
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: connector.Namespace,
			},
			Data: map[string]string{
				offsetsConfigMapKey: string(offsetsJSON),
			},
		}
		if err := r.Create(ctx, cm); err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
				"FailedExportOffsets", "ExportingOffsets", "Failed to create ConfigMap: %v", err)
			return nil, fmt.Errorf("failed to create ConfigMap: %w", err)
		}
	} else {
		// Update ConfigMap
		cm.Data = map[string]string{
			offsetsConfigMapKey: string(offsetsJSON),
		}
		if err := r.Update(ctx, cm); err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
				"FailedExportOffsets", "ExportingOffsets", "Failed to update ConfigMap: %v", err)
			return nil, fmt.Errorf("failed to update ConfigMap: %w", err)
		}
	}

	log.Info("Exported connector offsets", "configMap", cmName)
	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal,
		"ExportedOffsets", "ExportingOffsets", "Exported offsets to ConfigMap %s", cmName)

	connector.Status.LastExportedOffsetsAt = &metav1.Time{Time: time.Now()}
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update status with export timestamp: %w", err)
	}

	return r.removeOffsetsAnnotation(ctx, connector)
}

func (r *ConnectorReconciler) reconcileImportOffsets(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	if connector.Spec.ImportOffsets == nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorImportOffsets", "ImportingOffsets", "spec.importOffsets.configMapRef is required for import")
		return r.removeOffsetsAnnotation(ctx, connector)
	}

	kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to create the client: %w", err)
	}

	// Validate connector is stopped
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"FailedImportOffsets", "ImportingOffsets", "Failed to get connector status: %v", err)
		return nil, fmt.Errorf("failed to get connector status: %w", err)
	}

	if status == nil {
		log.Info("Connector status not found")
		return nil, nil
	}

	if status.Connector.State != connectorStatusStopped {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorImportOffsets", "ImportingOffsets", "Connector must be stopped to import offsets, current state: %s", status.Connector.State)
		// Keep annotation for retry
		return connector, nil
	}

	// Read ConfigMap
	cmName := connector.Spec.ImportOffsets.ConfigMapRef.Name
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, client.ObjectKey{Name: cmName, Namespace: connector.Namespace}, cm)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorImportOffsets", "ImportingOffsets", "Failed to get ConfigMap %s: %v", cmName, err)
		return nil, fmt.Errorf("failed to get ConfigMap: %w", err)
	}

	offsetsJSON, ok := cm.Data[offsetsConfigMapKey]
	if !ok {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorImportOffsets", "ImportingOffsets", "ConfigMap %s missing key %s", cmName, offsetsConfigMapKey)
		return nil, fmt.Errorf("ConfigMap %s missing key %s", cmName, offsetsConfigMapKey)
	}

	offsets := &kafkaconnect.ConnectorOffsets{}
	if err := json.Unmarshal([]byte(offsetsJSON), offsets); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorImportOffsets", "ImportingOffsets", "Failed to parse offsets from ConfigMap: %v", err)
		return nil, fmt.Errorf("failed to parse offsets from ConfigMap: %w", err)
	}

	if err := kafkaConnect.PatchConnectorOffsets(ctx, connector.Name, offsets); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"FailedImportOffsets", "ImportingOffsets", "Failed to import offsets: %v", err)
		return nil, fmt.Errorf("failed to patch connector offsets: %w", err)
	}

	log.Info("Imported connector offsets", "configMap", cmName)
	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal,
		"ImportedOffsets", "ImportingOffsets", "Imported offsets from ConfigMap %s", cmName)

	connector.Status.LastImportedOffsetsAt = &metav1.Time{Time: time.Now()}
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update status with import timestamp: %w", err)
	}

	return r.removeOffsetsAnnotation(ctx, connector)
}

func (r *ConnectorReconciler) reconcileResetOffsets(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to create the client: %w", err)
	}

	// Validate connector is stopped
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"FailedResetOffsets", "ResettingOffsets", "Failed to get connector status: %v", err)
		return nil, fmt.Errorf("failed to get connector status: %w", err)
	}

	if status == nil {
		log.Info("Connector status not found")
		return nil, nil
	}

	if status.Connector.State != connectorStatusStopped {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"ErrorResetOffsets", "ResettingOffsets", "Connector must be stopped to reset offsets, current state: %s", status.Connector.State)
		// Keep annotation for retry
		return connector, nil
	}

	if err := kafkaConnect.DeleteConnectorOffsets(ctx, connector.Name); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
			"FailedResetOffsets", "ResettingOffsets", "Failed to reset offsets: %v", err)
		return nil, fmt.Errorf("failed to delete connector offsets: %w", err)
	}

	log.Info("Reset connector offsets")
	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal,
		"ResetOffsets", "ResettingOffsets", "Reset connector offsets")

	connector.Status.LastResetOffsetsAt = &metav1.Time{Time: time.Now()}
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update status with reset timestamp: %w", err)
	}

	return r.removeOffsetsAnnotation(ctx, connector)
}

func (r *ConnectorReconciler) removeOffsetsAnnotation(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	delete(connector.Annotations, offsetsAnnotation)
	if err := r.Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to remove offsets annotation: %w", err)
	}
	// Return nil to restart reconciliation with updated resource
	return nil, nil
}

func (r *ConnectorReconciler) reconcileConnectorStatus(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Save current status before applying changes
	// previousStatus := connector.Status.DeepCopy()

	// Create Kafka Connect client
	kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to create the client: %w", err)
	}

	// Get connector status from Kafka Connect
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		r.recordWarningEvent(connector, "FailedStatus", "Reconcile", "Faiiled to get connector status: %v", err)
		return nil, fmt.Errorf("failed to get connector status: %w", err)
	}

	if status == nil {
		log.Info("Connector status not found")
		return nil, nil
	}

	// Restart failed connectors/tasks when desired state is running
	desiredState := getDesiredConnectorState(connector)
	if desiredState == kcv1alpha1.ConnectorStateRunning {
		err := r.restartFailedConnectorAndTasks(ctx, kafkaConnect, connector, status)
		if err != nil {
			return nil, err
		}
	}

	// Populate connector and task status from Kafka Connect
	connector.Status.Connector = &kcv1alpha1.ConnectorStateStatus{
		State:    status.Connector.State,
		WorkerID: status.Connector.WorkerID,
		Trace:    status.Connector.Trace,
	}

	connector.Status.Tasks = make([]kcv1alpha1.TaskStateStatus, len(status.Tasks))
	for i, task := range status.Tasks {
		connector.Status.Tasks[i] = kcv1alpha1.TaskStateStatus{
			ID:       task.ID,
			State:    task.State,
			WorkerID: task.WorkerID,
			Trace:    task.Trace,
		}
	}

	// Remove the old "Running" condition type, superseded by "Ready"
	meta.RemoveStatusCondition(&connector.Status.Conditions, "Running")

	// Set condition
	newCondition := mapConnectorStatusToCondition(status, desiredState)
	newCondition.ObservedGeneration = connector.Generation
	meta.SetStatusCondition(&connector.Status.Conditions, newCondition)

	log.Info("Updating connector status")
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update connector status: %w", err)
	}
	return connector, nil
}

// Try to restart the connector if in failed state and each failed task,
// if successfully restarted it will update the LastRestartAt and RestartCount
func (r *ConnectorReconciler) restartFailedConnectorAndTasks(
	ctx context.Context,
	kafkaConnect *kafkaconnect.Client,
	connector *kcv1alpha1.Connector,
	status *kafkaconnect.ConnectorStatus,
) error {
	log := logf.FromContext(ctx)

	restarted := false

	if status.Connector.State == connectorStatusFailed {
		if r.restartFailedConnectorBackoff(connector) {
			log.Info("Skip restart of recently updated or restarted connector")
			return nil
		}

		log.Info("Restarting failed connector")
		if err := kafkaConnect.RestartConnector(ctx, connector.Name); err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedRestart", "Restart", "Failed to restart failed connector: %v", err)
			return fmt.Errorf("failed to restart failed connector: %w", err)
		}
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "Restarted", "Restart", "Restarted failed connector")
		restarted = true
	}

	var failedTaskIDs []int
	for _, task := range status.Tasks {
		if task.State == connectorStatusFailed {
			failedTaskIDs = append(failedTaskIDs, task.ID)
		}
	}

	if len(failedTaskIDs) > 0 {
		if r.restartFailedConnectorBackoff(connector) {
			log.Info("Skip restart of recently updated or restarted connector")
			return nil
		}

		log.Info("Restarting failed tasks", "taskIDs", failedTaskIDs)
		for _, taskID := range failedTaskIDs {
			if err := kafkaConnect.RestartTask(ctx, connector.Name, taskID); err != nil {
				r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedRestart", "Restart", "Failed to restart task %d: %v", taskID, err)
				return fmt.Errorf("failed to restart failed task with id %d: %w", taskID, err)
			}
		}
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "Restarted", "Restart", "Restarted %d failed task(s)", len(failedTaskIDs))
		restarted = true
	}

	// Update restart status if any restarts were performed
	if restarted {
		now := metav1.Now()
		connector.Status.LastRestartAt = &now
		connector.Status.RestartCount++
	}
	return nil
}

// restartFailedConnectorBackoff return true if the controller should backoff from trying to restart the connector.
func (r *ConnectorReconciler) restartFailedConnectorBackoff(connector *kcv1alpha1.Connector) bool {
	lastUpdatedAt := connector.Status.LastUpdatedAt
	lastRestartAt := connector.Status.LastRestartAt

	if lastUpdatedAt == nil && lastRestartAt == nil {
		return false
	}

	last := lastUpdatedAt
	if last == nil {
		last = lastRestartAt
	}
	if lastRestartAt != nil && lastRestartAt.After(last.Time) {
		last = lastRestartAt
	}

	return last.After(time.Now().Add(-r.RestartFailedConnectorBackoff))
}

// stateTransitionBackoff return true if the controller should backoff from retry the state change or make another state change.
func (r *ConnectorReconciler) stateTransitionBackoff(connector *kcv1alpha1.Connector) bool {
	lastStateTransitionAt := connector.Status.LastStateTransitionAt

	if lastStateTransitionAt == nil {
		return false
	}

	return lastStateTransitionAt.After(time.Now().Add(-r.ConnectorStateTransitionBackoff))
}

func (r *ConnectorReconciler) reconcileConnectorFinalizer(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Check if the connector is being deleted
	if !connector.DeletionTimestamp.IsZero() {
		// Connector is being deleted
		log.Info("Deleting connector")

		kafkaConnect, err := r.NewKafkaConnectClientFunc(connector)
		if err != nil {
			return nil, fmt.Errorf("failed to create the client: %w", err)
		}

		// Delete the connector from Kafka Connect
		err = kafkaConnect.DeleteConnector(ctx, connector.Name)
		if err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning,
				"Failed", "Delete", "Failed to delete the connector: %v", err)

			return nil, fmt.Errorf("failed to delete the connector: %w", err)
		}

		log.Info("Connector deleted successfully, removing finalizer")

		// Remove the finalizer
		controllerutil.RemoveFinalizer(connector, connectorFinalizer)
		err = r.Update(ctx, connector)
		if err != nil {
			return nil, fmt.Errorf("failed to remove finalizer: %w", err)
		}

		// Return nil to stop reconciliation (resource is being deleted)
		return nil, nil
	}

	// Connector is not being deleted, ensure finalizer is present
	if !controllerutil.ContainsFinalizer(connector, connectorFinalizer) {
		log.Info("Adding finalizer")

		controllerutil.AddFinalizer(connector, connectorFinalizer)
		err := r.Update(ctx, connector)
		if err != nil {
			return nil, fmt.Errorf("failed to add finalizer: %w", err)
		}

		// Return nil to restart reconciliation with updated resource
		return nil, nil
	}

	return connector, nil
}

func (r *ConnectorReconciler) updateStatusCondition(
	ctx context.Context,
	connector *kcv1alpha1.Connector,
	condition metav1.Condition,
) error {
	log := logf.FromContext(ctx)
	log.V(1).Info("Update Connector status condition", "type", condition.Type, "status", condition.Status)

	if condition.ObservedGeneration == 0 {
		condition.ObservedGeneration = connector.Generation
	}

	meta.SetStatusCondition(&connector.Status.Conditions, condition)
	err := r.Status().Update(ctx, connector)
	if err != nil {
		return fmt.Errorf("failed to update Connector status condition: %w", err)
	}

	return nil
}

func (r *ConnectorReconciler) recordNormalEvent(connector *kcv1alpha1.Connector, action, reason, note string) {
	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal, reason, action, note)
}

func (r *ConnectorReconciler) recordWarningEvent(connector *kcv1alpha1.Connector, action, reason, note string, args ...any) {
	r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, reason, action, note, args...)
}

// NewDefaultKafkaConnectClientFunc returns a factory that creates Kafka Connect
// clients using the in-cluster service endpoint.
func NewDefaultKafkaConnectClientFunc(connector *kcv1alpha1.Connector) (*kafkaconnect.Client, error) {
	endpoint := fmt.Sprintf("http://%s.%s:8083", connector.Spec.ClusterRef.Name, connector.Namespace)
	return kafkaconnect.NewClient(endpoint), nil
}

type PortForwardProcess struct {
	Port      int
	IsRunning bool
}

var portForwardProcesses = map[string]*PortForwardProcess{}

// NewPortForwardKafkaConnectClientFunc is a factory func that start the kubectl port-forward
// in the bacgrkound and return the the Kafka Connect client using the port-forward as endpoint.
func NewPortForwardKafkaConnectClientFunc(connector *kcv1alpha1.Connector) (*kafkaconnect.Client, error) {
	log := logf.FromContext(context.TODO())

	k := fmt.Sprintf("%s/%s", connector.Namespace, connector.Spec.ClusterRef.Name)
	var process *PortForwardProcess
	if p, ok := portForwardProcesses[k]; ok {
		process = p
	}

	// Check if kubectl port-forward is running
	if process != nil && !process.IsRunning {
		process = nil
	}

	if process == nil {
		process = &PortForwardProcess{
			Port:      49152 + rand.N(65535-49152+1), // Random port between 49152 and 65535
			IsRunning: false,
		}

		// Start a new kubectl port-forward process
		cmd := exec.Command(
			"kubectl", "port-forward",
			"--namespace", connector.Namespace,
			fmt.Sprintf("services/%s", connector.Spec.ClusterRef.Name),
			fmt.Sprintf("%d:8083", process.Port))

		// Run a bacgrkound goroutine to update the process when exit
		log.Info("Starting kubect port-forward", "command", strings.Join(cmd.Args, " "))
		go func() {
			process.IsRunning = true
			out, err := cmd.CombinedOutput()
			log.Error(err, "Kubectl port-forward exit", "output", string(out))
			process.IsRunning = false
		}()

		// Wait 1 second after starting to process to make sure the kubectl port-fowarding
		// did not fail immediately
		time.Sleep(time.Second)

		if !process.IsRunning {
			return nil, fmt.Errorf("failed to run kubectl port-forward on local port %d", process.Port)
		}

		portForwardProcesses[k] = process
	}

	// Kubectl port-forward is running from at-least 1 second
	endpoint := fmt.Sprintf("http://localhost:%d", process.Port)
	return kafkaconnect.NewClient(endpoint), nil
}

func connectorConfigsEqual(actual, desired map[string]string) bool {
	if len(actual) != len(desired) {
		return false
	}

	for k, v := range desired {
		if actual[k] != v {
			return false
		}
	}

	return true
}

func countFailedTasks(tasks []kafkaconnect.ConnectorStatusTask) int {
	count := 0
	for _, task := range tasks {
		if task.State == connectorStatusFailed {
			count++
		}
	}
	return count
}

// mapConnectorStatusToCondition builds the Ready condition to reflects whether the connector has reached
// the spec desired state. A stopped connector whose spec asks for stopped is Ready=True;
// a mismatch between actual and desired state is Ready=Unknown (reconciling).
func mapConnectorStatusToCondition(status *kafkaconnect.ConnectorStatus, desiredState kcv1alpha1.ConnectorState) metav1.Condition {
	condition := metav1.Condition{
		Type: typeReadyConnector,
	}

	switch status.Connector.State {
	case connectorStatusFailed:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Failed"
		condition.Message = fmt.Sprintf("Connector failed with trace: %s", strings.ReplaceAll(status.Connector.Trace, "\n\t", "\n"))
		return condition
	case connectorStatusUnassigned:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Unassigned"
		condition.Message = "Connector is unassigned"
		return condition
	}

	if failedTasks := countFailedTasks(status.Tasks); failedTasks > 0 {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "FailedTasks"
		condition.Message = fmt.Sprintf("Connector has %d failed task(s) out of %d", failedTasks, len(status.Tasks))
		return condition
	}

	switch status.Connector.State {
	case connectorStatusRunning, connectorStatusPaused, connectorStatusStopped:
		// known states, evaluate against the desired state below
	default:
		condition.Status = metav1.ConditionUnknown
		condition.Reason = "Unknown"
		condition.Message = fmt.Sprintf("Connector in unknown state: %s", status.Connector.State)
		return condition
	}

	actualState := kcv1alpha1.ConnectorState(strings.ToLower(status.Connector.State))
	if actualState != desiredState {
		condition.Status = metav1.ConditionUnknown
		condition.Reason = "Reconciling"
		condition.Message = fmt.Sprintf("Connector is transitioning from %s to %s", actualState, desiredState)
		return condition
	}

	condition.Status = metav1.ConditionTrue
	switch actualState {
	case kcv1alpha1.ConnectorStateRunning:
		condition.Reason = "Running"
		condition.Message = fmt.Sprintf("Connector is running with %d task(s)", len(status.Tasks))
	case kcv1alpha1.ConnectorStatePaused:
		condition.Reason = "Paused"
		condition.Message = "Connector is paused"
	case kcv1alpha1.ConnectorStateStopped:
		condition.Reason = "Stopped"
		condition.Message = "Connector is stopped"
	}

	return condition
}

// controllerRequeueAfter returns an exponential requeue delay: it stays short right after any
// connector activity (state transition, update, restart) and grows toward r.ReconcileInterval
func (r *ConnectorReconciler) controllerRequeueAfter(status *kcv1alpha1.ConnectorStatus) time.Duration {
	times := []*metav1.Time{status.LastStateTransitionAt, status.LastUpdatedAt, status.LastRestartAt}

	var last *metav1.Time
	for _, t := range times {
		if t == nil {
			continue
		}
		if last == nil || t.After(last.Time) {
			last = t
		}
	}

	if last == nil {
		return r.ReconcileInterval
	}

	const base = 3 * time.Second
	elapsed := time.Since(last.Time)

	requeue := base
	for requeue < r.ReconcileInterval && elapsed >= requeue*8 {
		requeue *= 2
	}
	if requeue > r.ReconcileInterval {
		requeue = r.ReconcileInterval
	}
	return requeue
}
