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
	"reflect"
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
	typeRunningConnector       = "Running"
	connectorFinalizer         = "kafka-connect.b1zzu.net/connector"
	connectorRestartAnnotation = "kafka-connect.b1zzu.net/restart"

	connectorStatusRunning = "RUNNING"
	connectorStatusPaused  = "PAUSED"
	connectorStatusStopped = "STOPPED"
	connectorStatusFailed  = "FAILED"

	offsetsAnnotation   = "kafka-connect.b1zzu.net/offsets"
	offsetsConfigMapKey = "offsets.json"
)

type connectorReconcileFunc func(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error)

type ConnectorReconciliationError struct {
	err       error
	msg       string
	connector *kcv1alpha1.Connector
}

func (e *ConnectorReconciliationError) Reason() string {
	if e.err == nil {
		return "UserError"
	}
	return "Error"
}

func (e *ConnectorReconciliationError) Error() string {
	if e.err == nil {
		return e.msg
	}
	return fmt.Errorf("%s: %w", e.msg, e.err).Error()
}

// ConnectorReconciler reconciles a Connector object
type ConnectorReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder

	// ReconcileInterval is the interval between each reconcile loop
	// to monitor the status of the connector.
	ReconcileInterval time.Duration

	// RestartFailedConnectorBackoff is the time to wait before attempting
	// to restart a failed connector after it was updated.
	RestartFailedConnectorBackoff time.Duration

	// NewKafkaConnectClientFunc creates a Kafka Connect client for the given connector.
	NewKafkaConnectClientFunc func(connector *kcv1alpha1.Connector) *kafkaconnect.Client
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
	log.Info("Start Reconcile loop")

	connector, err := r.getConnector(ctx, req.NamespacedName)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
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
			return ctrl.Result{}, r.handleReconciliationError(ctx, err)
		}
		if connector == nil {
			return ctrl.Result{RequeueAfter: r.ReconcileInterval}, nil
		}
	}

	log.Info("Reconcile completed")
	// Monitor the connector status every minute
	return ctrl.Result{RequeueAfter: r.ReconcileInterval}, nil
}

func (r *ConnectorReconciler) handleReconciliationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if rerr, ok := err.(*ConnectorReconciliationError); ok {
		err = r.updateStatusCondition(ctx, rerr.connector, metav1.Condition{
			Type:    typeRunningConnector,
			Status:  metav1.ConditionFalse,
			Reason:  rerr.Reason(),
			Message: fmt.Sprintf("Error: %s", rerr),
		})
		if err != nil {
			return fmt.Errorf("failed to update Connector status with error: %w; after error: %w", err, rerr)
		}
		if rerr.err != nil {
			return rerr
		}
		return nil
	}
	return err
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
		err := r.updateStatusCondition(ctx, connector, metav1.Condition{
			Type:   typeRunningConnector,
			Status: metav1.ConditionUnknown,
			Reason: "Reconciling",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize condition: %w", err)
		}

		log.Info("Resource initial condition updated successfully")

		// The Connector resource was updated, it must be refetched or the reconciliation loop restarted
		return nil, nil
	}

	return connector, nil
}

func (r *ConnectorReconciler) reconcileConnector(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Create Kafka Connect client
	kafkaConnect := r.NewKafkaConnectClientFunc(connector)

	// Get existing connector from Kafka Connect
	existingConnector, err := kafkaConnect.GetConnector(ctx, connector.Name)
	if err != nil {
		return nil, &ConnectorReconciliationError{err: err, msg: "failed to get connector", connector: connector}
	}

	// Connector doesn't exist, create it
	if existingConnector == nil {
		log.Info("Creating connector")

		desiredState := getDesiredConnectorState(connector)

		// Create the connector
		newConnector := &kafkaconnect.Connector{
			Name:   connector.Name,
			Config: connector.Spec.Config,
		}

		switch desiredState {
		case kcv1alpha1.ConnectorStatePaused:
			newConnector.InitialState = connectorStatusPaused
		case kcv1alpha1.ConnectorStateStopped:
			newConnector.InitialState = connectorStatusStopped
		}

		err = kafkaConnect.CreateConnector(ctx, newConnector)
		if err != nil {
			return nil, &ConnectorReconciliationError{err: err, msg: "failed to create connector", connector: connector}
		}

		now := metav1.Now()
		connector.Status.LastUpdatedAt = &now
		err = r.updateStatusCondition(ctx, connector, metav1.Condition{
			Type:   typeRunningConnector,
			Status: metav1.ConditionFalse,
			Reason: "Starting",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update status: %w", err)
		}

		log.Info("Connector created successfully")

		// Return nil to restart reconciliation and proceed to status sync
		return nil, nil
	}

	actualConfig := existingConnector.Config
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
			return nil, &ConnectorReconciliationError{err: err, msg: "failed to update connector config", connector: connector}
		}

		now := metav1.Now()
		connector.Status.LastUpdatedAt = &now
		err := r.updateStatusCondition(ctx, connector, metav1.Condition{
			Type:   typeRunningConnector,
			Status: metav1.ConditionFalse,
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
// and it's controlled using annotations.
func (r *ConnectorReconciler) reconcileConnectorState(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	desiredState := getDesiredConnectorState(connector)

	kafkaConnect := r.NewKafkaConnectClientFunc(connector)

	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		return nil, &ConnectorReconciliationError{err: err, msg: "failed to get connector status", connector: connector}
	}

	actualState := kcv1alpha1.ConnectorState(strings.ToLower(status.Connector.State))

	if actualState == desiredState {
		return connector, nil
	}

	// Skip state reconciliation if the connector is in an unrecoverable or unknown state
	switch actualState {
	case kcv1alpha1.ConnectorStateRunning, kcv1alpha1.ConnectorStatePaused, kcv1alpha1.ConnectorStateStopped:
		// known states, proceed with reconciliation
	default:
		log.Info("Skipping connector state reconciliation, connector is in unreconcilable state", "actual", actualState, "desired", desiredState)
		return connector, nil
	}

	log.Info("Reconciling connector state", "actual", actualState, "desired", desiredState)

	switch desiredState {
	case kcv1alpha1.ConnectorStateRunning:
		log.Info("Resuming connector")
		err = kafkaConnect.ResumeConnector(ctx, connector.Name)
		if err != nil {
			return nil, &ConnectorReconciliationError{err: err, msg: "failed to resume connector", connector: connector}
		}
	case kcv1alpha1.ConnectorStatePaused:
		if actualState == kcv1alpha1.ConnectorStateStopped {
			// Cannot pause a stopped connector directly; resume first, then requeue to pause
			log.Info("Resuming connector before pausing")
			err = kafkaConnect.ResumeConnector(ctx, connector.Name)
			if err != nil {
				return nil, &ConnectorReconciliationError{err: err, msg: "failed to resume connector before pausing", connector: connector}
			}
		} else {
			log.Info("Pausing connector")
			err = kafkaConnect.PauseConnector(ctx, connector.Name)
			if err != nil {
				return nil, &ConnectorReconciliationError{err: err, msg: "failed to pause connector", connector: connector}
			}
		}
	case kcv1alpha1.ConnectorStateStopped:
		log.Info("Stopping connector")
		err = kafkaConnect.StopConnector(ctx, connector.Name)
		if err != nil {
			return nil, &ConnectorReconciliationError{err: err, msg: "failed to stop connector", connector: connector}
		}
	}

	// Update status to Unknown to trigger an immediate reconciliation via watch event.
	// Use "StateChange" reason to ensure the condition differs from a previous "Reconciling"
	// status (e.g., during stopped→paused transitions that require two state changes).
	err = r.updateStatusCondition(ctx, connector, metav1.Condition{
		Type:    typeRunningConnector,
		Status:  metav1.ConditionUnknown,
		Reason:  "Reconciling",
		Message: fmt.Sprintf("Reconciling connector state from %s to %s", actualState, desiredState),
	})
	if err != nil {
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

	kafkaConnect := r.NewKafkaConnectClientFunc(connector)
	if err := kafkaConnect.RestartConnector(ctx, connector.Name); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedRestart", "Restart", "Failed to restart connector: %v", err)
		return nil, fmt.Errorf("failed to restart connector: %w", err)
	}

	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal, "Restarted", "Restart", "Connector restarted successfully")

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
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedOffsets", "ManageOffsets", "Unknown offsets annotation value: %s", annotation)
		return r.removeOffsetsAnnotation(ctx, connector)
	}
}

func (r *ConnectorReconciler) reconcileExportOffsets(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	if connector.Spec.ExportOffsets == nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedExportOffsets", "ExportOffsets", "spec.exportOffsets.configMapRef is required for export")
		return r.removeOffsetsAnnotation(ctx, connector)
	}

	kafkaConnect := r.NewKafkaConnectClientFunc(connector)
	offsets, err := kafkaConnect.GetConnectorOffsets(ctx, connector.Name)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedExportOffsets", "ExportOffsets", "Failed to get offsets: %v", err)
		return nil, fmt.Errorf("failed to get connector offsets: %w", err)
	}

	offsetsJSON, err := json.Marshal(offsets)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedExportOffsets", "ExportOffsets", "Failed to marshal offsets: %v", err)
		return nil, fmt.Errorf("failed to marshal offsets: %w", err)
	}

	cmName := connector.Spec.ExportOffsets.ConfigMapRef.Name
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, client.ObjectKey{Name: cmName, Namespace: connector.Namespace}, cm)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedExportOffsets", "ExportOffsets", "Failed to get ConfigMap: %v", err)
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
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedExportOffsets", "ExportOffsets", "Failed to create ConfigMap: %v", err)
			return nil, fmt.Errorf("failed to create ConfigMap: %w", err)
		}
	} else {
		// Update ConfigMap
		cm.Data = map[string]string{
			offsetsConfigMapKey: string(offsetsJSON),
		}
		if err := r.Update(ctx, cm); err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedExportOffsets", "ExportOffsets", "Failed to update ConfigMap: %v", err)
			return nil, fmt.Errorf("failed to update ConfigMap: %w", err)
		}
	}

	log.Info("Exported connector offsets", "configMap", cmName)
	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal, "ExportedOffsets", "ExportOffsets", "Exported offsets to ConfigMap %s", cmName)

	connector.Status.LastExportedOffsetsAt = &metav1.Time{Time: time.Now()}
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update status with export timestamp: %w", err)
	}

	return r.removeOffsetsAnnotation(ctx, connector)
}

func (r *ConnectorReconciler) reconcileImportOffsets(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	if connector.Spec.ImportOffsets == nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedImportOffsets", "ImportOffsets", "spec.importOffsets.configMapRef is required for import")
		return r.removeOffsetsAnnotation(ctx, connector)
	}

	// Validate connector is stopped
	kafkaConnect := r.NewKafkaConnectClientFunc(connector)
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedImportOffsets", "ImportOffsets", "Failed to get connector status: %v", err)
		return nil, fmt.Errorf("failed to get connector status: %w", err)
	}

	if status.Connector.State != connectorStatusStopped {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedImportOffsets", "ImportOffsets", "Connector must be stopped to import offsets, current state: %s", status.Connector.State)
		// Keep annotation for retry
		return connector, nil
	}

	// Read ConfigMap
	cmName := connector.Spec.ImportOffsets.ConfigMapRef.Name
	cm := &corev1.ConfigMap{}
	err = r.Get(ctx, client.ObjectKey{Name: cmName, Namespace: connector.Namespace}, cm)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedImportOffsets", "ImportOffsets", "Failed to get ConfigMap %s: %v", cmName, err)
		return nil, fmt.Errorf("failed to get ConfigMap: %w", err)
	}

	offsetsJSON, ok := cm.Data[offsetsConfigMapKey]
	if !ok {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedImportOffsets", "ImportOffsets", "ConfigMap %s missing key %s", cmName, offsetsConfigMapKey)
		return nil, fmt.Errorf("ConfigMap %s missing key %s", cmName, offsetsConfigMapKey)
	}

	offsets := &kafkaconnect.ConnectorOffsets{}
	if err := json.Unmarshal([]byte(offsetsJSON), offsets); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedImportOffsets", "ImportOffsets", "Failed to parse offsets from ConfigMap: %v", err)
		return nil, fmt.Errorf("failed to parse offsets from ConfigMap: %w", err)
	}

	if err := kafkaConnect.PatchConnectorOffsets(ctx, connector.Name, offsets); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedImportOffsets", "ImportOffsets", "Failed to import offsets: %v", err)
		return nil, fmt.Errorf("failed to patch connector offsets: %w", err)
	}

	log.Info("Imported connector offsets", "configMap", cmName)
	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal, "ImportedOffsets", "ImportOffsets", "Imported offsets from ConfigMap %s", cmName)

	connector.Status.LastImportedOffsetsAt = &metav1.Time{Time: time.Now()}
	if err := r.Status().Update(ctx, connector); err != nil {
		return nil, fmt.Errorf("failed to update status with import timestamp: %w", err)
	}

	return r.removeOffsetsAnnotation(ctx, connector)
}

func (r *ConnectorReconciler) reconcileResetOffsets(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Validate connector is stopped
	kafkaConnect := r.NewKafkaConnectClientFunc(connector)
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedResetOffsets", "ResetOffsets", "Failed to get connector status: %v", err)
		return nil, fmt.Errorf("failed to get connector status: %w", err)
	}

	if status.Connector.State != connectorStatusStopped {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedResetOffsets", "ResetOffsets", "Connector must be stopped to reset offsets, current state: %s", status.Connector.State)
		// Keep annotation for retry
		return connector, nil
	}

	if err := kafkaConnect.DeleteConnectorOffsets(ctx, connector.Name); err != nil {
		r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedResetOffsets", "ResetOffsets", "Failed to reset offsets: %v", err)
		return nil, fmt.Errorf("failed to delete connector offsets: %w", err)
	}

	log.Info("Reset connector offsets")
	r.Recorder.Eventf(connector, nil, corev1.EventTypeNormal, "ResetOffsets", "ResetOffsets", "Reset connector offsets")

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
	previousStatus := connector.Status.DeepCopy()

	// Create Kafka Connect client
	kafkaConnect := r.NewKafkaConnectClientFunc(connector)

	// Get connector status from Kafka Connect
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		return nil, &ConnectorReconciliationError{err: err, msg: "failed to get connector status", connector: connector}
	}

	// Restart failed connectors/tasks when desired state is running
	desiredState := getDesiredConnectorState(connector)
	if desiredState == kcv1alpha1.ConnectorStateRunning {
		r.restartFailedConnectorAndTasks(ctx, kafkaConnect, connector, status)
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

	// Set condition
	newCondition := mapConnectorStatusToCondition(status)
	newCondition.ObservedGeneration = connector.Generation
	meta.SetStatusCondition(&connector.Status.Conditions, newCondition)

	// Only persist if status has drifted
	if !reflect.DeepEqual(*previousStatus, connector.Status) {
		log.Info("Updating connector status")
		if err := r.Status().Update(ctx, connector); err != nil {
			return nil, fmt.Errorf("failed to update connector status: %w", err)
		}
		return nil, nil
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
) {
	log := logf.FromContext(ctx)

	lastUpdatedAt := connector.Status.LastUpdatedAt
	if lastUpdatedAt != nil && lastUpdatedAt.After(time.Now().Add(-r.RestartFailedConnectorBackoff)) {
		log.Info("Skip restart of recently updated Connector")
		return
	}

	restarted := false

	if status.Connector.State == connectorStatusFailed {
		log.Info("Restarting failed connector")
		if err := kafkaConnect.RestartConnector(ctx, connector.Name); err != nil {
			r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedRestart", "Restart", "Failed to restart failed connector: %v", err)
			log.Info("Failed to restart failed connector", "error", err.Error())
			return
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
		log.Info("Restarting failed tasks", "taskIDs", failedTaskIDs)
		for _, taskID := range failedTaskIDs {
			if err := kafkaConnect.RestartTask(ctx, connector.Name, taskID); err != nil {
				r.Recorder.Eventf(connector, nil, corev1.EventTypeWarning, "FailedRestart", "Restart", "Failed to restart task %d: %v", taskID, err)
				log.Info("Failed to restart failed task", "taskID", taskID, "error", err.Error())
				return
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
}

func (r *ConnectorReconciler) reconcileConnectorFinalizer(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Check if the connector is being deleted
	if !connector.DeletionTimestamp.IsZero() {
		// Connector is being deleted
		log.Info("Deleting connector")

		// Delete the connector from Kafka Connect
		kafkaConnect := r.NewKafkaConnectClientFunc(connector)
		err := kafkaConnect.DeleteConnector(ctx, connector.Name)
		if err != nil {
			return nil, &ConnectorReconciliationError{err: err, msg: "failed to delete connector", connector: connector}
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
	log.Info("Update Connector status condition", "type", condition.Type, "status", condition.Status)

	condition.ObservedGeneration = connector.Generation
	meta.SetStatusCondition(&connector.Status.Conditions, condition)
	err := r.Status().Update(ctx, connector)
	if err != nil {
		return fmt.Errorf("failed to update Connector status condition: %w", err)
	}

	return nil
}

// NewDefaultKafkaConnectClientFunc returns a factory that creates Kafka Connect
// clients using the in-cluster service endpoint.
func NewDefaultKafkaConnectClientFunc(connector *kcv1alpha1.Connector) *kafkaconnect.Client {
	endpoint := fmt.Sprintf("http://%s.%s:8083", connector.Spec.ClusterRef.Name, connector.Namespace)
	return kafkaconnect.NewClient(endpoint)
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

func mapConnectorStatusToCondition(status *kafkaconnect.ConnectorStatus) metav1.Condition {
	condition := metav1.Condition{
		Type: typeRunningConnector,
	}

	switch status.Connector.State {
	case connectorStatusRunning:
		failedTasks := countFailedTasks(status.Tasks)
		if failedTasks > 0 {
			condition.Status = metav1.ConditionFalse
			condition.Reason = "Failed"
			condition.Message = fmt.Sprintf("Connector has %d failed task(s) out of %d", failedTasks, len(status.Tasks))
		} else {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "Running"
			condition.Message = fmt.Sprintf("Connector is running with %d task(s)", len(status.Tasks))
		}
	case connectorStatusPaused:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Paused"
		condition.Message = "Connector is paused"
	case connectorStatusStopped:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Stopped"
		condition.Message = "Connector is stopped"
	case connectorStatusFailed:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Failed"
		condition.Message = fmt.Sprintf("Connector failed with trace: %s", strings.ReplaceAll(status.Connector.Trace, "\n\t", "\n"))
	// TODO: Handle Unasigned
	default:
		condition.Status = metav1.ConditionUnknown
		condition.Reason = "Unknown"
		condition.Message = fmt.Sprintf("Connector in unknown state: %s", status.Connector.State)
	}

	return condition
}
