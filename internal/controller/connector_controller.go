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
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	kafkaconnect "github.com/b1zzu/kafka-connect-operator/pkg/kafka-connect"
)

const (
	typeRunningConnector = "Running"
	connectorFinalizer   = "kafka-connect.b1zzu.net/connector"

	connectorStatusFailed = "FAILED"
)

type connectorReconcileFunc func(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error)

type ConnectorReconciliationError struct {
	err       error
	msg       string
	connector *kcv1alpha1.Connector
}

func (e *ConnectorReconciliationError) Error() string {
	return fmt.Errorf("%s: %w", e.msg, e.err).Error()
}

// ConnectorReconciler reconciles a Connector object
type ConnectorReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NewKafkaConnectClientFunc creates a Kafka Connect client for the given connector.
	NewKafkaConnectClientFunc func(connector *kcv1alpha1.Connector) *kafkaconnect.Client
}

// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=connectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=connectors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=connectors/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Connector object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
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

	for _, reconcile := range [4]connectorReconcileFunc{
		r.reconcileConnectorFinalizer,
		r.reconcileConnector,
		r.reconcileConnectorState,
		r.reconcileConnectorStatus,
	} {
		connector, err = reconcile(ctx, connector)
		if err != nil || connector == nil {
			return ctrl.Result{}, r.handleReconciliationError(ctx, err)
		}
	}

	log.Info("Reconcile completed")
	// Monitor the connector status every minute
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

func (r *ConnectorReconciler) handleReconciliationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if rerr, ok := err.(*ConnectorReconciliationError); ok {
		err = r.updateStatusCondition(ctx, rerr.connector, metav1.Condition{
			Type:    typeRunningConnector,
			Status:  metav1.ConditionFalse,
			Reason:  "Error",
			Message: fmt.Sprintf("Error: %s", rerr),
		})
		if err != nil {
			return fmt.Errorf("failed to update Connector status with error: %w; after error: %w", err, rerr)
		}
		return rerr
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

		// The Connector resource was updated, it must be refecth or th reconciliation loop restarted
		return nil, nil
	}

	return connector, nil
}

func (r *ConnectorReconciler) reconcileConnector(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Create Kafka Connect client
	kafkaConnect := r.newKafkaConnectClient(connector)

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
			newConnector.InitialState = "PAUSED"
		case kcv1alpha1.ConnectorStateStopped:
			newConnector.InitialState = "STOPPED"
		}

		err = kafkaConnect.CreateConnector(ctx, newConnector)
		if err != nil {
			return nil, &ConnectorReconciliationError{err: err, msg: "failed to create connector", connector: connector}
		}

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

func (r *ConnectorReconciler) reconcileConnectorState(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	desiredState := getDesiredConnectorState(connector)

	kafkaConnect := r.newKafkaConnectClient(connector)

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

func (r *ConnectorReconciler) reconcileConnectorStatus(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Create Kafka Connect client
	kafkaConnect := r.newKafkaConnectClient(connector)

	// Get connector status from Kafka Connect
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		return nil, &ConnectorReconciliationError{err: err, msg: "failed to get connector status", connector: connector}
	}

	// Restart failed connectors/tasks when desired state is running
	desiredState := getDesiredConnectorState(connector)

	if desiredState == kcv1alpha1.ConnectorStateRunning {
		if status.Connector.State == connectorStatusFailed {
			log.Info("Restarting failed connector")
			if err := kafkaConnect.RestartConnector(ctx, connector.Name); err != nil {
				return nil, &ConnectorReconciliationError{err: err, msg: "failed to restart connector", connector: connector}
			}
			return nil, fmt.Errorf("connector %s is in FAILED state, restarted and requeueing", connector.Name)
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
					return nil, &ConnectorReconciliationError{err: err, msg: fmt.Sprintf("failed to restart task %d", taskID), connector: connector}
				}
			}
			return nil, fmt.Errorf("connector %s has %d failed task(s), restarted and requeueing", connector.Name, len(failedTaskIDs))
		}
	}

	// Save current status before applying changes
	previousStatus := connector.Status.DeepCopy()

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

func (r *ConnectorReconciler) reconcileConnectorFinalizer(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// Check if the connector is being deleted
	if !connector.DeletionTimestamp.IsZero() {
		// Connector is being deleted
		log.Info("Deleting connector")

		// Delete the connector from Kafka Connect
		kafkaConnect := r.newKafkaConnectClient(connector)
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
	meta.SetStatusCondition(&connector.Status.Conditions, condition)
	err := r.Status().Update(ctx, connector)
	if err != nil {
		return fmt.Errorf("failed to update Connector status condition: %w", err)
	}

	return nil
}

func (r *ConnectorReconciler) newKafkaConnectClient(connector *kcv1alpha1.Connector) *kafkaconnect.Client {
	return r.NewKafkaConnectClientFunc(connector)
}

// NewDefaultKafkaConnectClientFunc returns a factory that creates Kafka Connect
// clients using the in-cluster service endpoint.
func NewDefaultKafkaConnectClientFunc(connector *kcv1alpha1.Connector) *kafkaconnect.Client {
	endpoint := fmt.Sprintf("http://%s-connect.%s:8083", connector.Spec.ClusterRef.Name, connector.Namespace)
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
	case "RUNNING":
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
	case "PAUSED":
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Paused"
		condition.Message = "Connector is paused"
	case "STOPPED":
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Stopped"
		condition.Message = "Connector is stopped"
	case connectorStatusFailed:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "Failed"
		condition.Message = fmt.Sprintf("Connector failed with trace: %s", strings.ReplaceAll(status.Connector.Trace, "\n\t", "\n"))
	default:
		condition.Status = metav1.ConditionUnknown
		condition.Reason = "Unknown"
		condition.Message = fmt.Sprintf("Connector in unknown state: %s", status.Connector.State)
	}

	return condition
}
