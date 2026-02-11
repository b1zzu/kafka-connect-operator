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
	"fmt"
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
)

// ConnectorReconciler reconciles a Connector object
type ConnectorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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

	connector, err = r.reconcileConnectorFinalizer(ctx, connector)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
	}

	// Step 4: Reconcile connector in Kafka Connect
	connector, err = r.reconcileConnector(ctx, connector)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
	}

	// Step 5: Sync status from Kafka Connect
	connector, err = r.reconcileConnectorStatus(ctx, connector)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
	}

	log.Info("Reconcile completed")
	// Monitor the connector status every minute
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConnectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kcv1alpha1.Connector{}).
		Named("connector").
		Complete(r)
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
		err := r.updateStatusCondition(ctx, connector, metav1.Condition{
			Type:    typeRunningConnector,
			Status:  metav1.ConditionUnknown,
			Reason:  "Error",
			Message: fmt.Sprintf("Failed to get connector: %s", err.Error()),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update status after failed to get connector: %w", err)
		}
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	// Connector doesn't exist, create it
	if existingConnector == nil {
		log.Info("Creating connector")

		// Create the connector
		newConnector := &kafkaconnect.Connector{
			Name:   connector.Name,
			Config: connector.Spec.Config,
		}

		err = kafkaConnect.CreateConnector(ctx, newConnector)
		if err != nil {
			return nil, r.updateStatusConditionAndReturnError(ctx, connector, err, "failed to create connector")
		}

		err := r.updateStatusCondition(ctx, connector, metav1.Condition{
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
			return nil, r.updateStatusConditionAndReturnError(ctx, connector, err, "failed to update connector config")
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

func (r *ConnectorReconciler) reconcileConnectorStatus(ctx context.Context, connector *kcv1alpha1.Connector) (*kcv1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	// TODO: Add current configuration and running tasks to the resource status

	// Create Kafka Connect client
	kafkaConnect := r.newKafkaConnectClient(connector)

	// Get connector status from Kafka Connect
	status, err := kafkaConnect.GetConnectorStatus(ctx, connector.Name)
	if err != nil {
		return nil, r.updateStatusConditionAndReturnError(ctx, connector, err, "failed to get connector status")
	}

	// Map Kafka Connect status to Kubernetes condition
	newCondition := mapConnectorStatusToCondition(status)

	// Get current condition
	currentCondition := meta.FindStatusCondition(connector.Status.Conditions, typeRunningConnector)

	// Only update if condition has changed
	if currentCondition == nil ||
		currentCondition.Status != newCondition.Status ||
		currentCondition.Reason != newCondition.Reason ||
		currentCondition.Message != newCondition.Message {

		log.Info("Updating condition", "condition", newCondition)

		err := r.updateStatusCondition(ctx, connector, newCondition)
		if err != nil {
			return nil, fmt.Errorf("failed to update status condition: %w", err)
		}

		// Return nil to restart reconciliation with updated status
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
			return nil, r.updateStatusConditionAndReturnError(ctx, connector, err, "failed to delete connector")
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

func (r *ConnectorReconciler) updateStatusConditionAndReturnError(
	ctx context.Context,
	connector *kcv1alpha1.Connector,
	err error,
	reason string,
) error {
	if err := r.updateStatusCondition(ctx, connector, metav1.Condition{
		Type:    typeRunningConnector,
		Status:  metav1.ConditionFalse,
		Reason:  "Error",
		Message: fmt.Sprintf("%s: %s", reason, err.Error()),
	}); err != nil {
		return fmt.Errorf("failed to update status after %s: %w", reason, err)
	}
	return fmt.Errorf("%s: %w", reason, err)
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
		if task.State == "FAILED" {
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
	case "FAILED":
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
