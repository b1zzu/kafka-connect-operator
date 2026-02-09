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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kc1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
)

const (
	typeRunningConnector = "Running"
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
	_ = logf.FromContext(ctx)

	connector, err := r.getConnector(ctx, req.NamespacedName)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
	}

	connector, err = r.initializeStatusConditions(ctx, connector)
	if err != nil || connector == nil {
		return ctrl.Result{}, err
	}

	// TODO: Handle delete using finalizers

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConnectorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kc1alpha1.Connector{}).
		Named("connector").
		Complete(r)
}

func (r *ConnectorReconciler) getConnector(ctx context.Context, key client.ObjectKey) (*kc1alpha1.Connector, error) {
	log := logf.FromContext(ctx)

	connector := &kc1alpha1.Connector{}
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
			return nil, fmt.Errorf("failed to initialize Connector condition: %w", err)
		}

		log.Info("Resource initial condition updated successfully")

		// The Connector resource was updated, it must be refecth or th reconciliation loop restarted
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

	meta.SetStatusCondition(&connector.Status.Conditions, condition)
	err := r.Status().Update(ctx, connector)
	if err != nil {
		return fmt.Errorf("failed to update Connector status condition: %w", err)
	}

	return nil
}
