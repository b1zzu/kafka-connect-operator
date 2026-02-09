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

// Package controller
package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kcv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
	"github.com/b1zzu/kafka-connect-operator/pkg/utils"
)

// Definitions to manage status conditions
const (
	// typeAvailableCluster represents the status of the Deployment reconciliation
	typeAvailableCluster = "Available"
)

const (
	// serverSideApplyManager the manager id set when performing Server-Side Apply
	serverSideApplyManager = "kafka-connect-operator"
)

// ClusterReconciler reconciles a Cluster object
type ClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	log.Info("Start Reconcile loop")

	// Get the resource definition from the API
	cluster, err := r.getCluster(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if cluster == nil {
		// The Cluster resource was deleted
		return ctrl.Result{}, nil
	}

	// Set the status to Unknown when no status is available
	cluster, err = r.initializeStatusConditions(ctx, cluster)
	if err != nil || cluster == nil {
		return ctrl.Result{}, err
	}

	cluster, err = r.reconcileConfigMap(ctx, cluster)
	if err != nil || cluster == nil {
		return ctrl.Result{}, err
	}

	cluster, err = r.reconcileDeployment(ctx, cluster)
	if err != nil || cluster == nil {
		return ctrl.Result{}, err
	}

	log.Info("Reconcile completed")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kcv1alpha1.Cluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Named("cluster").
		Complete(r)
}

func (r *ClusterReconciler) getCluster(ctx context.Context, key types.NamespacedName) (*kcv1alpha1.Cluster, error) {
	log := logf.FromContext(ctx)

	cluster := &kcv1alpha1.Cluster{}
	err := r.Get(ctx, key, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// This happens when the resource is deleted, in this case
			// we are just letting the kubernetes garbage collector do
			// it's job.
			log.Info("Resource has been deleted")
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get Cluster: %w", err)
	}
	return cluster, nil
}

func (r *ClusterReconciler) initializeStatusConditions(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	log := logf.FromContext(ctx)

	if len(cluster.Status.Conditions) == 0 {
		err := r.updateStatusCondition(ctx, cluster, metav1.Condition{
			Type:   typeAvailableCluster,
			Status: metav1.ConditionUnknown,
			Reason: "Reconciling",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Cluster condition: %w", err)
		}

		log.Info("Resource initial condition updated successfully")

		// The Cluster resource was updted, it must be refecth or th reconciliation loop restarted
		return nil, nil
	}

	// Cluster is unchanged
	return cluster, nil
}

func (r *ClusterReconciler) updateStatusCondition(
	ctx context.Context,
	cluster *kcv1alpha1.Cluster,
	condition metav1.Condition,
) error {
	log := logf.FromContext(ctx)

	log.Info("Update Cluster status condition", "type", condition.Type, "status", condition.Status)

	meta.SetStatusCondition(&cluster.Status.Conditions, condition)
	err := r.Status().Update(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to update Cluster status condition: %w", err)
	}

	return nil
}

func (r *ClusterReconciler) serverSideApply(ctx context.Context, obj runtime.ApplyConfiguration) error {
	return r.Apply(ctx, obj, &client.ApplyOptions{
		Force:        ptr.To(false),
		FieldManager: serverSideApplyManager,
	})
}

func (r *ClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	log := logf.FromContext(ctx)

	configMapA := configMapForCluster(cluster)

	err := r.serverSideApply(ctx, configMapA)
	if err != nil {

		err := r.updateStatusCondition(ctx, cluster, metav1.Condition{
			Type:    typeAvailableCluster,
			Status:  metav1.ConditionFalse,
			Reason:  "Error",
			Message: fmt.Sprintf("Failed to apply ConfigMap: %s", err),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update Cluster status after failed to apply ConfigMap: %w", err)
		}
		return nil, fmt.Errorf("failed to apply ConfigMap: %w", err)
	}

	// Refetch the cluster after Server-Side Apply
	cluster, err = r.getCluster(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace})
	if err != nil || cluster == nil {
		return cluster, err
	}

	// Fetch the ConfigMap to compute the full hash of what is actually applied
	configMap := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: *configMapA.Name, Namespace: *configMapA.Namespace}, configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to get ConfigMap after apply: %w", err)
	}

	// Compute the ConfigMap hash
	h := fnv.New64a()
	for k, v := range configMap.Data {
		h.Write([]byte(k))
		h.Write([]byte(v))
	}
	for k, v := range configMap.BinaryData {
		h.Write([]byte(k))
		h.Write(v)
	}
	configMapHash := strconv.FormatUint(h.Sum64(), 16)

	// Update the cluster status with the ConfigMap hash
	if cluster.Status.ConfigHash == nil || *cluster.Status.ConfigHash != configMapHash {
		cluster.Status.ConfigHash = &configMapHash

		log.Info("Update Cluster status configHash", "configHash", configMapHash)

		err := r.Status().Update(ctx, cluster)
		if err != nil {
			return nil, fmt.Errorf("failed to update Cluster status after updating the configHash: %w", err)
		}

		// Reconciliation will be re-triggered by the status update
		return nil, nil
	}

	// ConfigMap is already align
	return cluster, nil
}

func (r *ClusterReconciler) reconcileDeployment(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	log := logf.FromContext(ctx)

	deploymentA := deploymentForCluster(cluster)

	err := r.serverSideApply(ctx, deploymentA)
	if err != nil {
		log.Error(err, "Failed to apply Deployment")

		err := r.updateStatusCondition(ctx, cluster, metav1.Condition{
			Type:    typeAvailableCluster,
			Status:  metav1.ConditionFalse,
			Reason:  "Error",
			Message: fmt.Sprintf("Failed to apply Deployment: %s", err),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to update Cluster status after failed to apply Deployment: %w", err)
		}
		return nil, fmt.Errorf("failed to apply Deployment: %w", err)
	}

	// Refetch the cluster after Server-Side Apply
	cluster, err = r.getCluster(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace})
	if err != nil || cluster == nil {
		return cluster, err
	}

	// Update cluster condition according to deployment condition
	deployment := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: *deploymentA.Name, Namespace: *deploymentA.Namespace}, deployment)
	if err != nil {
		return nil, err
	}

	deploymentAvailable := utils.FindStatusDeploymentCondition(deployment.Status.Conditions, "Available")
	clusterAvailable := meta.FindStatusCondition(cluster.Status.Conditions, typeAvailableCluster)
	if deploymentAvailable != nil {
		if clusterAvailable.Status != metav1.ConditionStatus(deploymentAvailable.Status) {
			clusterAvailable.Status = metav1.ConditionStatus(deploymentAvailable.Status)
			clusterAvailable.Reason = deploymentAvailable.Reason
			clusterAvailable.Message = deploymentAvailable.Message

			log.Info("Update Cluster status Available condition according to Deployment status", "status", clusterAvailable.Status, "reason", clusterAvailable.Reason)

			err := r.updateStatusCondition(ctx, cluster, *clusterAvailable)
			if err != nil {
				return nil, fmt.Errorf("failed to udpate Cluster status with Deployment status: %w", err)
			}

			return nil, nil
		}
	}

	// Deployment applied and status unchanged
	return cluster, nil
}
