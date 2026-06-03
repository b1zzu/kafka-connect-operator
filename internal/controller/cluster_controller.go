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
	netv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

	// serverSideApplyManager the manager id set when performing Server-Side Apply
	serverSideApplyManager = "kafka-connect-operator"

	pauseReconciliationAnnotation = "kafka-connect.b1zzu.net/pause-reconciliation"
	pauseReconciliationTrue       = "true"
)

// ClusterReconciliationError when returned triggerd a Cluster status
// condition update with the error details.
type ClusterReconciliationError struct {
	err     error
	msg     string
	cluster *kcv1alpha1.Cluster
}

func (e *ClusterReconciliationError) Reason() string {
	if e.err == nil {
		return "UserError"
	}
	return "Error"
}

func (e *ClusterReconciliationError) Error() string {
	if e.err == nil {
		return e.msg
	}
	return fmt.Errorf("%s: %w", e.msg, e.err).Error()
}

type reconcileFunc func(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error)

// ClusterReconciler reconciles a Cluster object
type ClusterReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Namespace string
}

// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kafka-connect.b1zzu.net,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

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

	// Skip reconciliation loop if paused
	if pause, ok := cluster.GetAnnotations()[pauseReconciliationAnnotation]; ok {
		if pause == pauseReconciliationTrue {
			log.Info("Skipping reconciliation, reconciliation is paused.")
			return ctrl.Result{}, nil
		}
	}

	// Set the status to Unknown when no status is available
	cluster, err = r.initializeStatusConditions(ctx, cluster)
	if err != nil || cluster == nil {
		return ctrl.Result{}, err
	}

	for _, reconcile := range []reconcileFunc{
		r.reconcileService,
		r.reconcileNetworkPolicy,
		r.reconcileServiceAccount,
		r.reconcilePodDisruptionBudget,
		r.reconcileConfigMap,
		r.reconcileDeployment,
	} {
		cluster, err = reconcile(ctx, cluster)
		if err != nil || cluster == nil {
			return ctrl.Result{}, r.handleReconciliationError(ctx, err)
		}
	}

	log.Info("Reconcile completed")
	return ctrl.Result{}, nil
}

func (r *ClusterReconciler) handleReconciliationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if rerr, ok := err.(*ClusterReconciliationError); ok {
		if rerr.cluster == nil {
			return fmt.Errorf("unexpected nil cluster in ClusterReconciliationError; after error: %w", rerr)
		}

		err = r.updateStatusCondition(ctx, rerr.cluster, metav1.Condition{
			Type:    typeAvailableCluster,
			Status:  metav1.ConditionFalse,
			Reason:  rerr.Reason(),
			Message: fmt.Sprintf("Error: %s", rerr),
		})
		if err != nil {
			return fmt.Errorf("failed to update Cluster status with error: %w; after error: %w", err, rerr)
		}

		if rerr.err != nil {
			return rerr
		}

		// If ClusterReconciliationError.err is not set, than it's a user error
		// therfore we do not return an error on top of updating the status,
		// and we stop the reconciliation loop until the user fixes the error.
		return nil
	}
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kcv1alpha1.Cluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&netv1.NetworkPolicy{}).
		Owns(&policyv1.PodDisruptionBudget{}).
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

		// The Cluster resource was updated, it must be refetched or the reconciliation loop restarted
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

	condition.ObservedGeneration = cluster.Generation
	meta.SetStatusCondition(&cluster.Status.Conditions, condition)
	err := r.Status().Update(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to update Cluster status condition: %w", err)
	}

	return nil
}

func (r *ClusterReconciler) serverSideApply(ctx context.Context, cluster *kcv1alpha1.Cluster, obj runtime.ApplyConfiguration) (*kcv1alpha1.Cluster, error) {
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

	err := r.Apply(ctx, obj, &client.ApplyOptions{
		FieldManager: serverSideApplyManager,
	})
	if err != nil {
		// Try to refetch the Cluster if Apply fails to update the Cluster status
		c, e := r.getCluster(ctx, key)
		if e != nil {
			return nil, fmt.Errorf("failed to get Cluster with error: %w; after error: %w", e, err)
		}

		return c, err
	}

	return r.getCluster(ctx, key)
}

func (r *ClusterReconciler) reconcileService(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	cluster, err := r.serverSideApply(ctx, cluster, serviceForCluster(cluster))
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to apply Service", cluster: cluster}
	}
	return cluster, nil
}

// configMapRestartKeys lists ConfigMap keys whose changes require a pod restart.
// The slice is ordered so the hash is deterministic without sorting.
var configMapRestartKeys = []string{"connect.properties", "jmx-exporter-config.yaml"}

func hashConfigMap(configMap *corev1.ConfigMap) string {
	h := fnv.New64a()
	for _, k := range configMapRestartKeys {
		if v, ok := configMap.Data[k]; ok {
			h.Write([]byte(k))
			h.Write([]byte(v))
		}
	}
	return strconv.FormatUint(h.Sum64(), 16)
}

func (r *ClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	log := logf.FromContext(ctx)

	configMapA, err := configMapForCluster(cluster)
	if err != nil {
		return nil, &ClusterReconciliationError{msg: err.Error(), cluster: cluster}
	}

	cluster, err = r.serverSideApply(ctx, cluster, configMapA)
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to apply ConfigMap", cluster: cluster}
	}
	if cluster == nil {
		return nil, nil
	}

	// Fetch the ConfigMap to compute the full hash of what is actually applied
	configMap := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: *configMapA.Name, Namespace: *configMapA.Namespace}, configMap)
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to get ConfigMap after apply", cluster: cluster}
	}

	configMapHash := hashConfigMap(configMap)

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

func (r *ClusterReconciler) reconcileNetworkPolicy(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	// Check if NetworkPolicy is enabled (default: true)
	enabled := true
	if cluster.Spec.NetworkPolicy != nil && cluster.Spec.NetworkPolicy.Enabled != nil {
		enabled = *cluster.Spec.NetworkPolicy.Enabled
	}

	// TODO: When the networkpolicy is disabled we need to delete it

	if !enabled {
		// NetworkPolicy is disabled, skip reconciliation
		return cluster, nil
	}

	cluster, err := r.serverSideApply(ctx, cluster, networkPolicyForCluster(cluster, r.Namespace))
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to apply NetworkPolicy", cluster: cluster}
	}
	return cluster, nil
}

func (r *ClusterReconciler) reconcileServiceAccount(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	cluster, err := r.serverSideApply(ctx, cluster, serviceAccountForCluster(cluster))
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to apply ServiceAccount", cluster: cluster}
	}
	return cluster, nil
}

func (r *ClusterReconciler) reconcilePodDisruptionBudget(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	cluster, err := r.serverSideApply(ctx, cluster, podDisruptionBudgetForCluster(cluster))
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to apply PodDisruptionBudget", cluster: cluster}
	}
	return cluster, nil
}

func (r *ClusterReconciler) reconcileDeployment(ctx context.Context, cluster *kcv1alpha1.Cluster) (*kcv1alpha1.Cluster, error) {
	log := logf.FromContext(ctx)

	deploymentA := deploymentForCluster(cluster)

	cluster, err := r.serverSideApply(ctx, cluster, deploymentA)
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to apply Deployment", cluster: cluster}
	}
	if cluster == nil {
		return nil, nil
	}

	// Update cluster condition according to deployment condition
	deployment := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: *deploymentA.Name, Namespace: *deploymentA.Namespace}, deployment)
	if err != nil {
		return nil, &ClusterReconciliationError{err: err, msg: "failed to get Deployment", cluster: cluster}
	}

	// TODO: When scaling up we shouldn't report the Available condition as False because the cluster is available

	deploymentAvailable := utils.FindStatusDeploymentCondition(deployment.Status.Conditions, "Available")
	clusterAvailable := meta.FindStatusCondition(cluster.Status.Conditions, typeAvailableCluster)
	if deploymentAvailable != nil {
		if clusterAvailable.Status != metav1.ConditionStatus(deploymentAvailable.Status) {
			clusterAvailable.Status = metav1.ConditionStatus(deploymentAvailable.Status)
			clusterAvailable.Reason = deploymentAvailable.Reason
			clusterAvailable.Message = deploymentAvailable.Message
			clusterAvailable.ObservedGeneration = cluster.Generation

			log.Info("Update Cluster status Available condition according to Deployment status", "status", clusterAvailable.Status, "reason", clusterAvailable.Reason)

			err := r.updateStatusCondition(ctx, cluster, *clusterAvailable)
			if err != nil {
				return nil, fmt.Errorf("failed to update Cluster status with Deployment status: %w", err)
			}

			return nil, nil
		}
	}

	// Deployment applied and status unchanged
	return cluster, nil
}
