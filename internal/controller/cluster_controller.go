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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafkaconnectv1alpha1 "github.com/b1zzu/kafka-connect-operator/api/v1alpha1"
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

	cluster := &kafkaconnectv1alpha1.Cluster{}
	err := r.Get(ctx, req.NamespacedName, cluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Cluster resource not found. Ingoring since object must be delete")
			return ctrl.Result{}, nil
		}
	}

	// Set the status to Unknown when no status is available
	if len(cluster.Status.Conditions) == 0 {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Available", Status: metav1.ConditionUnknown, Reason: "Reconciling"})
		err := r.Status().Update(ctx, cluster)
		if err != nil {
			log.Error(err, "Failed to update Cluster status")
			return ctrl.Result{}, err
		}

		// Always refetch afeter updating
		err = r.Get(ctx, req.NamespacedName, cluster)
		if err != nil {
			log.Error(err, "Failed to re-fetch Cluster")
			return ctrl.Result{}, err
		}
	}

	// Define a new deployment
	desiredDeployment, err := r.deploymentForCluster(cluster)
	if err != nil {
		log.Error(err, "Failed to define desired Deployment resource for Cluster")

		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type: "Available", Status: metav1.ConditionFalse, Reason: "Reconciling",
			Message: fmt.Sprintf("Failed to create Deployment (%s): (%s)", cluster.Name, err),
		})
		err := r.Status().Update(ctx, cluster)
		if err != nil {
			log.Error(err, "Failed to update Cluster status")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, err
	}

	// Check if the deployment already exists, if not create a new one
	foundDeployment := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, foundDeployment)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to get Deployment")
			return ctrl.Result{}, err
		}

		log.Info("Creating a new Deployment")
		err = r.Create(ctx, desiredDeployment)
		if err != nil {
			log.Error(err, "Failed to create new Deployment")
			return ctrl.Result{}, err
		}

		// Deployment created successfully, reconcile after 1 minute
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	// Define a new config map
	desiredConfigMap, err := r.configMapForCluster(cluster)
	if err != nil {
		log.Error(err, "Failed to define desired ConfigMap resource for Cluster")
		return ctrl.Result{}, err
	}

	// Check if the config map already exists
	foundConfigMap := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, foundConfigMap)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to get ConfigMap")
			return ctrl.Result{}, err
		}

		log.Info("Creating a new ConfigMap")
		err = r.Create(ctx, desiredConfigMap)
		if err != nil {
			log.Error(err, "Failed to create new ConfigMap")
			return ctrl.Result{}, err
		}

		// Deployment created successfully, reconcile after 1 minute
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: "Available", Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("Deployment (%s) created successfully", cluster.Name),
	})
	err = r.Status().Update(ctx, cluster)
	if err != nil {
		log.Error(err, "Failed to update Cluster status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kafkaconnectv1alpha1.Cluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Named("cluster").
		Complete(r)
}

func (r *ClusterReconciler) deploymentForCluster(cluster *kafkaconnectv1alpha1.Cluster) (*appsv1.Deployment, error) {
	image := "apache/kafka:4.1.1"

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app.kubernetes.io/name": "todo"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app.kubernetes.io/name": "todo"},
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
					},
					Containers: []corev1.Container{{
						Image:           image,
						Name:            "kafka-connect",
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"/opt/kafka/bin/connect-distributed.sh", "/config/connect.properties"},
						Ports: []corev1.ContainerPort{{
							ContainerPort: 8083,
							Name:          "todo",
						}},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "config",
							ReadOnly:  true,
							MountPath: "/config",
						}},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             ptr.To(true),
							RunAsUser:                ptr.To(int64(65534)),
							AllowPrivilegeEscalation: ptr.To(false),
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{"ALL"},
							},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: cluster.Name,
								},
							},
						},
					}},
				},
			},
		},
	}

	// Set the ownerRef on the Deployment
	err := ctrl.SetControllerReference(cluster, dep, r.Scheme)
	if err != nil {
		return nil, err
	}
	return dep, nil
}

func (r *ClusterReconciler) configMapForCluster(cluster *kafkaconnectv1alpha1.Cluster) (*corev1.ConfigMap, error) {
	properties := &strings.Builder{}
	for k, v := range cluster.Spec.Properties {
		fmt.Fprintf(properties, "%s=%s\n", k, v)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		Data: map[string]string{
			"connect.properties": properties.String(),
		},
	}

	err := ctrl.SetControllerReference(cluster, cm, r.Scheme)
	if err != nil {
		return nil, err
	}
	return cm, nil
}
