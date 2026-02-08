// Package utils
package utils

import (
	appsv1 "k8s.io/api/apps/v1"
)

// FindStatusDeploymentCondition finds the conditionType in conditions.
func FindStatusDeploymentCondition(
	conditions []appsv1.DeploymentCondition,
	conditionType string,
) *appsv1.DeploymentCondition {
	for i := range conditions {
		if string(conditions[i].Type) == conditionType {
			return &conditions[i]
		}
	}

	return nil
}
