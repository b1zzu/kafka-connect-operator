package utils

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

func PropertiesToEnvs(properties map[string]string) []corev1.EnvVar {
	envs := make([]corev1.EnvVar, 0, len(properties))
	for k, v := range properties {
		k = strings.ReplaceAll(k, ".", "_")
		k = strings.ToUpper(k)
		k = fmt.Sprintf("KAFKA_%s", k)

		envs = append(envs, corev1.EnvVar{Name: k, Value: v})
	}

	return envs
}
