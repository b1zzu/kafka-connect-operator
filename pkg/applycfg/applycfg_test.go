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

package applycfg

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestEnvVars(t *testing.T) {
	envVars := []corev1.EnvVar{
		{Name: "PLAIN", Value: "value"},
		{Name: "EMPTY"},
		{Name: "FIELD", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"},
		}},
		{Name: "RESOURCE", ValueFrom: &corev1.EnvVarSource{
			ResourceFieldRef: &corev1.ResourceFieldSelector{
				ContainerName: "kafka-connect",
				Resource:      "limits.memory",
				Divisor:       resource.MustParse("1Mi"),
			},
		}},
		{Name: "CONFIGMAP", ValueFrom: &corev1.EnvVarSource{
			ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cm"},
				Key:                  "key",
				Optional:             new(true),
			},
		}},
		{Name: "SECRET", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "secret"},
				Key:                  "key",
			},
		}},
		{Name: "FILE", ValueFrom: &corev1.EnvVarSource{
			FileKeyRef: &corev1.FileKeySelector{VolumeName: "vol", Path: "env", Key: "key"},
		}},
	}

	want, err := json.Marshal(envVars)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(EnvVars(envVars))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("EnvVars mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestEnvVarsOmitsEmptyOptionalFields(t *testing.T) {
	envVars := []corev1.EnvVar{
		{Name: "FIELD", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
		}},
		{Name: "RESOURCE", ValueFrom: &corev1.EnvVarSource{
			ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "limits.cpu"},
		}},
	}

	got, err := json.Marshal(EnvVars(envVars))
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"name":"FIELD","valueFrom":{"fieldRef":{"fieldPath":"status.podIP"}}},` +
		`{"name":"RESOURCE","valueFrom":{"resourceFieldRef":{"resource":"limits.cpu"}}}]`
	if string(got) != want {
		t.Errorf("EnvVars mismatch\n got: %s\nwant: %s", got, want)
	}
}
