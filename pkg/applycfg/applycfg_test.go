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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestProbe_Nil(t *testing.T) {
	result := Probe(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestProbe_Exec(t *testing.T) {
	probe := &corev1.Probe{
		InitialDelaySeconds: 10,
		TimeoutSeconds:      5,
		PeriodSeconds:       30,
		SuccessThreshold:    1,
		FailureThreshold:    3,
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/sh", "-c", "echo hello"},
			},
		},
	}

	result := Probe(probe)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.InitialDelaySeconds == nil {
		t.Error("expected InitialDelaySeconds to be set")
	} else if *result.InitialDelaySeconds != probe.InitialDelaySeconds {
		t.Errorf("expected InitialDelaySeconds %d, got %d", probe.InitialDelaySeconds, *result.InitialDelaySeconds)
	}

	if result.Exec == nil {
		t.Fatal("expected Exec to be set")
	}
	if len(result.Exec.Command) != len(probe.Exec.Command) {
		t.Errorf("expected Command length %d, got %d", len(probe.Exec.Command), len(result.Exec.Command))
	}
}

func TestProbe_HTTPGet(t *testing.T) {
	path := "/health"
	host := "localhost"
	scheme := corev1.URISchemeHTTP
	port := intstr.FromInt(8080)

	probe := &corev1.Probe{
		InitialDelaySeconds: 15,
		TimeoutSeconds:      10,
		PeriodSeconds:       20,
		SuccessThreshold:    1,
		FailureThreshold:    3,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   path,
				Host:   host,
				Scheme: scheme,
				Port:   port,
				HTTPHeaders: []corev1.HTTPHeader{
					{Name: "X-Custom-Header", Value: "test-value"},
				},
			},
		},
	}

	result := Probe(probe)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.HTTPGet == nil {
		t.Fatal("expected HTTPGet to be set")
	}

	if result.HTTPGet.Path == nil {
		t.Error("expected Path to be set")
	} else if *result.HTTPGet.Path != path {
		t.Errorf("expected Path %s, got %s", path, *result.HTTPGet.Path)
	}

	if result.HTTPGet.Host == nil {
		t.Error("expected Host to be set")
	} else if *result.HTTPGet.Host != host {
		t.Errorf("expected Host %s, got %s", host, *result.HTTPGet.Host)
	}

	if result.HTTPGet.Scheme == nil {
		t.Error("expected Scheme to be set")
	} else if *result.HTTPGet.Scheme != scheme {
		t.Errorf("expected Scheme %s, got %s", scheme, *result.HTTPGet.Scheme)
	}

	if result.HTTPGet.Port == nil {
		t.Error("expected Port to be set")
	} else if *result.HTTPGet.Port != port {
		t.Errorf("expected Port %v, got %v", port, *result.HTTPGet.Port)
	}

	if len(result.HTTPGet.HTTPHeaders) != 1 {
		t.Errorf("expected 1 HTTPHeader, got %d", len(result.HTTPGet.HTTPHeaders))
	} else {
		if result.HTTPGet.HTTPHeaders[0].Name == nil {
			t.Error("expected header Name to be set")
		} else if *result.HTTPGet.HTTPHeaders[0].Name != "X-Custom-Header" {
			t.Errorf("expected header Name X-Custom-Header, got %s", *result.HTTPGet.HTTPHeaders[0].Name)
		}

		if result.HTTPGet.HTTPHeaders[0].Value == nil {
			t.Error("expected header Value to be set")
		} else if *result.HTTPGet.HTTPHeaders[0].Value != "test-value" {
			t.Errorf("expected header Value test-value, got %s", *result.HTTPGet.HTTPHeaders[0].Value)
		}
	}
}

func TestProbe_TCPSocket(t *testing.T) {
	host := "localhost"
	port := intstr.FromInt(8080)

	probe := &corev1.Probe{
		InitialDelaySeconds: 5,
		TimeoutSeconds:      3,
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{
				Host: host,
				Port: port,
			},
		},
	}

	result := Probe(probe)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.TCPSocket == nil {
		t.Fatal("expected TCPSocket to be set")
	}

	if result.TCPSocket.Host == nil {
		t.Error("expected Host to be set")
	} else if *result.TCPSocket.Host != host {
		t.Errorf("expected Host %s, got %s", host, *result.TCPSocket.Host)
	}

	if result.TCPSocket.Port == nil {
		t.Error("expected Port to be set")
	} else if *result.TCPSocket.Port != port {
		t.Errorf("expected Port %v, got %v", port, *result.TCPSocket.Port)
	}
}

func TestProbe_GRPC(t *testing.T) {
	port := int32(9090)
	service := "grpc.health.v1.Health"

	probe := &corev1.Probe{
		InitialDelaySeconds: 10,
		ProbeHandler: corev1.ProbeHandler{
			GRPC: &corev1.GRPCAction{
				Port:    port,
				Service: &service,
			},
		},
	}

	result := Probe(probe)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.GRPC == nil {
		t.Fatal("expected GRPC to be set")
	}

	if result.GRPC.Port == nil {
		t.Error("expected Port to be set")
	} else if *result.GRPC.Port != port {
		t.Errorf("expected Port %d, got %d", port, *result.GRPC.Port)
	}

	if result.GRPC.Service == nil {
		t.Error("expected Service to be set")
	} else if *result.GRPC.Service != service {
		t.Errorf("expected Service %s, got %s", service, *result.GRPC.Service)
	}
}

func TestProbe_ZeroValues(t *testing.T) {
	probe := &corev1.Probe{
		InitialDelaySeconds: 0,
		TimeoutSeconds:      0,
		PeriodSeconds:       0,
		SuccessThreshold:    0,
		FailureThreshold:    0,
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"echo", "hello"},
			},
		},
	}

	result := Probe(probe)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Zero values should still be assigned as pointers according to the pattern
	if result.InitialDelaySeconds == nil {
		t.Error("expected InitialDelaySeconds to be set even for zero value")
	} else if *result.InitialDelaySeconds != int32(0) {
		t.Errorf("expected InitialDelaySeconds 0, got %d", *result.InitialDelaySeconds)
	}

	if result.SuccessThreshold == nil {
		t.Error("expected SuccessThreshold to be set even for zero value")
	} else if *result.SuccessThreshold != int32(0) {
		t.Errorf("expected SuccessThreshold 0, got %d", *result.SuccessThreshold)
	}
}
