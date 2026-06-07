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

package kafkaconnect

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateConnector_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/connectors" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var received Connector
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("failed to decode body: %v", err)
		}
		if received.Name != "my-connector" {
			t.Errorf("expected my-connector as name in body, got %s", received.Name)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.CreateConnector(context.Background(), &Connector{
		Name: "my-connector",
		Config: map[string]string{
			"connector.class": "com.example.Connector",
		},
		InitialState: "running",
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestCreateConnector_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/connectors" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad request"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.CreateConnector(context.Background(), &Connector{Name: "my-connector"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if _, ok := err.(*ResponseError); !ok {
		t.Fatalf("expected error of type *kafkaconnect.ResponseError, got %T", err)
	}
}

func TestRestartConnector_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/connectors/my-connector/restart" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.RestartConnector(context.Background(), "my-connector")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestRestartConnector_204(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.RestartConnector(context.Background(), "my-connector")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestRestartConnector_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.RestartConnector(context.Background(), "my-connector")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestRestartTask_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/connectors/my-connector/tasks/2/restart" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.RestartTask(context.Background(), "my-connector", 2)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestRestartTask_204(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.RestartTask(context.Background(), "my-connector", 0)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestRestartTask_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.RestartTask(context.Background(), "my-connector", 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got == "" {
		t.Fatal("expected non-empty error message")
	}
}

const testConnectorOffsetsPath = "/connectors/my-connector/offsets"

func TestGetConnectorOffsets_Success(t *testing.T) {
	expected := &ConnectorOffsets{
		Offsets: []ConnectorOffset{
			{
				Partition: map[string]any{"kafka_topic": "topic", "kafka_partition": float64(0)},
				Offset:    map[string]any{"kafka_offset": float64(12345)},
			},
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != testConnectorOffsetsPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(expected)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	offsets, err := client.GetConnectorOffsets(context.Background(), "my-connector")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(offsets.Offsets) != 1 {
		t.Fatalf("expected 1 offset, got %d", len(offsets.Offsets))
	}
}

func TestGetConnectorOffsets_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	_, err := client.GetConnectorOffsets(context.Background(), "my-connector")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestPatchConnectorOffsets_Success(t *testing.T) {
	offsets := &ConnectorOffsets{
		Offsets: []ConnectorOffset{
			{
				Partition: map[string]any{"kafka_topic": "topic", "kafka_partition": float64(0)},
				Offset:    map[string]any{"kafka_offset": float64(100)},
			},
		},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		if r.URL.Path != testConnectorOffsetsPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var received ConnectorOffsets
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("failed to decode body: %v", err)
		}
		if len(received.Offsets) != 1 {
			t.Errorf("expected 1 offset in body, got %d", len(received.Offsets))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.PatchConnectorOffsets(context.Background(), "my-connector", offsets)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestPatchConnectorOffsets_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("connector must be stopped"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.PatchConnectorOffsets(context.Background(), "my-connector", &ConnectorOffsets{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestDeleteConnectorOffsets_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != testConnectorOffsetsPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.DeleteConnectorOffsets(context.Background(), "my-connector")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestDeleteConnectorOffsets_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("connector must be stopped"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.DeleteConnectorOffsets(context.Background(), "my-connector")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
