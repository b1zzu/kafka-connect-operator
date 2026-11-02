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
	"math/rand/v2"
	"os/exec"
	"strings"
	"sync"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kafkaconnect "github.com/b1zzu/kafka-connect-operator/pkg/kafka-connect"
)

// KafkaConnectClientFunc creates a Kafka Connect client for the Cluster with
// the given namespace and name.
type KafkaConnectClientFunc func(namespace, cluster string) (*kafkaconnect.Client, error)

// clusterURL returns the REST API URL of the Cluster Service.
func clusterURL(namespace, cluster string) string {
	return fmt.Sprintf("http://%s.%s.svc:8083", cluster, namespace)
}

// NewDefaultKafkaConnectClientFunc creates a Kafka Connect client using the
// in-cluster Service endpoint.
func NewDefaultKafkaConnectClientFunc(namespace, cluster string) (*kafkaconnect.Client, error) {
	return newInstrumentedClient(namespace, cluster, clusterURL(namespace, cluster)), nil
}

type PortForwardProcess struct {
	Port      int
	IsRunning bool
}

var (
	portForwardProcesses   = map[string]*PortForwardProcess{}
	portForwardProcessesMu sync.Mutex
)

// NewPortForwardKafkaConnectClientFunc is a factory func that start the kubectl port-forward
// in the bacgrkound and return the the Kafka Connect client using the port-forward as endpoint.
func NewPortForwardKafkaConnectClientFunc(namespace, cluster string) (*kafkaconnect.Client, error) {
	log := logf.FromContext(context.TODO())

	portForwardProcessesMu.Lock()
	defer portForwardProcessesMu.Unlock()

	k := fmt.Sprintf("%s/%s", namespace, cluster)
	var process *PortForwardProcess
	if p, ok := portForwardProcesses[k]; ok {
		process = p
	}

	// Check if kubectl port-forward is running
	if process != nil && !process.IsRunning {
		process = nil
	}

	if process == nil {
		process = &PortForwardProcess{
			Port:      49152 + rand.N(65535-49152+1), // Random port between 49152 and 65535
			IsRunning: false,
		}

		// Start a new kubectl port-forward process
		cmd := exec.Command(
			"kubectl", "port-forward",
			"--namespace", namespace,
			fmt.Sprintf("services/%s", cluster),
			fmt.Sprintf("%d:8083", process.Port))

		// Run a bacgrkound goroutine to update the process when exit
		log.Info("Starting kubect port-forward", "command", strings.Join(cmd.Args, " "))
		go func() {
			process.IsRunning = true
			out, err := cmd.CombinedOutput()
			log.Error(err, "Kubectl port-forward exit", "output", string(out))
			process.IsRunning = false
		}()

		// Wait 1 second after starting to process to make sure the kubectl port-fowarding
		// did not fail immediately
		time.Sleep(time.Second)

		if !process.IsRunning {
			return nil, fmt.Errorf("failed to run kubectl port-forward on local port %d", process.Port)
		}

		portForwardProcesses[k] = process
	}

	// Kubectl port-forward is running from at-least 1 second
	endpoint := fmt.Sprintf("http://localhost:%d", process.Port)
	return newInstrumentedClient(namespace, cluster, endpoint), nil
}

func newInstrumentedClient(namespace, cluster, endpoint string) *kafkaconnect.Client {
	return kafkaconnect.NewClient(endpoint).WithTransport(&instrumentedTransport{
		namespace: namespace,
		cluster:   cluster,
	})
}
