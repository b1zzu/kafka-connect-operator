//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/b1zzu/kafka-connect-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "kafka-connect-operator"

// serviceAccountName created for the project
const serviceAccountName = "kafka-connect-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "kafka-connect-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "kafka-connect-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy-e2e")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("installing Strimzi operator")
		cmd = exec.Command("kubectl", "create", "-f",
			"https://strimzi.io/install/latest?namespace=default", "-n", "default")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install Strimzi operator")

		By("waiting for Strimzi operator to be available")
		cmd = exec.Command("kubectl", "wait", "deployment/strimzi-cluster-operator",
			"-n", "default", "--for=condition=Available", "--timeout=5m")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Strimzi operator did not become available")

		By("deploying Kafka cluster")
		cmd = exec.Command("kubectl", "apply", "-f",
			"https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml", "-n", "default")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy Kafka cluster")

		By("waiting for Kafka cluster to be ready")
		cmd = exec.Command("kubectl", "wait", "kafka/my-cluster",
			"-n", "default", "--for=condition=Ready", "--timeout=10m")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Kafka cluster did not become ready")

		By("applying sample CRs")
		cmd = exec.Command("kubectl", "apply", "-k", "config/e2e", "-n", "default")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply sample CRs")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("deleting sample CRs")
		cmd := exec.Command("kubectl", "delete", "-k", "config/e2e",
			"-n", "default", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("deleting Kafka cluster")
		cmd = exec.Command("kubectl", "delete", "-f",
			"https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml",
			"-n", "default", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("deleting Strimzi operator")
		cmd = exec.Command("kubectl", "delete", "-f",
			"https://strimzi.io/install/latest?namespace=default",
			"-n", "default", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("cleaning up the curl pod for metrics")
		cmd = exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("cleaning up the curl pod for jmx metrics")
		cmd = exec.Command("kubectl", "delete", "pod", "curl-jmx-metrics",
			"-n", "default", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy-e2e")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}

			By("Fetching events from default namespace")
			cmd = exec.Command("kubectl", "get", "events", "-n", "default", "--sort-by=.lastTimestamp")
			defaultEvents, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Default namespace events:\n%s", defaultEvents)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get default namespace events: %s", err)
			}

			By("Fetching Kafka Connect pod logs")
			cmd = exec.Command("kubectl", "logs", "deployment/my-cluster-connect",
				"-n", "default", "--tail=100")
			connectLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kafka Connect logs:\n%s", connectLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kafka Connect logs: %s", err)
			}

			By("Fetching Connector CR status")
			cmd = exec.Command("kubectl", "get", "connector", "my-connector",
				"-n", "default", "-o", "yaml")
			connectorStatus, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Connector CR:\n%s", connectorStatus)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Connector CR: %s", err)
			}

			By("Fetching Cluster CR status")
			cmd = exec.Command("kubectl", "get", "cluster", "my-cluster-connect",
				"-n", "default", "-o", "yaml")
			clusterStatus, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Cluster CR:\n%s", clusterStatus)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Cluster CR: %s", err)
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=kafka-connect-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		It("should deploy Connect and reach Connector Running state", func() {
			By("waiting for the Connect deployment to be available")
			verifyConnectDeployment := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment", "my-cluster-connect",
					"-n", "default",
					"-o", "jsonpath={.status.conditions[?(@.type=='Available')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Connect deployment not yet available")
			}
			Eventually(verifyConnectDeployment, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("waiting for Connector my-connector to be Ready")
			verifyConnectorReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "connector", "my-connector",
					"-n", "default",
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Connector not yet Ready")
			}
			Eventually(verifyConnectorReady, 5*time.Minute, 10*time.Second).Should(Succeed())

			By("verifying the Connector Ready condition reason")
			cmd := exec.Command("kubectl", "get", "connector", "my-connector",
				"-n", "default",
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(Equal("Running"), "Unexpected Connector condition reason")
		})

		It("should expose JMX Exporter metrics on the Connect pod", func() {
			By("getting the Kafka Connect pod IP")
			var connectPodIP string
			getConnectPodIP := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods",
					"-l", "app.kubernetes.io/name=kafka-connect,app.kubernetes.io/instance=my-cluster-connect",
					"-n", "default",
					"-o", "jsonpath={.items[0].status.podIP}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).NotTo(BeEmpty())
				connectPodIP = output
			}
			Eventually(getConnectPodIP).Should(Succeed())

			By("curling the metrics endpoint on the Connect pod")
			verifyMetrics := func(g Gomega) {
				cmd := exec.Command("kubectl", "run", "curl-jmx-metrics",
					"--restart=Never", "--rm", "-i",
					"--image=curlimages/curl:latest",
					"-n", "default",
					"--", "curl", "-sf", fmt.Sprintf("http://%s:9404/metrics", connectPodIP))
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to curl metrics endpoint")
				g.Expect(output).To(ContainSubstring("# TYPE"),
					"Expected Prometheus metrics output")
			}
			Eventually(verifyMetrics, 3*time.Minute, 10*time.Second).Should(Succeed())
		})

		It("should report a start-delayed connector as not running until the delay elapses", func() {
			By("creating the start-delayed test connector")
			cmd := exec.Command("kubectl", "apply", "-f",
				"config/e2e/test_connector.yaml", "-n", "default")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "-f",
					"config/e2e/test_connector.yaml", "-n", "default", "--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			getConnectorReadyStatus := func() (string, error) {
				cmd := exec.Command("kubectl", "get", "connector", "my-test-connector",
					"-n", "default",
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				return utils.Run(cmd)
			}

			getConnectorReadyReason := func() (string, error) {
				cmd := exec.Command("kubectl", "get", "connector", "my-test-connector",
					"-n", "default",
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}")
				return utils.Run(cmd)
			}

			getRunningTaskCount := func() (string, error) {
				cmd := exec.Command("kubectl", "get", "connector", "my-test-connector",
					"-n", "default",
					"-o", "jsonpath={range .status.tasks[?(@.state=='RUNNING')]}x{end}")
				return utils.Run(cmd)
			}

			By("verifying it stays not-Ready with no running tasks during the start delay")
			notRunning := func(g Gomega) {
				status, err := getConnectorReadyStatus()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).NotTo(Equal("True"), "Connector should not be Ready during the start delay")

				runningTasks, err := getRunningTaskCount()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(runningTasks).To(BeEmpty(), "Connector should have no running tasks during the start delay")
			}
			Consistently(notRunning, 8*time.Second, 2*time.Second).Should(Succeed())

			By("verifying it becomes Ready/Running once the start delay elapses")
			running := func(g Gomega) {
				status, err := getConnectorReadyStatus()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"), "Connector not yet Ready")

				reason, err := getConnectorReadyReason()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).To(Equal("Running"), "Unexpected Connector condition reason")

				runningTasks, err := getRunningTaskCount()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(runningTasks).NotTo(BeEmpty(), "Connector should have at least one running task")
			}
			Eventually(running, 30*time.Second, 2*time.Second).Should(Succeed())
		})
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
