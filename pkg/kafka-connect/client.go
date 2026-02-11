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

// Package kafkaconnect
package kafkaconnect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	client   *http.Client
	endpoint string
}

type Connector struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

type ConnectorStatus struct {
	Name      string                   `json:"name"`
	Connector ConnectorStatusConnector `json:"connector"`
	Tasks     []ConnectorStatusTask    `json:"tasks"`
}

type ConnectorStatusConnector struct {
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

type ConnectorStatusTask struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

// NewClient create a a http client to interact with kafka connect.
//
// The endpoint is the kafka connect url prefix to each
// request, example: "http://localhost:8083"
func NewClient(endpoint string) *Client {
	client := &http.Client{
		Timeout: time.Second * 30,
	}

	return &Client{
		client:   client,
		endpoint: endpoint,
	}
}

// TODO: Return a detail error structure with the parsed error body

func (c *Client) GetConnector(ctx context.Context, name string) (*Connector, error) {
	url := fmt.Sprintf("%s/connectors/%s", c.endpoint, name)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			// connector not found
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get connector with status: %s", res.Status)
	}

	connector := &Connector{}
	d := json.NewDecoder(res.Body)
	err = d.Decode(connector)
	if err != nil {
		return nil, fmt.Errorf("failed to decode connector response: %w", err)
	}

	return connector, nil
}

func (c *Client) CreateConnector(ctx context.Context, connector *Connector) error {
	url := fmt.Sprintf("%s/connectors", c.endpoint)

	body := &bytes.Buffer{}
	e := json.NewEncoder(body)
	err := e.Encode(connector)
	if err != nil {
		return fmt.Errorf("failed to encode connector: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create connector: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 201 && res.StatusCode != 200 {
		rb, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to create connector with status: %s; body: %s", res.Status, string(rb))
	}

	return nil
}

func (c *Client) GetConnectorStatus(ctx context.Context, name string) (*ConnectorStatus, error) {
	url := fmt.Sprintf("%s/connectors/%s/status", c.endpoint, name)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector status: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get connector status with status: %s", res.Status)
	}

	status := &ConnectorStatus{}
	d := json.NewDecoder(res.Body)
	err = d.Decode(status)
	if err != nil {
		return nil, fmt.Errorf("failed to decode connector status response: %w", err)
	}

	return status, nil
}

func (c *Client) UpdateConnectorConfig(ctx context.Context, name string, config map[string]string) error {
	url := fmt.Sprintf("%s/connectors/%s/config", c.endpoint, name)

	body := &bytes.Buffer{}
	e := json.NewEncoder(body)
	err := e.Encode(config)
	if err != nil {
		return fmt.Errorf("failed to encode connector config: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", url, body)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to update connector config: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 && res.StatusCode != 201 {
		rb, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to update connector config with status: %s; body: %s", res.Status, string(rb))
	}

	return nil
}

func (c *Client) DeleteConnector(ctx context.Context, name string) error {
	url := fmt.Sprintf("%s/connectors/%s", c.endpoint, name)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete connector: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 204 {
		if res.StatusCode == 404 {
			// connector already deleted
			return nil
		}

		rb, _ := io.ReadAll(res.Body)
		return fmt.Errorf("failed to delete connector with status: %s; body: %s", res.Status, string(rb))
	}

	return nil
}
