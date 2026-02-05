/*
 * salt_client.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package securityupdate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	defaultSaltTimeout = 10 * time.Second
)

// SaltClient provides a minimal client for the salt API.
type SaltClient struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
}

// NewSaltClient constructs a client for the salt API.
func NewSaltClient(baseURL string, authToken string, timeout time.Duration) *SaltClient {
	if timeout <= 0 {
		timeout = defaultSaltTimeout
	}
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}

	return &SaltClient{
		baseURL:   strings.TrimRight(baseURL, "/"),
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// NeedRestartResult captures the output of needrestart and whether a kernel restart is required.
type NeedRestartResult struct {
	Minion             string
	Output             string
	KernelNeedsRestart bool
}

// NeedRestart fetches the needrestart output for a minion.
func (client *SaltClient) NeedRestart(ctx context.Context, minion string) (NeedRestartResult, error) {
	output, err := client.runCommand(ctx, minion, "needrestart -b")
	if err != nil {
		return NeedRestartResult{}, err
	}

	return NeedRestartResult{
		Minion:             minion,
		Output:             output,
		KernelNeedsRestart: kernelUpgradeRequired(output),
	}, nil
}

// RebootMinion triggers a reboot for a minion.
func (client *SaltClient) RebootMinion(ctx context.Context, minion string) error {
	_, err := client.runSaltFunction(ctx, minion, "system.reboot", nil)
	return err
}

func (client *SaltClient) runCommand(ctx context.Context, minion string, command string) (string, error) {
	response, err := client.runSaltFunction(ctx, minion, "cmd.run", []string{command})
	if err != nil {
		return "", err
	}

	result, ok := response.(string)
	if !ok {
		return "", fmt.Errorf("unexpected salt response type: %T", response)
	}

	return result, nil
}

func (client *SaltClient) runSaltFunction(
	ctx context.Context,
	minion string,
	function string,
	args []string,
) (interface{}, error) {
	payload := saltRequest{
		Client: "local",
		Target: minion,
		Func:   function,
		Args:   args,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if client.authToken != "" {
		req.Header.Set("X-Auth-Token", client.authToken)
	}

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("salt API request failed with status %s", resp.Status)
	}

	var decoded saltResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, err
	}

	return extractSaltResult(minion, decoded)
}

type saltRequest struct {
	Client string   `json:"client"`
	Target string   `json:"tgt"`
	Func   string   `json:"fun"`
	Args   []string `json:"arg,omitempty"`
}

type saltResponse struct {
	Return []map[string]interface{} `json:"return"`
}

func extractSaltResult(minion string, response saltResponse) (interface{}, error) {
	if len(response.Return) == 0 {
		return nil, fmt.Errorf("salt API response missing return data")
	}

	for _, entry := range response.Return {
		if result, ok := entry[minion]; ok {
			return result, nil
		}
	}

	return nil, fmt.Errorf("salt API response missing minion %q result", minion)
}
