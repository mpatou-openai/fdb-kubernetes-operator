/*
 * config.go
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
	"fmt"
	"os"
	"regexp"
	"sort"

	"sigs.k8s.io/yaml"
)

// CloudConfig models the cloud configuration used for security updates.
type CloudConfig struct {
	Subscriptions []Subscription `json:"subscriptions" yaml:"subscriptions"`
}

// Subscription defines a subscription and its VMSS entries.
type Subscription struct {
	Name string       `json:"name" yaml:"name"`
	VMSS []VMSSConfig `json:"vmss" yaml:"vmss"`
}

// VMSSConfig describes a VMSS and the nodes to target.
type VMSSConfig struct {
	Name         string   `json:"name" yaml:"name"`
	NodeRegex    string   `json:"nodeRegex" yaml:"nodeRegex"`
	Nodes        []string `json:"nodes" yaml:"nodes"`
	ProcessClass string   `json:"processClass" yaml:"processClass"`
}

// MinionTarget captures a salt minion derived from the VMSS config.
type MinionTarget struct {
	Subscription string
	VMSS         string
	Node         string
	ProcessClass string
	MinionName   string
}

// LoadCloudConfig reads the cloud configuration from a YAML/JSON file.
func LoadCloudConfig(path string) (*CloudConfig, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := &CloudConfig{}
	if err := yaml.Unmarshal(contents, config); err != nil {
		return nil, err
	}

	return config, nil
}

// MinionTargets returns the list of salt minions for a cluster based on the config.
func (config *CloudConfig) MinionTargets(clusterName string) ([]MinionTarget, error) {
	if config == nil {
		return nil, nil
	}

	minions := make([]MinionTarget, 0)
	for _, subscription := range config.Subscriptions {
		for _, vmss := range subscription.VMSS {
			selector, err := buildNodeSelector(vmss.NodeRegex)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid nodeRegex for subscription %q vmss %q: %w",
					subscription.Name,
					vmss.Name,
					err,
				)
			}

			for _, node := range vmss.Nodes {
				if selector != nil && !selector.MatchString(node) {
					continue
				}

				minions = append(minions, MinionTarget{
					Subscription: subscription.Name,
					VMSS:         vmss.Name,
					Node:         node,
					ProcessClass: vmss.ProcessClass,
					MinionName:   buildSaltMinionName(vmss.Name, clusterName, node),
				})
			}
		}
	}

	sort.Slice(minions, func(i, j int) bool {
		return minions[i].MinionName < minions[j].MinionName
	})

	return minions, nil
}

func buildNodeSelector(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, nil
	}

	return regexp.Compile(pattern)
}

// buildSaltMinionName builds the minion name from VMSS, cluster, and node.
func buildSaltMinionName(vmssName, clusterName, nodeName string) string {
	return fmt.Sprintf("%s-%s-%s", vmssName, clusterName, nodeName)
}
