/*
 * test_helper.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package internal

import (
	"math/rand"
	"strings"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
)

// CreateDefaultCluster creates a default FoundationDBBaremetalCluster for testing
func CreateDefaultCluster() *fdbv1beta2.FoundationDBBaremetalCluster {
	return &fdbv1beta2.FoundationDBBaremetalCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "operator-test-1",
			Namespace: "my-ns",
		},
		Status: fdbv1beta2.FoundationDBBaremetalClusterStatus{
			RunningVersion: fdbv1beta2.Versions.Default.String(),
		},
	}
}

// GetEnvVars returns a HashMap of EnvVars for the container
func GetEnvVars(container v1.Container) map[string]*v1.EnvVar {
	results := make(map[string]*v1.EnvVar)
	for index, env := range container.Env {
		results[env.Name] = &container.Env[index]
	}

	return results
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// GenerateRandomString can be used to generate a random string with length n
func GenerateRandomString(n int) string {
	var res strings.Builder
	for i := 0; i < n; i++ {
		res.WriteByte(letterBytes[rand.Intn(len(letterBytes))])
	}

	return res.String()
}

// SetupClusterForTest will generate all required resources like Pods for test cases.
func SetupClusterForTest(
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
	k8sClient ctrlClient.Client,
) error {

	err := NormalizeClusterSpec(cluster)
	if err != nil {
		return err
	}

	return nil
}
