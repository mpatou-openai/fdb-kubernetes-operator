/*
 * coordination_client.go
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

package fdbclient

import (
	"path"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/go-logr/logr"
)

// CoordinationClient provides access to coordination keys in FDB.
type CoordinationClient struct {
	cluster   *fdbv1beta2.FoundationDBBaremetalCluster
	logger    logr.Logger
	fdbClient fdbLibClient
	timeout   time.Duration
}

// NewCoordinationClient constructs a new coordination client for the cluster.
func NewCoordinationClient(
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
	logger logr.Logger,
	timeout time.Duration,
) *CoordinationClient {
	if timeout <= 0 {
		timeout = DefaultCLITimeout
	}

	return &CoordinationClient{
		cluster: cluster,
		logger:  logger.WithName("fdb-coordination"),
		fdbClient: &realFdbLibClient{
			cluster: cluster,
			logger:  logger.WithName("fdb-coordination"),
		},
		timeout: timeout,
	}
}

// GetEntries returns the coordination entries under the prefix.
func (client *CoordinationClient) GetEntries(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.fdbClient.getGlobalCoordinationKeys(prefix)
}

// SetEntry writes a single coordination entry with a timestamp.
func (client *CoordinationClient) SetEntry(prefix string, key string, timestamp time.Time) error {
	entryKey := path.Join(client.cluster.GetLockPrefix(), prefix, key)
	payload, err := tuple.Tuple{timestamp.Unix()}.Pack()
	if err != nil {
		return err
	}

	return client.fdbClient.executeTransaction(func(tr fdb.Transaction) error {
		tr.Set([]byte(entryKey), payload)
		return nil
	}, client.timeout)
}

// ClearEntry removes a single coordination entry.
func (client *CoordinationClient) ClearEntry(prefix string, key string) error {
	entryKey := path.Join(client.cluster.GetLockPrefix(), prefix, key)
	return client.fdbClient.executeTransaction(func(tr fdb.Transaction) error {
		tr.Clear([]byte(entryKey))
		return nil
	}, client.timeout)
}
