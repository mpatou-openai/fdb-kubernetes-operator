/*
 * fdb_operator_fixtures.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

package fixtures

import (
	"fmt"
	"log"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

func (factory *Factory) ensureFdbClusterExists(
	clusterSpec *fdbv1beta2.FoundationDBBaremetalCluster,
	config *ClusterConfig,
) (*FdbCluster, error) {
	clusterStatus, err := factory.getClusterStatus(clusterSpec.Name, clusterSpec.Namespace)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, fmt.Errorf("could not look up FDB cluster: %w", err)
	}
	if err == nil {
		log.Printf("reuse cluster: %s/%s", clusterSpec.Namespace, clusterStatus.Name)
		return factory.createFdbClusterObject(clusterStatus), nil
	}

	log.Printf("preparing to create fdb cluster: %s/%s", clusterSpec.Namespace, clusterSpec.Name)
	fdbCluster := factory.createFdbClusterObject(clusterSpec)
	err = fdbCluster.Create()
	if err != nil {
		// consider checking k8serrors.IsAlreadyExists(err), but if that's
		// the case, we're probably running concurrently with another
		// test that's using this cluster name -- may as well fail now.
		return nil, err
	}
	// Wait until the cluster CRD object exists. The caller should wait for whatever state they care about.
	fdbCluster.WaitUntilExists()
	// Wait until cluster is reconciled -- otherwise, the operator may not have
	// assigned pods, etc.
	err = fdbCluster.WaitForReconciliation(CreationTrackerLoggerOption(config.CreationTracker))
	if err != nil {
		return nil, err
	}

	config.CreationCallback(fdbCluster)

	return fdbCluster, nil
}

// GetDcIDsFromConfig returns  unique DC IDs from the current config.
// TODO (johscheuer): Should this be part of v1beta2?
func GetDcIDsFromConfig(databaseConfiguration fdbv1beta2.DatabaseConfiguration) []string {
	dcSet := map[string]struct{}{}
	dcIDs := make([]string, 0)

	for _, region := range databaseConfiguration.Regions {
		for _, dc := range region.DataCenters {
			if _, ok := dcSet[dc.ID]; ok {
				continue
			}
			dcSet[dc.ID] = struct{}{}

			dcIDs = append(dcIDs, dc.ID)
		}
	}

	return dcIDs
}
