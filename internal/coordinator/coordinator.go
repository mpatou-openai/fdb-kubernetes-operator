/*
 * coordinator.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package coordinator

import (
	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/pkg/fdbadminclient"
	"github.com/go-logr/logr"
)

// ChangeCoordinators will change the coordinators and set the new connection string on the FoundationDBCluster resource.
func ChangeCoordinators(
	logger logr.Logger,
	adminClient fdbadminclient.AdminClient,
	cluster *fdbv1beta2.FoundationDBCluster,
	status *fdbv1beta2.FoundationDBStatus,
) error {
	coordinators, err := SelectCoordinators(logger, cluster, status)
	if err != nil {
		return err
	}

	logger.Info("Final coordinators candidates", "coordinators", coordinators)
	connectionString, err := adminClient.ChangeCoordinators(coordinators)
	if err != nil {
		return err
	}
	cluster.Status.ConnectionString = connectionString

	return nil
}

// selectCandidates is a helper for Reconcile that picks non-excluded, not-being-removed class-matching process groups.

// selectCoordinatorsLocalities will return a set of new coordinators.

// SelectCoordinators will return a set of new coordinators.
func SelectCoordinators(
	logger logr.Logger,
	cluster *fdbv1beta2.FoundationDBCluster,
	status *fdbv1beta2.FoundationDBStatus,
) ([]fdbv1beta2.ProcessAddress, error) {
	var coordinatorAddresses []fdbv1beta2.ProcessAddress

	return coordinatorAddresses, nil
}

// GetCoordinatorAddress returns the coordinator address.
func GetCoordinatorAddress(
	cluster *fdbv1beta2.FoundationDBCluster,
) fdbv1beta2.ProcessAddress {
	var address fdbv1beta2.ProcessAddress

	// FIXME: implent that

	return address
}
