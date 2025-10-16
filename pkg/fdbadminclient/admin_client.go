/*
 * admin_client.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2018-2025 Apple Inc. and the FoundationDB project authors
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

package fdbadminclient

import (
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
)

// AdminClient describes an interface for running administrative commands on a
// cluster
type AdminClient interface {
	// GetStatus gets the database's status.
	GetStatus() (*fdbv1beta2.FoundationDBStatus, error)

	// ConfigureDatabase sets the database configuration.
	ConfigureDatabase(configuration fdbv1beta2.DatabaseConfiguration, newDatabase bool) error

	// KillProcessesForUpgrade restarts processes for upgrades, this will issue 2 kill commands to make sure all
	// processes are restarted.
	KillProcessesForUpgrade(addresses []fdbv1beta2.ProcessAddress) error

	// VersionSupported reports whether we can support a cluster with a given
	// version.
	VersionSupported(version string) (bool, error)

	// GetProtocolVersion determines the protocol version that is used by a
	// version of FDB.
	GetProtocolVersion(version string) (string, error)

	// GetRestoreStatus gets the status of the current restore.
	GetRestoreStatus() (string, error)

	// Close shuts down any resources for the client once it is no longer
	// needed.
	Close() error

	// SetKnobs sets the Knobs that should be used for the commandline call.
	SetKnobs([]string)

	// WithValues will update the logger used by the current AdminClient to contain the provided key value pairs. The provided
	// arguments must be even.
	WithValues(keysAndValues ...interface{})

	// SetTimeout will overwrite the default timeout for interacting the FDB cluster.
	SetTimeout(timeout time.Duration)

	// GetProcessesUnderMaintenance will return all process groups that are currently stored to be under maintenance.
	// The result is a map with the process group ID as key and the start of the maintenance as value.
	GetProcessesUnderMaintenance() (map[fdbv1beta2.ProcessGroupID]int64, error)

	// RemoveProcessesUnderMaintenance will remove the provided process groups from the list of processes that
	// are planned to be taken down for maintenance. If a process group is not present in the list it will be ignored.
	RemoveProcessesUnderMaintenance([]fdbv1beta2.ProcessGroupID) error

	// SetProcessesUnderMaintenance will add the provided process groups to the list of processes that will be taken
	// down for maintenance. The value will be the provided time stamp. If a process group is already present in the
	// list, the timestamp will be updated.
	SetProcessesUnderMaintenance([]fdbv1beta2.ProcessGroupID, int64) error

	// GetVersionFromReachableCoordinators will return the running version based on the reachable coordinators. This method
	// can be used during version incompatible upgrades and based on the responses of the coordinators, this method will
	// assume the current running version of the cluster. If the fdbcli calls for none of the provided version return
	// a majority of reachable coordinators, the default version from the cluster.Status.RunningVersion will be returned.
	GetVersionFromReachableCoordinators() string

	ChangeCoordinators(newCoordinators []fdbv1beta2.ProcessAddress) (string, error)
}
