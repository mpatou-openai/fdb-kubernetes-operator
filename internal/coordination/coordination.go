/*
 * coordination.go
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

package coordination

import (
	"fmt"
	"strings"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/go-logr/logr"
)

// IgnoreMissingProcessDuration defines the duration a Process Group must have the MissingProcess condition to be
// ignored in the exclusion check and let the exclusions potentially move forward.
// We should consider to make this configurable in the long term.
const IgnoreMissingProcessDuration = 5 * time.Minute

// Ensure that the WaitTimeError implements the error interface.
var _ error = (*WaitTimeError)(nil)

// WaitTimeError represent and error when the last pending process groups was added earlier than the wait time allows.
type WaitTimeError struct {
	// timeSinceLastPendingWasAdded represents the time since the last pending process group was added.
	timeSinceLastPendingWasAdded time.Duration
	// waitTime represents the minimum time the operator should be waiting since a pending process group was added.
	waitTime time.Duration
}

// Error returns the error string for this error.
func (err WaitTimeError) Error() string {
	return fmt.Sprintf(
		"last pending process group was added: %s ago, wait time for pending additions is: %s",
		err.timeSinceLastPendingWasAdded.String(),
		err.waitTime.String(),
	)
}

// GetWaitTime returns the difference between the wait time and the time since the last pending process group was added.
// The result can be used to delay the reconcile queue.
func (err WaitTimeError) GetWaitTime() time.Duration {
	return err.waitTime - err.timeSinceLastPendingWasAdded
}

// AllProcessesReady will return all the process groups that are in the pending and ready list. If the time since the
// last update was made is earlier than the wait time, a WaitTimeError will be returned.
func AllProcessesReady(
	logger logr.Logger,
	pendingProcessGroups map[fdbv1beta2.ProcessGroupID]time.Time,
	readyProcessGroups map[fdbv1beta2.ProcessGroupID]time.Time,
	waitTime time.Duration,
) error {
	notReadyProcesses, _, err := getNotReadyProcesses(
		logger,
		pendingProcessGroups,
		readyProcessGroups,
		waitTime,
	)
	if err != nil {
		return err
	}

	if len(notReadyProcesses) > 0 {
		return fmt.Errorf("not all processes are ready: %v", strings.Join(notReadyProcesses, ","))
	}

	if len(pendingProcessGroups) != len(readyProcessGroups) {
		return fmt.Errorf(
			"not all processes are ready: mismatch in %d pending processes and %d ready processes",
			len(pendingProcessGroups),
			len(readyProcessGroups),
		)
	}

	return nil
}

func getNotReadyProcesses(
	logger logr.Logger,
	pendingProcessGroups map[fdbv1beta2.ProcessGroupID]time.Time,
	readyProcessGroups map[fdbv1beta2.ProcessGroupID]time.Time,
	waitTime time.Duration,
) ([]string, time.Time, error) {
	timestampLastAdded := time.Time{}

	notReadyProcesses := make(
		[]string,
		0,
		max(len(pendingProcessGroups)-len(readyProcessGroups), 0),
	)
	for pending, timestamp := range pendingProcessGroups {
		// Tester processes are not managed over the global coordination.
		if pending.GetProcessClass() == fdbv1beta2.ProcessClassTest {
			continue
		}

		if timestamp.After(timestampLastAdded) {
			timestampLastAdded = timestamp
		}

		readyTimestamp, isReady := readyProcessGroups[pending]
		if isReady {
			if readyTimestamp.After(timestampLastAdded) {
				timestampLastAdded = readyTimestamp
			}

			continue
		}

		notReadyProcesses = append(notReadyProcesses, string(pending))
		logger.Info(
			"found process group in pending list but not in ready list",
			"processGroupID",
			pending,
		)
	}

	// Check if the last addition was longer ago than the wait time duration.
	if time.Since(timestampLastAdded) < waitTime {
		return notReadyProcesses, timestampLastAdded, WaitTimeError{
			timeSinceLastPendingWasAdded: time.Since(timestampLastAdded),
			waitTime:                     waitTime,
		}
	}

	return notReadyProcesses, timestampLastAdded, nil
}

// AllProcessesReadyForExclusion will return all the process groups that are in the pending and ready list. If the time since the
// last update was made is earlier than the wait time, a WaitTimeError will be returned. It implements a similar logic to the
// AllProcessesReady with some modifications for the exclude reconciler to ensure that the excludes can mode forward.
func AllProcessesReadyForExclusion(
	logger logr.Logger,
	pendingProcessGroups map[fdbv1beta2.ProcessGroupID]time.Time,
	readyProcessGroups map[fdbv1beta2.ProcessGroupID]time.Time,
	waitTime time.Duration,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	notReadyProcesses, timestampLastAdded, err := getNotReadyProcesses(
		logger,
		pendingProcessGroups,
		readyProcessGroups,
		waitTime,
	)
	if err != nil {
		return nil, err
	}

	// In case that not all processes are ready we have to check when the last time an update to the pending and
	// ready list has happened.
	if len(notReadyProcesses) > 0 {
		// If there was a change in the last 5 minutes (or longer), we will return an error message and wait for the processes
		// to get  ready.
		if time.Since(timestampLastAdded) < max(5*waitTime, IgnoreMissingProcessDuration) {
			return nil, fmt.Errorf(
				"not all processes are ready: %v",
				strings.Join(notReadyProcesses, ","),
			)
		}

		// In case that we already waited for 5 minutes, start the exclusion on the processes that are marked to be ready.
		// Otherwise, we might be blocking forever, e.g. in cases where the quota is limited.
		return readyProcessGroups, nil
	}

	if len(pendingProcessGroups) != len(readyProcessGroups) {
		return nil, fmt.Errorf(
			"not all processes are ready: mismatch in %d pending processes and %d ready processes",
			len(pendingProcessGroups),
			len(readyProcessGroups),
		)
	}

	return readyProcessGroups, nil
}

// GetAddressesFromStatus will return the process addresses for the provided processGroups based on the provided machine-readable status.
func GetAddressesFromStatus(
	logger logr.Logger,
	status *fdbv1beta2.FoundationDBStatus,
	processGroups map[fdbv1beta2.ProcessGroupID]time.Time,
) []fdbv1beta2.ProcessAddress {
	addresses := make([]fdbv1beta2.ProcessAddress, 0, len(processGroups))
	visited := make(map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None, len(processGroups))
	for _, process := range status.Cluster.Processes {
		// Ignore any tester processes as those are not managed by the global coordination system.
		if process.ProcessClass == fdbv1beta2.ProcessClassTest {
			continue
		}

		processID, ok := process.Locality[fdbv1beta2.FDBLocalityInstanceIDKey]
		if !ok {
			logger.Info(
				"Ignore process with missing locality field",
				"address",
				process.Address.String(),
			)
			continue
		}

		if _, ok := processGroups[fdbv1beta2.ProcessGroupID(processID)]; ok {
			visited[fdbv1beta2.ProcessGroupID(processID)] = fdbv1beta2.None{}
			addresses = append(addresses, process.Address)
		}
	}

	// If some processes are not part of the machine-readable status print it out.
	if len(visited) != len(processGroups) {
		missing := map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None{}
		for processGroupID := range processGroups {
			if _, ok := visited[processGroupID]; ok {
				continue
			}

			missing[processGroupID] = fdbv1beta2.None{}
		}

		logger.Info(
			"Not all requested process groups are part of the machine-readable status",
			"missingProcessGroups",
			missing,
		)
	}

	return addresses
}

// GetProcessesFromProcessMap returns the slice of processes matching the process group ID.
func GetProcessesFromProcessMap(
	processGroupID fdbv1beta2.ProcessGroupID,
	processesMap map[fdbv1beta2.ProcessGroupID][]fdbv1beta2.FoundationDBStatusProcessInfo,
) []fdbv1beta2.FoundationDBStatusProcessInfo {
	if len(processesMap) == 0 {
		return nil
	}

	processes, ok := processesMap[processGroupID]
	if !ok || len(processes) == 0 {
		// Fallback for multiple storage or log servers, those will contain the process information with the process number as a suffix.
		processes, ok = processesMap[processGroupID+"-1"]
		if !ok || len(processes) == 0 {
			return nil
		}
	}

	return processes
}
