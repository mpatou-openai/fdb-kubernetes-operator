/*
 * fdb_cluster_creation_tracker.go
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
	"log"
	"strconv"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CreationTrackerLogger is an interface that can be used to log the time between different creation steps.
type CreationTrackerLogger interface {
	// NewEntry adds an entry to the internal map.
	NewEntry() map[string]interface{}
	// Log will write the values in  the map directly to the logger.
	Log(values map[string]interface{}) error
	// Flush will write all values from the entry map to the logger.
	Flush() error
}

// NewDefaultCreationTrackerLogger creates a no-op logger which is not writing anything.
func NewDefaultCreationTrackerLogger() CreationTrackerLogger {
	return &DefaultCreationTrackerLogger{}
}

// DefaultCreationTrackerLogger is an implementation of CreationTrackerLogger that is not printing any values.
type DefaultCreationTrackerLogger struct{}

// NewEntry adds an entry to the internal map.
func (logger *DefaultCreationTrackerLogger) NewEntry() map[string]interface{} {
	return map[string]interface{}{}
}

// Log will write the values in  the map directly to the logger.
func (logger *DefaultCreationTrackerLogger) Log(_ map[string]interface{}) error {
	return nil
}

// Flush will write all values from the entry map to the logger.
func (logger *DefaultCreationTrackerLogger) Flush() error {
	return nil
}

// This is a helper type to distinguish different creation steps.
type creationStep int

// Those are the different steps during a FDB cluster creation with the operator. If needed we can add additional steps.
const (
	initialCreationStep creationStep = iota
	creationStepProcessGroupsCreated
	creationStepPodsCreated
	creationStepPodsRunning
	creationStepCoordinatorSelected
	creationStepConfigMapSynced
	creationStepDatabaseConfigured
	creationStepFullyReconciled
)

func (step creationStep) String() string {
	switch step {
	case initialCreationStep:
		return "InitialCreation"
	case creationStepProcessGroupsCreated:
		return "ProcessGroupCreation"
	case creationStepPodsCreated:
		return "PodCreation"
	case creationStepPodsRunning:
		return "PodsRunning"
	case creationStepConfigMapSynced:
		return "ConfigMapSync"
	case creationStepCoordinatorSelected:
		return "CoordinatorSelection"
	case creationStepDatabaseConfigured:
		return "DatabaseConfiguration"
	case creationStepFullyReconciled:
		return "FullyReconcile"
	default:
		return strconv.Itoa(int(step))
	}
}

type fdbClusterCreationTracker struct {
	lastTransitionTime time.Time
	currentStep        creationStep
	durationTracker    map[creationStep]time.Duration
	ctrlClient         client.Client
	logger             CreationTrackerLogger
}

func newFdbClusterCreationTracker(
	ctrlClient client.Client,
	logger CreationTrackerLogger,
) *fdbClusterCreationTracker {
	return &fdbClusterCreationTracker{
		lastTransitionTime: time.Now(),
		currentStep:        initialCreationStep,
		durationTracker:    map[creationStep]time.Duration{},
		ctrlClient:         ctrlClient,
		logger:             logger,
	}
}

func (tracker *fdbClusterCreationTracker) nextStep() {
	duration := time.Since(tracker.lastTransitionTime)
	tracker.currentStep++
	tracker.durationTracker[tracker.currentStep] = duration
	tracker.lastTransitionTime = time.Now()
	log.Println("Finished step", tracker.currentStep.String(), "in", duration)

	// Log the duration in milliseconds
	gomega.Expect(tracker.logger.Log(map[string]interface{}{
		tracker.currentStep.String(): duration.Milliseconds(),
	})).NotTo(gomega.HaveOccurred())
}

// checkStep will check if the current step is full filled and if the state machine should move to the next step.
func (tracker *fdbClusterCreationTracker) checkStep(
	step creationStep,
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
) {
	switch step {
	case initialCreationStep:
		// Initial step, always move to the next step
	case creationStepConfigMapSynced:
		if !cluster.Status.Configured {
			// Wait until the database was configured
			return
		}
	case creationStepDatabaseConfigured:
		reconciled := cluster.Status.Generations == fdbv1beta2.ClusterGenerationStatus{
			Reconciled: cluster.ObjectMeta.Generation,
		}

		if !reconciled {
			// Wait until the cluster is "fully" reconciled
			return
		}
	case creationStepFullyReconciled:
		return
	}

	tracker.nextStep()
}

// trackProgress will check the current step until the state machine cannot make progress.
func (tracker *fdbClusterCreationTracker) trackProgress(cluster *fdbv1beta2.FoundationDBBaremetalCluster) {
	curStep := -1

	// Check as many steps as possible.
	for curStep != int(tracker.currentStep) {
		curStep = int(tracker.currentStep)
		tracker.checkStep(tracker.currentStep, cluster)
	}
}

// report will write the result (duration for each step) to stdout and to the tracker.
func (tracker *fdbClusterCreationTracker) report() {
	for step := 1; step <= int(creationStepFullyReconciled); step++ {
		createStep := creationStep(step)
		duration, ok := tracker.durationTracker[createStep]
		if !ok {
			log.Println("Missing duration for step", createStep.String())
			continue
		}

		log.Println("step", createStep.String(), "took", duration.String())
	}

	gomega.Expect(tracker.logger.Flush()).NotTo(gomega.HaveOccurred())
}
