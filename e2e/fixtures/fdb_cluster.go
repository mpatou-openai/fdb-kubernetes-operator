/*
 * fdb_cluster.go
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
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FdbCluster is a fixture that allows tests to manipulate an FDB cluster with some name.
// Depending on how it was instantiated, the cluster may or may not exist, and may or may not
// be part of an HA configuration.
type FdbCluster struct {
	cluster *fdbv1beta2.FoundationDBBaremetalCluster
	factory *Factory
}

// ExecuteCmdOnPod will run the provided command in a Shell.
func (fdbCluster *FdbCluster) ExecuteCmdOnPod(
	pod corev1.Pod,
	container string,
	command string,
	printOutput bool,
) (string, string, error) {
	return fdbCluster.factory.ExecuteCmd(
		context.Background(),
		pod.Namespace,
		pod.Name,
		container,
		command,
		printOutput,
	)
}

func (factory *Factory) createFdbClusterObject(
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
) *FdbCluster {
	return &FdbCluster{
		cluster,
		factory,
	}
}

// Name returns the name for the FoundationDBBaremetalCluster.
func (fdbCluster *FdbCluster) Name() string {
	return fdbCluster.cluster.Name
}

func (fdbCluster *FdbCluster) getClient() client.Client {
	return fdbCluster.factory.GetControllerRuntimeClient()
}

// Namespace returns the namespace for the FoundationDBBaremetalCluster.
func (fdbCluster *FdbCluster) Namespace() string {
	return fdbCluster.cluster.Namespace
}

// WaitUntilExists synchronously waits until the cluster exists.  Usually called after Create().
func (fdbCluster *FdbCluster) WaitUntilExists() {
	clusterRequest := fdbv1beta2.FoundationDBBaremetalCluster{}
	key := client.ObjectKeyFromObject(fdbCluster.cluster)

	gomega.Eventually(func() error {
		return fdbCluster.getClient().
			Get(context.Background(), key, &clusterRequest)
	}).WithTimeout(2 * time.Minute).ShouldNot(gomega.HaveOccurred())
}

// Create asynchronously creates this FDB cluster.
func (fdbCluster *FdbCluster) Create() error {
	return fdbCluster.getClient().Create(context.Background(), fdbCluster.cluster)
}

// Update asynchronously updates this FDB cluster definition.
func (fdbCluster *FdbCluster) Update() error {
	return fdbCluster.getClient().Update(context.Background(), fdbCluster.cluster)
}

// ReconciliationOptions defines the different reconciliation options.
type ReconciliationOptions struct {
	allowSoftReconciliation bool
	creationTrackerLogger   CreationTrackerLogger
	minimumGeneration       int64
	timeOutInSeconds        int
	pollTimeInSeconds       int
}

// ReconciliationOption defines the reconciliation option.
type ReconciliationOption func(*ReconciliationOptions)

// SoftReconcileOption specifies that the reconciliation is completed as soon as the Status.Generations.Reconciled reaches the
// expected generation. Independent of other possible generations, e.g. it could be still the case that the operator has to
// delete additional Process Groups.
func SoftReconcileOption(enable bool) ReconciliationOption {
	return func(options *ReconciliationOptions) {
		options.allowSoftReconciliation = enable
	}
}

// CreationTrackerLoggerOption sets the creation tracker that will printout the time for the different creation stages.
func CreationTrackerLoggerOption(creationTrackerLogger CreationTrackerLogger) ReconciliationOption {
	return func(options *ReconciliationOptions) {
		options.creationTrackerLogger = creationTrackerLogger
	}
}

// MinimumGenerationOption specifies the minimum generation to be reconciled too.
func MinimumGenerationOption(minimumGeneration int64) ReconciliationOption {
	return func(options *ReconciliationOptions) {
		options.minimumGeneration = minimumGeneration
	}
}

// TimeOutInSecondsOption defines the timeout for the reconciliation. If not set the default is 4800 seconds
func TimeOutInSecondsOption(timeOutInSeconds int) ReconciliationOption {
	return func(options *ReconciliationOptions) {
		options.timeOutInSeconds = timeOutInSeconds
	}
}

// PollTimeInSecondsOption defines the polling time for the reconciliation. If not set the default is 10 seconds
func PollTimeInSecondsOption(pollTimeInSeconds int) ReconciliationOption {
	return func(options *ReconciliationOptions) {
		options.pollTimeInSeconds = pollTimeInSeconds
	}
}

// MakeReconciliationOptionsStruct applies the provided options to the ReconciliationOptions.
func MakeReconciliationOptionsStruct(
	options ...func(*ReconciliationOptions),
) *ReconciliationOptions {
	reconciliationOptions := &ReconciliationOptions{}

	for _, option := range options {
		option(reconciliationOptions)
	}

	if reconciliationOptions.timeOutInSeconds == 0 {
		// Wait for 30 minutes as timeout.
		reconciliationOptions.timeOutInSeconds = 1800
	}

	if reconciliationOptions.pollTimeInSeconds == 0 {
		reconciliationOptions.pollTimeInSeconds = 10
	}

	return reconciliationOptions
}

// WaitForReconciliation waits for the cluster to be reconciled based on the provided options.
func (fdbCluster *FdbCluster) WaitForReconciliation(options ...func(*ReconciliationOptions)) error {
	reconciliationOptions := MakeReconciliationOptionsStruct(options...)

	return fdbCluster.waitForReconciliationToGeneration(
		reconciliationOptions.minimumGeneration,
		reconciliationOptions.allowSoftReconciliation,
		reconciliationOptions.creationTrackerLogger,
		reconciliationOptions.timeOutInSeconds,
		reconciliationOptions.pollTimeInSeconds,
	)
}

// waitForReconciliationToGeneration waits for a specific generation to be reached.
func (fdbCluster *FdbCluster) waitForReconciliationToGeneration(
	minimumGeneration int64,
	softReconciliationAllowed bool,
	creationTrackerLogger CreationTrackerLogger,
	timeOutInSeconds int,
	pollTimeInSeconds int,
) error {
	if timeOutInSeconds < pollTimeInSeconds {
		return fmt.Errorf(
			"timeout %d is less than poll time %d",
			timeOutInSeconds,
			pollTimeInSeconds,
		)
	}

	log.Printf(
		"waiting until the cluster %s/%s is healthy and reconciled",
		fdbCluster.cluster.Namespace,
		fdbCluster.cluster.Name,
	)

	if minimumGeneration > 0 {
		log.Printf(
			"waiting for generation %d, current generation: %d",
			minimumGeneration,
			fdbCluster.cluster.Generation,
		)
	}

	var creationTracker *fdbClusterCreationTracker
	if creationTrackerLogger != nil {
		creationTracker = newFdbClusterCreationTracker(
			fdbCluster.getClient(),
			creationTrackerLogger,
		)
	}

	checkIfReconciliationIsDone := func(cluster *fdbv1beta2.FoundationDBBaremetalCluster) bool {
		if creationTracker != nil {
			creationTracker.trackProgress(cluster)
		}

		var reconciled bool
		if softReconciliationAllowed {
			reconciled = cluster.Status.Generations.Reconciled == cluster.ObjectMeta.Generation
		} else {
			reconciled = cluster.Status.Generations == fdbv1beta2.ClusterGenerationStatus{Reconciled: cluster.ObjectMeta.Generation}
		}

		if minimumGeneration > 0 {
			reconciled = reconciled &&
				cluster.Status.Generations.Reconciled >= minimumGeneration
		}

		if reconciled {
			log.Printf(
				"reconciled name=%s, namespace=%s, generation:%d",
				fdbCluster.cluster.Name,
				fdbCluster.cluster.Namespace,
				fdbCluster.cluster.Generation,
			)
			return true
		}

		return false
	}

	err := fdbCluster.WaitUntilWithForceReconcile(
		pollTimeInSeconds,
		timeOutInSeconds,
		checkIfReconciliationIsDone,
	)
	if creationTracker != nil {
		creationTracker.report()
	}

	return err
}

// WaitUntilWithForceReconcile will wait either until the checkMethod returns true or until the timeout is hit.
func (fdbCluster *FdbCluster) WaitUntilWithForceReconcile(
	pollTimeInSeconds int,
	timeOutInSeconds int,
	checkMethod func(cluster *fdbv1beta2.FoundationDBBaremetalCluster) bool,
) error {
	// Printout the initial state of the cluster before we're moving forward waiting for the checkMethod to return true.
	fdbCluster.factory.DumpState(fdbCluster)

	lastForcedReconciliationTime := time.Now()
	forceReconcileDuration := 4 * time.Minute

	// TODO (johscheuer): Convert this into a gomega statement.
	return wait.PollUntilContextTimeout(context.Background(),
		time.Duration(pollTimeInSeconds)*time.Second,
		time.Duration(timeOutInSeconds)*time.Second,
		true,
		func(_ context.Context) (bool, error) {
			resCluster := fdbCluster.GetCluster()

			if checkMethod(resCluster) {
				return true, nil
			}

			// Force a reconcile if needed.
			if time.Since(lastForcedReconciliationTime) >= forceReconcileDuration {
				fdbCluster.ForceReconcile()
				lastForcedReconciliationTime = time.Now()
			}

			return false, nil
		},
	)
}

// ForceReconcile will add an annotation with the current timestamp on the FoundationDBBaremetalCluster resource to make sure
// the operator reconciliation loop is triggered. This is used to speed up some test cases.
func (fdbCluster *FdbCluster) ForceReconcile() {
	log.Printf("ForceReconcile: Status Generations=%s, Metadata Generation=%d",
		ToJSON(fdbCluster.cluster.Status.Generations),
		fdbCluster.cluster.ObjectMeta.Generation)

	fdbCluster.factory.DumpState(fdbCluster)
	patch := client.MergeFrom(fdbCluster.cluster.DeepCopy())
	if fdbCluster.cluster.Annotations == nil {
		fdbCluster.cluster.Annotations = make(map[string]string)
	}
	fdbCluster.cluster.Annotations["foundationdb.org/reconcile"] = strconv.FormatInt(
		time.Now().UnixNano(),
		10,
	)

	// This will apply an Annotation to the object which will trigger the reconcile loop.
	// This should speed up the reconcile phase.
	err := fdbCluster.getClient().Patch(
		context.Background(),
		fdbCluster.cluster,
		patch)
	if err != nil {
		log.Println("error patching annotation to force reconcile, error:", err.Error())
	}
}

// GetCluster returns the FoundationDBBaremetalCluster of the cluster. This will fetch the latest value from  the Kubernetes API.
func (fdbCluster *FdbCluster) GetCluster() *fdbv1beta2.FoundationDBBaremetalCluster {
	var cluster *fdbv1beta2.FoundationDBBaremetalCluster

	gomega.Eventually(func() error {
		var err error
		cluster, err = fdbCluster.factory.getClusterStatus(
			fdbCluster.Name(),
			fdbCluster.Namespace(),
		)

		if err != nil {
			log.Println(
				"error fetching information for FoundationDBBaremetalCluster",
				fdbCluster.Name(),
				"in",
				fdbCluster.Namespace(),
				"got error:",
				err.Error(),
			)
		}

		return err
	}).WithTimeout(2 * time.Minute).WithPolling(1 * time.Second).ShouldNot(gomega.HaveOccurred())

	// Update the cached cluster
	fdbCluster.cluster = cluster
	return cluster
}

// GetCachedCluster returns the current cluster definition stored in the fdbCluster struct. This could be outdated and
// if you need the most recent version of the definition you should use `GetCluster`. This method is useful if you want
// to inspect fields that are not changing.
func (fdbCluster *FdbCluster) GetCachedCluster() *fdbv1beta2.FoundationDBBaremetalCluster {
	return fdbCluster.cluster
}

// SetDatabaseConfiguration sets the provided DatabaseConfiguration for the FoundationDBBaremetalCluster.
func (fdbCluster *FdbCluster) SetDatabaseConfiguration(
	config fdbv1beta2.DatabaseConfiguration,
	waitForReconcile bool,
) error {
	fdbCluster.cluster.Spec.DatabaseConfiguration = config
	fdbCluster.UpdateClusterSpec()

	if !waitForReconcile {
		return nil
	}

	return fdbCluster.WaitForReconciliation()
}

// UpdateClusterStatus updates the FoundationDBBaremetalCluster status. This method allows to modify the status sub-resource of
// the FoundationDBBaremetalCluster resource.
func (fdbCluster *FdbCluster) UpdateClusterStatus() {
	fdbCluster.UpdateClusterStatusWithStatus(fdbCluster.cluster.Status.DeepCopy())
}

// UpdateClusterStatusWithStatus ensures that the FoundationDBBaremetalCluster status will be updated in Kubernetes. This method has a retry mechanism
// implemented and ensures that the provided (local) Status matches the status in Kubernetes. You must make sure that you call
// fdbCluster.GetCluster() before updating the status, to make sure you are not overwriting the current state with an outdated state.
// An example on how to update a field with this method:
//
//		// Make sure the operator doesn't modify the status.
//		fdbCluster.SetSkipReconciliation(true)
//		status := fdbCluster.GetCluster().Status.DeepCopy() // Fetch the current status.
//	    // Create a new process group.
//		processGroupID = cluster.GetNextRandomProcessGroupID(fdbv1beta2.ProcessClassStateless, processGroupIDs[fdbv1beta2.ProcessClassStateless])
//		status.ProcessGroups = append(status.ProcessGroups, fdbv1beta2.NewProcessGroupStatus(processGroupID, fdbv1beta2.ProcessClassStateless, nil))
//		fdbCluster.UpdateClusterStatusWithStatus(status)
//
//		// Make sure the operator picks up the work again
//		fdbCluster.SetSkipReconciliation(false)
func (fdbCluster *FdbCluster) UpdateClusterStatusWithStatus(
	desiredStatus *fdbv1beta2.FoundationDBBaremetalClusterStatus,
) {
	fetchedCluster := &fdbv1beta2.FoundationDBBaremetalCluster{}

	// This is flaky. It sometimes responds with an error saying that the object has been updated.
	// Try a few times before giving up.
	gomega.Eventually(func(g gomega.Gomega) bool {
		err := fdbCluster.getClient().
			Get(context.Background(), client.ObjectKeyFromObject(fdbCluster.cluster), fetchedCluster)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "error fetching cluster")

		updated := equality.Semantic.DeepEqual(fetchedCluster.Status, *desiredStatus)
		log.Println("UpdateClusterStatus: updated:", updated)
		if updated {
			return true
		}

		desiredStatus.DeepCopyInto(&fetchedCluster.Status)
		err = fdbCluster.getClient().Status().Update(context.Background(), fetchedCluster)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "error updating cluster status")
		// Retry here and let the method fetch the latest version of the cluster again until the spec is updated.
		return false
	}).WithTimeout(10 * time.Minute).WithPolling(1 * time.Second).Should(gomega.BeTrue())

	fdbCluster.cluster = fetchedCluster
}

// UpdateClusterSpec ensures that the FoundationDBBaremetalCluster will be updated in Kubernetes. This method has a retry mechanism
// implemented and ensures that the provided (local) Spec matches the Spec in Kubernetes.
func (fdbCluster *FdbCluster) UpdateClusterSpec() {
	fdbCluster.UpdateClusterSpecWithSpec(fdbCluster.cluster.Spec.DeepCopy())
}

// UpdateClusterSpecWithSpec ensures that the FoundationDBBaremetalCluster will be updated in Kubernetes. This method has a retry mechanism
// implemented and ensures that the provided (local) Spec matches the Spec in Kubernetes. You must make sure that you call
// fdbCluster.GetCluster() before updating the spec, to make sure you are not overwriting the current state with an outdated state.
// An example on how to update a field with this method:
//
//	spec := fdbCluster.GetCluster().Spec.DeepCopy() // Fetch the current Spec.
//	spec.Version = "7.1.27" // Make your changes.
//
//	fdbCluster.UpdateClusterSpecWithSpec(spec) // Update the spec.
func (fdbCluster *FdbCluster) UpdateClusterSpecWithSpec(
	desiredSpec *fdbv1beta2.FoundationDBBaremetalClusterSpec,
) {
	fetchedCluster := &fdbv1beta2.FoundationDBBaremetalCluster{}

	// This is flaky. It sometimes responds with an error saying that the object has been updated.
	// Try a few times before giving up.
	gomega.Eventually(func(g gomega.Gomega) bool {
		err := fdbCluster.getClient().
			Get(context.Background(), client.ObjectKeyFromObject(fdbCluster.cluster), fetchedCluster)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "error fetching cluster")

		specUpdated := equality.Semantic.DeepEqual(fetchedCluster.Spec, *desiredSpec)
		log.Println("UpdateClusterSpec: specUpdated:", specUpdated)
		if specUpdated {
			return true
		}

		desiredSpec.DeepCopyInto(&fetchedCluster.Spec)
		err = fdbCluster.getClient().Update(context.Background(), fetchedCluster)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "error updating cluster spec")
		// Retry here and let the method fetch the latest version of the cluster again until the spec is updated.
		return false
	}).WithTimeout(10 * time.Minute).WithPolling(1 * time.Second).Should(gomega.BeTrue())

	fdbCluster.cluster = fetchedCluster
}

// UpdateConnectionString will update the connection string in the ConfigMap and the SeedConnectionString of the custer.
func (fdbCluster *FdbCluster) UpdateConnectionString(connectionString string) {
	fdbCluster.UpdateClusterSpec()

	cm := &corev1.ConfigMap{}
	gomega.Expect(fdbCluster.factory.controllerRuntimeClient.Get(context.Background(), client.ObjectKey{Namespace: fdbCluster.Namespace(), Name: fdbCluster.Name() + "-config"}, cm)).
		NotTo(gomega.HaveOccurred())
	gomega.Expect(cm.Data).To(gomega.HaveKey(fdbv1beta2.ClusterFileKey))
	cm.Data[fdbv1beta2.ClusterFileKey] = connectionString
	gomega.Expect(fdbCluster.factory.controllerRuntimeClient.Update(context.Background(), cm)).
		NotTo(gomega.HaveOccurred())
}

// GetClusterVersion returns the cluster's version
func (fdbCluster *FdbCluster) GetClusterVersion() uint64 {
	stdout, _, err := fdbCluster.RunFdbCliCommandInOperatorWithoutRetry("getversion", false, 30)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	version, err := strconv.ParseUint(strings.TrimSpace(stdout), 10, 64)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return version
}

// ClearRange will delete the provided range.
func (fdbCluster *FdbCluster) ClearRange(prefixBytes []byte, timeout int) {
	begin := FdbPrintable(prefixBytes)
	endBytes, err := FdbStrinc(prefixBytes)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	end := FdbPrintable(endBytes)
	_, stderr, err := fdbCluster.RunFdbCliCommandInOperatorWithoutRetry(fmt.Sprintf(
		"writemode on; option on ACCESS_SYSTEM_KEYS; clearrange %s %s",
		begin,
		end,
	), true, timeout)

	gomega.Expect(err).NotTo(gomega.HaveOccurred(), stderr)
}

// KeyValue represents a key and value that can be stored in FDB.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// GetKey returns all the printable characters of the key.
func (keyValue *KeyValue) GetKey() string {
	return FdbPrintable(keyValue.Key)
}

// GetValue returns all printable characters of the value.
func (keyValue *KeyValue) GetValue() string {
	return FdbPrintable(keyValue.Value)
}

// GetRange will return the values of the provided range.
func (fdbCluster *FdbCluster) GetRange(
	prefixBytes []byte,
	limit int,
	timeout int,
) (keyValues []KeyValue) {
	endBytes, err := FdbStrinc(prefixBytes)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	stdout, _, err := fdbCluster.RunFdbCliCommandInOperatorWithoutRetry(fmt.Sprintf(
		"option on ACCESS_SYSTEM_KEYS; getrange %s %s %d",
		FdbPrintable(prefixBytes),
		FdbPrintable(endBytes),
		limit,
	), false, timeout)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	for _, line := range strings.Split(strings.TrimSuffix(stdout, "\n"), "\n") {
		line = strings.TrimSpace(line)
		sep := "' is `"
		idx := strings.Index(line, sep)
		if idx != -1 {
			key, parseErr := Unprintable(line[1:idx]) // Remove the first "`"
			gomega.Expect(parseErr).NotTo(gomega.HaveOccurred())
			value, parseErr := Unprintable(line[idx+len(sep) : len(line)-1]) // Remove the last "'"
			gomega.Expect(parseErr).NotTo(gomega.HaveOccurred())
			keyValues = append(keyValues, KeyValue{
				Key:   key,
				Value: value,
			})
		}
	}

	return keyValues
}

// GenerateRandomValues will generate n random values with the provided prefix.
func (fdbCluster *FdbCluster) GenerateRandomValues(
	n int,
	prefix byte,
) []KeyValue {
	res := make([]KeyValue, 0, n)
	index := []byte{'a'}
	var err error
	for i := 0; i < n; i++ {
		res = append(res, KeyValue{
			Key:   append([]byte{prefix}, index...),
			Value: []byte(fdbCluster.factory.RandStringRunes(24)),
		})
		index, err = FdbStrinc(index)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}

	return res
}

// WriteKeyValue writes a single key value pair into FDB.
func (fdbCluster *FdbCluster) WriteKeyValue(
	keyValue KeyValue,
	timeout int,
) {
	_, stderr, err := fdbCluster.RunFdbCliCommandInOperatorWithoutRetry(
		fmt.Sprintf("writemode on; set %s %s", keyValue.GetKey(), keyValue.GetValue()),
		false,
		timeout,
	)

	gomega.Expect(err).NotTo(gomega.HaveOccurred(), stderr)
}

// WriteKeyValuesWithTimeout writes multiples key values into FDB with the specified timeout.
func (fdbCluster *FdbCluster) WriteKeyValuesWithTimeout(keyValues []KeyValue, timeout int) {
	for _, kv := range keyValues {
		fdbCluster.WriteKeyValue(kv, timeout)
	}
}

// WriteKeyValues writes multiples key values into FDB.
func (fdbCluster *FdbCluster) WriteKeyValues(keyValues []KeyValue) {
	fdbCluster.WriteKeyValuesWithTimeout(keyValues, 30)
}
