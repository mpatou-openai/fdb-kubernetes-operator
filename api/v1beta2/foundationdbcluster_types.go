/*
Copyright 2020-2022 FoundationDB project authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta2

import (
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=bmfdb
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations="foundationdb.org/release=v2.15.0"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".status.runningVersion",description="Running version",priority=0
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:storageversion

// FoundationDBCluster is the Schema for the foundationdbclusters API
type FoundationDBCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FoundationDBClusterSpec   `json:"spec,omitempty"`
	Status FoundationDBClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// FoundationDBClusterList contains a list of FoundationDBCluster objects
type FoundationDBClusterList struct {
	metav1.TypeMeta `                      json:",inline"`
	metav1.ListMeta `                      json:"metadata,omitempty"`
	Items           []FoundationDBCluster `json:"items"`
}

// TODO (johscheuer): I think it would make sense to expose this as a setting in the FoundationDBCluster automation options
// that way users can define what conditions should be used for the replacement logic.
var defaultConditionsThatNeedReplacement = map[ProcessGroupConditionType]None{
	MissingProcesses:          {},
	PodFailing:                {},
	MissingPod:                {},
	MissingPVC:                {},
	MissingService:            {},
	PodPending:                {},
	NodeTaintReplacing:        {},
	ProcessIsMarkedAsExcluded: {},
	ProcessHasIOError:         {},
	SidecarUnreachable:        {},
	ProcessHasHighRunLoopBusy: {},
}

const (
	oneHourDuration = 1 * time.Hour
)

var (
	// MaxProcessGroupIDNum is the upper limit for the process group ID numbers. The picked value should be good enough
	// for the current setup but can be increased in the future.
	MaxProcessGroupIDNum = 99999
)

func init() {
	SchemeBuilder.Register(&FoundationDBCluster{}, &FoundationDBClusterList{})
}

// FoundationDBClusterSpec defines the desired state of a cluster.
type FoundationDBClusterSpec struct {
	// Version defines the version of FoundationDB the cluster should run.
	// +kubebuilder:validation:Pattern:=(\d+)\.(\d+)\.(\d+)
	Version string `json:"version"`

	// DatabaseConfiguration defines the database configuration.
	DatabaseConfiguration DatabaseConfiguration `json:"databaseConfiguration,omitempty"`
}

// ImageType defines a single kind of images used in the cluster.
// +kubebuilder:validation:MaxLength=1024
type ImageType string

const (
	// ImageTypeSplit defines the split image type.
	// Deprecated: New features will only be implemented for the unified.
	ImageTypeSplit ImageType = "split"
	// ImageTypeUnified defines the unified image type.
	ImageTypeUnified ImageType = "unified"
)

// FoundationDBClusterStatus defines the observed state of FoundationDBCluster
type FoundationDBClusterStatus struct {
	// DatabaseConfiguration provides the running configuration of the database.
	DatabaseConfiguration DatabaseConfiguration `json:"databaseConfiguration,omitempty"`

	// Generations provides information about the latest generation to be
	// reconciled, or to reach other stages at which reconciliation can halt.
	Generations ClusterGenerationStatus `json:"generations,omitempty"`

	// Health provides information about the health of the database.
	Health ClusterHealth `json:"health,omitempty"`

	// HasIncorrectConfigMap indicates whether the latest config map is out
	// of date with the cluster spec.
	HasIncorrectConfigMap bool `json:"hasIncorrectConfigMap,omitempty"`

	// NeedsNewCoordinators indicates whether the cluster needs to recruit
	// new coordinators to fulfill its fault tolerance requirements.
	NeedsNewCoordinators bool `json:"needsNewCoordinators,omitempty"`

	// RunningVersion defines the version of FoundationDB that the cluster is
	// currently running.
	RunningVersion string `json:"runningVersion,omitempty"`

	// ConnectionString defines the contents of the cluster file.
	ConnectionString string `json:"connectionString,omitempty"`

	// Configured defines whether we have configured the database yet.
	Configured bool `json:"configured,omitempty"`

	// MaintenenanceModeInfo contains information regarding process groups in maintenance mode
	// Deprecated: This setting is not used anymore.
	MaintenanceModeInfo MaintenanceModeInfo `json:"maintenanceModeInfo,omitempty"`
}

// MaintenanceModeInfo contains information regarding the zone and process groups that are put
// into maintenance mode by the operator
type MaintenanceModeInfo struct {
	// StartTimestamp provides the timestamp when this zone is put into maintenance mode
	// Deprecated: This setting is not used anymore.
	StartTimestamp *metav1.Time `json:"startTimestamp,omitempty"`
	// ZoneID that is placed in maintenance mode
	// Deprecated: This setting is not used anymore.
	ZoneID FaultDomain `json:"zoneID,omitempty"`
	// ProcessGroups that are placed in maintenance mode
	// +kubebuilder:validation:MaxItems=200
	// Deprecated: This setting is not used anymore.
	ProcessGroups []string `json:"processGroups,omitempty"`
}

// LockSystemStatus provides a summary of the status of the locking system.
type LockSystemStatus struct {
	// DenyList contains a list of operator instances that are prevented
	// from taking locks.
	DenyList []string `json:"lockDenyList,omitempty"`
}

// ProcessGroupStatus represents the status of a ProcessGroup.
type ProcessGroupStatus struct {
	// ProcessGroupID represents the ID of the process group
	ProcessGroupID ProcessGroupID `json:"processGroupID,omitempty"`
	// ProcessClass represents the class the process group has.
	ProcessClass ProcessClass `json:"processClass,omitempty"`
	// Addresses represents the list of addresses the process group has been known to have.
	Addresses []string `json:"addresses,omitempty"`
	// RemoveTimestamp if not empty defines when the process group was marked for removal.
	RemovalTimestamp *metav1.Time `json:"removalTimestamp,omitempty"`
	// ExclusionTimestamp defines when the process group has been fully excluded.
	// This is only used within the reconciliation process, and should not be considered authoritative.
	ExclusionTimestamp *metav1.Time `json:"exclusionTimestamp,omitempty"`
	// ExclusionSkipped determines if exclusion has been skipped for a process, which will allow the process group to be removed without exclusion.
	ExclusionSkipped bool `json:"exclusionSkipped,omitempty"`
	// ProcessGroupConditions represents a list of degraded conditions that the process group is in.
	ProcessGroupConditions []*ProcessGroupCondition `json:"processGroupConditions,omitempty"`
	// FaultDomain represents the last seen fault domain from the cluster status. This can be used if a Pod or process
	// is not running and would be missing in the cluster status.
	FaultDomain FaultDomain `json:"faultDomain,omitempty"`
}

// String returns string representation.
func (processGroupStatus *ProcessGroupStatus) String() string {
	var sb strings.Builder

	sb.WriteString("ID: ")
	sb.WriteString(string(processGroupStatus.ProcessGroupID))
	sb.WriteString(", Class: ")
	sb.WriteString(string(processGroupStatus.ProcessClass))
	sb.WriteString(", Addresses: ")
	sb.WriteString(strings.Join(processGroupStatus.Addresses, ","))

	sb.WriteString(", RemovalTimestamp: ")
	if processGroupStatus.RemovalTimestamp.IsZero() {
		sb.WriteString("-")
	} else {
		sb.WriteString(processGroupStatus.RemovalTimestamp.String())
	}

	sb.WriteString(", ExclusionTimestamp: ")
	if processGroupStatus.ExclusionTimestamp.IsZero() {
		sb.WriteString("-")
	} else {
		sb.WriteString(processGroupStatus.ExclusionTimestamp.String())
	}

	sb.WriteString(", ExclusionSkipped: ")
	sb.WriteString(strconv.FormatBool(processGroupStatus.ExclusionSkipped))

	sb.WriteString(", ProcessGroupConditions: ")
	for _, condition := range processGroupStatus.ProcessGroupConditions {
		sb.WriteString(condition.String())
	}

	sb.WriteString(", FaultDomain: ")
	sb.WriteString(string(processGroupStatus.FaultDomain))

	return sb.String()
}

// FaultDomain represents the FaultDomain of a process group
// +kubebuilder:validation:MaxLength=512
type FaultDomain string

// ProcessGroupID represents the ID of the process group
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern:=^(([\w-]+)-(\d+)|\*)$
type ProcessGroupID string

// GetIDNumber returns the ID number of the provided process group ID. This will be the suffix number, e.g. for the
// process group ID "testing-storage-12" this will return 12.
func (processGroupID ProcessGroupID) GetIDNumber() (int, error) {
	tmp := string(processGroupID)
	idx := strings.LastIndex(tmp, "-")
	// The ID number will always be the suffix after the last '-'.
	idNum, err := strconv.Atoi(tmp[idx+1:])
	if err != nil || idx <= 0 {
		return -1, fmt.Errorf("could not parse process group ID %s", tmp)
	}

	return idNum, nil
}

// GetProcessClass returns the process class from the process group ID.
func (processGroupID ProcessGroupID) GetProcessClass() ProcessClass {
	tmp := strings.Split(string(processGroupID), "-")
	if len(tmp) < 2 {
		return ""
	}

	// The process class will always be the second last item, the last item is the ID number,
	// e.g. stateless-1 or with a prefix: prefix-stateless-2.
	return ProcessClass(tmp[len(tmp)-2])
}

// GetExclusionString returns the exclusion string
func (processGroupID ProcessGroupID) GetExclusionString() string {
	return fmt.Sprintf("%s:%s", FDBLocalityExclusionPrefix, processGroupID)
}

// GetExclusionString returns the exclusion string
func (processGroupStatus *ProcessGroupStatus) GetExclusionString() string {
	return processGroupStatus.ProcessGroupID.GetExclusionString()
}

// IsExcluded returns if a process group is excluded
func (processGroupStatus *ProcessGroupStatus) IsExcluded() bool {
	return !processGroupStatus.ExclusionTimestamp.IsZero() || processGroupStatus.ExclusionSkipped
}

// SetExclude marks a process group as excluded and will reset the process group conditions to only include the ResourcesTerminating
// if already set, otherwise the conditions will be an empty slice. This reflects the operator behaviour that process
// groups that are marked for removal and are fully excluded will only have the ResourcesTerminating condition.
func (processGroupStatus *ProcessGroupStatus) SetExclude() {
	if !processGroupStatus.ExclusionTimestamp.IsZero() {
		return
	}

	processGroupStatus.ExclusionTimestamp = &metav1.Time{Time: time.Now()}
	// Reset all previous conditions as the operator will only track the ResourcesTerminating condition for process
	// groups marked as removal. If the ResourcesTerminating condition is already set we are not removing it.
	newConditions := make([]*ProcessGroupCondition, 0, 1)
	for _, condition := range processGroupStatus.ProcessGroupConditions {
		if condition.ProcessGroupConditionType != ResourcesTerminating {
			continue
		}

		newConditions = append(newConditions, condition)
	}

	processGroupStatus.ProcessGroupConditions = newConditions
}

// IsMarkedForRemoval returns if a process group is marked for removal
func (processGroupStatus *ProcessGroupStatus) IsMarkedForRemoval() bool {
	return !processGroupStatus.RemovalTimestamp.IsZero()
}

// MarkForRemoval marks a process group for removal. If the RemovalTimestamp is already set it won't be changed.
func (processGroupStatus *ProcessGroupStatus) MarkForRemoval() {
	if !processGroupStatus.RemovalTimestamp.IsZero() {
		return
	}

	processGroupStatus.RemovalTimestamp = &metav1.Time{Time: time.Now()}
}

// GetPodName returns the Pod name for the associated Process Group.
func (processGroupStatus *ProcessGroupStatus) GetPodName(cluster *FoundationDBCluster) string {
	var sb strings.Builder
	sb.WriteString(cluster.Name)
	sb.WriteString("-")
	// The Pod name will always be in the format ${cluster}-${process-class}-${id}. The ID is currently not available
	// in the processGroupStatus without doing any parsing, so we have to use the Process Group ID, which might contain
	// a prefix, so we take the part after the prefix, which will be ${process-class}-${id}.
	sanitizedProcessGroup := strings.ReplaceAll(string(processGroupStatus.ProcessGroupID), "_", "-")

	idx := strings.Index(
		sanitizedProcessGroup,
		processGroupStatus.ProcessClass.GetProcessClassForPodName(),
	)
	sb.WriteString(sanitizedProcessGroup[idx:])

	return sb.String()
}

// GetPvcName returns the PVC name for the associated Process Group. If the process class doesn't use PVCs, an empty
// string is returned.
func (processGroupStatus *ProcessGroupStatus) GetPvcName(cluster *FoundationDBCluster) string {
	if !processGroupStatus.ProcessClass.IsStateful() {
		return ""
	}

	return fmt.Sprintf("%s-data", processGroupStatus.GetPodName(cluster))
}

// NeedsReplacementWithConditions checks if the ProcessGroupStatus has conditions that require a replacement of the failed Process Group.
// The method will return the failure condition and the timestamp. If no failure is detected an empty condition and a 0
// will be returned. The conditions that should trigger a replacement can be passed to this method.
func (processGroupStatus *ProcessGroupStatus) NeedsReplacementWithConditions(
	failureTime int,
	taintReplacementTime int,
	conditionsThatNeedReplacement map[ProcessGroupConditionType]None,
) (ProcessGroupConditionType, int64) {
	var earliestFailureTime int64 = math.MaxInt64
	var earliestTaintReplacementTime int64 = math.MaxInt64

	// If the process group is already marked for removal we can ignore it.
	if processGroupStatus.IsMarkedForRemoval() {
		return "", 0
	}

	var failureCondition ProcessGroupConditionType

	// Iterate over all the conditions that the process group has, under normal circumstances the process group
	// should have no oder a minimal set of conditions. If any of the condition is part of the conditionsThatNeedReplacement
	// check how long the condition is present and check if the process group should be replaced.
	var hasConditionThatRequiresReplacement bool
	for _, condition := range processGroupStatus.ProcessGroupConditions {
		_, ok := conditionsThatNeedReplacement[condition.ProcessGroupConditionType]
		if !ok {
			continue
		}

		hasConditionThatRequiresReplacement = true
		if condition.ProcessGroupConditionType == NodeTaintReplacing {
			if earliestTaintReplacementTime > condition.Timestamp {
				earliestTaintReplacementTime = condition.Timestamp
			}

			failureCondition = condition.ProcessGroupConditionType
			continue
		}

		if earliestFailureTime > condition.Timestamp {
			earliestFailureTime = condition.Timestamp
			failureCondition = condition.ProcessGroupConditionType
		}
	}

	if !hasConditionThatRequiresReplacement {
		return "", 0
	}

	if earliestFailureTime < time.Now().Add(-1*time.Duration(failureTime)*time.Second).Unix() {
		return failureCondition, earliestFailureTime
	}

	taintWindowStart := time.Now().
		Add(-1 * time.Duration(taintReplacementTime) * time.Second).
		Unix()
	if earliestTaintReplacementTime < taintWindowStart {
		return failureCondition, earliestTaintReplacementTime
	}

	// No failure detected.
	return "", 0
}

// NeedsReplacement checks if the ProcessGroupStatus has conditions that require a replacement of the failed Process Group.
// The method will return the failure condition and the timestamp. If no failure is detected an empty condition and a 0
// will be returned.
// Deprecated: Use NeedsReplacementWithConditions.
func (processGroupStatus *ProcessGroupStatus) NeedsReplacement(
	failureTime int,
	taintReplacementTime int,
) (ProcessGroupConditionType, int64) {
	return processGroupStatus.NeedsReplacementWithConditions(
		failureTime,
		taintReplacementTime,
		defaultConditionsThatNeedReplacement,
	)
}

// AddAddresses adds the new address to the ProcessGroupStatus and removes duplicates and old addresses
// if the process group is not marked as removal.
func (processGroupStatus *ProcessGroupStatus) AddAddresses(
	addresses []string,
	includeOldAddresses bool,
) {
	newAddresses := make([]string, 0, len(addresses))
	// Currently this only contains one address but might include in the future multiple addresses
	// e.g. for dual stack
	for _, addr := range addresses {
		// empty address in the address list that means the Pod has no IP address assigned
		if addr == "" {
			continue
		}

		newAddresses = append(newAddresses, addr)
	}

	// If the newAddresses contains at least one IP address use this list as the new addresses
	// and return
	if len(newAddresses) > 0 && !includeOldAddresses {
		processGroupStatus.Addresses = newAddresses
		return
	}

	if includeOldAddresses {
		processGroupStatus.Addresses = cleanAddressList(
			append(processGroupStatus.Addresses, newAddresses...),
		)
		return
	}
}

// This method removes duplicates and empty strings from a list of addresses.
func cleanAddressList(addresses []string) []string {
	result := make([]string, 0, len(addresses))
	resultMap := make(map[string]bool)

	for _, value := range addresses {
		if value != "" && !resultMap[value] {
			result = append(result, value)
			resultMap[value] = true
		}
	}

	return result
}

// AllAddressesExcluded checks if the process group is excluded or if there are still addresses included in the remainingMap.
// This will return true if the process group skips exclusion or has no remaining addresses.
func (processGroupStatus *ProcessGroupStatus) AllAddressesExcluded(
	logger logr.Logger,
	remainingMap map[string]bool,
) (bool, error) {
	if processGroupStatus.IsExcluded() {
		return true, nil
	}

	localityExclusionString := processGroupStatus.GetExclusionString()
	if isRemaining, isPresent := remainingMap[localityExclusionString]; isPresent {
		if isRemaining {
			return false, fmt.Errorf(
				"process has missing exclusion string in exclusion results: %s",
				localityExclusionString,
			)
		}
		logger.V(1).
			Info("process group is fully excluded based on locality based exclusions", "processGroupID", processGroupStatus.ProcessGroupID, "exclusionString", localityExclusionString)
		return true, nil
	}

	// If the process group has no addresses assigned we cannot remove it safely and we have to set the skip exclusion.
	if len(processGroupStatus.Addresses) == 0 {
		pendingTime := processGroupStatus.GetConditionTime(PodPending)
		// If the process group has the PodPending condition for more than 1 hour we allow to remove this process group.
		if pendingTime != nil && time.Since(time.Unix(*pendingTime, 0)) > oneHourDuration {
			logger.Info(
				"allow removal of process group without address that is stuck in pending over 1 hour",
				"processGroupID",
				processGroupStatus.ProcessGroupID,
			)
			return true, nil
		}

		return false, fmt.Errorf(
			"process has no addresses, cannot safely determine if process can be removed",
		)
	}

	// If a process group has more than one address we have to make sure that all the provided addresses are part
	// of the remainingMap as excluded.
	for _, address := range processGroupStatus.Addresses {
		isRemaining, isPresent := remainingMap[address]
		if !isPresent || isRemaining {
			return false, fmt.Errorf(
				"process has missing address in exclusion results: %s",
				address,
			)
		}
	}

	return true, nil
}

// NewProcessGroupStatus returns a new GroupStatus for the given processGroupID and processClass.
func NewProcessGroupStatus(
	processGroupID ProcessGroupID,
	processClass ProcessClass,
	addresses []string,
) *ProcessGroupStatus {
	initialConditions := []*ProcessGroupCondition{
		NewProcessGroupCondition(MissingProcesses),
		NewProcessGroupCondition(MissingPod),
	}

	if processClass.IsStateful() {
		initialConditions = append(initialConditions, NewProcessGroupCondition(MissingPVC))
	}

	return &ProcessGroupStatus{
		ProcessGroupID:         processGroupID,
		ProcessClass:           processClass,
		Addresses:              addresses,
		ProcessGroupConditions: initialConditions,
	}
}

// FindProcessGroupByID finds a process group status for a given processGroupID.
func FindProcessGroupByID(
	processGroups []*ProcessGroupStatus,
	processGroupID ProcessGroupID,
) *ProcessGroupStatus {
	for _, processGroup := range processGroups {
		if processGroup.ProcessGroupID == processGroupID {
			return processGroup
		}
	}

	return nil
}

// ContainsProcessGroupID evaluates if the ProcessGroupStatus contains a given processGroupID.
func ContainsProcessGroupID(
	processGroups []*ProcessGroupStatus,
	processGroupID ProcessGroupID,
) bool {
	return FindProcessGroupByID(processGroups, processGroupID) != nil
}

// MarkProcessGroupForRemoval sets the remove flag for the given process and ensures that the address is added.
func MarkProcessGroupForRemoval(
	processGroups []*ProcessGroupStatus,
	processGroupID ProcessGroupID,
	processClass ProcessClass,
	address string,
) (bool, *ProcessGroupStatus) {
	for _, processGroup := range processGroups {
		if processGroup.ProcessGroupID != processGroupID {
			continue
		}

		hasAddress := false
		for _, addr := range processGroup.Addresses {
			if addr != address {
				continue
			}

			hasAddress = true
			break
		}

		if !hasAddress && address != "" {
			processGroup.Addresses = append(processGroup.Addresses, address)
		}

		processGroup.MarkForRemoval()
		return true, nil
	}

	var addresses []string
	if address == "" {
		addresses = nil
	} else {
		addresses = []string{address}
	}

	processGroup := NewProcessGroupStatus(processGroupID, processClass, addresses)
	processGroup.MarkForRemoval()

	return false, processGroup
}

// UpdateCondition will add or remove a condition in the ProcessGroupStatus.
func (processGroupStatus *ProcessGroupStatus) UpdateCondition(
	conditionType ProcessGroupConditionType,
	set bool,
) {
	if set {
		processGroupStatus.addCondition(conditionType)
		return
	}

	processGroupStatus.removeCondition(conditionType)
}

// UpdateConditionTime will update the conditionType's condition time to newTime
// If the conditionType does not exist, the function is no-op
func (processGroupStatus *ProcessGroupStatus) UpdateConditionTime(
	conditionType ProcessGroupConditionType,
	newTime int64,
) {
	for i, condition := range processGroupStatus.ProcessGroupConditions {
		if condition.ProcessGroupConditionType == conditionType {
			processGroupStatus.ProcessGroupConditions[i].Timestamp = newTime
			break
		}
	}
}

// addCondition will add the condition to the ProcessGroupStatus. If the condition is already present this method will not
// change the timestamp. If a process group is marked for removal and exclusion only the ResourcesTerminating can be added
// and all other conditions will be reset.
func (processGroupStatus *ProcessGroupStatus) addCondition(
	conditionType ProcessGroupConditionType,
) {
	// Check if we already got this condition in the current ProcessGroupStatus.
	for _, condition := range processGroupStatus.ProcessGroupConditions {
		if condition.ProcessGroupConditionType == conditionType {
			return
		}
	}

	// If a process group is marked for removal and is fully excluded we only keep the ResourcesTerminating condition.
	if processGroupStatus.IsMarkedForRemoval() && processGroupStatus.IsExcluded() {
		if conditionType != ResourcesTerminating {
			return
		}
	}

	// We didn't find any condition so we create a new one
	processGroupStatus.ProcessGroupConditions = append(
		processGroupStatus.ProcessGroupConditions,
		NewProcessGroupCondition(conditionType),
	)
}

// removeCondition will remove a condition from the ProcessGroupStatus, if it is
// present.
func (processGroupStatus *ProcessGroupStatus) removeCondition(
	conditionType ProcessGroupConditionType,
) {
	conditions := make([]*ProcessGroupCondition, 0, len(processGroupStatus.ProcessGroupConditions))
	for _, condition := range processGroupStatus.ProcessGroupConditions {
		if condition.ProcessGroupConditionType != conditionType {
			conditions = append(conditions, condition)
		}
	}
	processGroupStatus.ProcessGroupConditions = conditions
}

// CreateProcessCountsFromProcessGroupStatus creates a ProcessCounts struct from the current ProcessGroupStatus.
func CreateProcessCountsFromProcessGroupStatus(
	processGroupStatus []*ProcessGroupStatus,
	includeRemovals bool,
) ProcessCounts {
	processCounts := ProcessCounts{}

	for _, groupStatus := range processGroupStatus {
		if !groupStatus.IsMarkedForRemoval() || includeRemovals {
			processCounts.IncreaseCount(groupStatus.ProcessClass, 1)
		}
	}

	return processCounts
}

// FilterByCondition returns a string slice of all ProcessGroupIDs that contains a condition with the given type.
func FilterByCondition(
	processGroupStatus []*ProcessGroupStatus,
	conditionType ProcessGroupConditionType,
	ignoreRemoved bool,
) []ProcessGroupID {
	return FilterByConditions(
		processGroupStatus,
		map[ProcessGroupConditionType]bool{conditionType: true},
		ignoreRemoved,
	)
}

// FilterByConditions returns a string slice of all ProcessGroupIDs whose
// conditions match a set of rules.
//
// If a condition is mapped to true in the conditionRules map, only process
// groups with that condition will be returned. If a condition is mapped to
// false in the conditionRules map, only process groups without that condition
// will be returned.
func FilterByConditions(
	processGroupStatus []*ProcessGroupStatus,
	conditionRules map[ProcessGroupConditionType]bool,
	ignoreRemoved bool,
) []ProcessGroupID {
	result := make([]ProcessGroupID, 0, len(processGroupStatus))

	for _, groupStatus := range processGroupStatus {
		if ignoreRemoved && groupStatus.IsMarkedForRemoval() {
			continue
		}

		if groupStatus.MatchesConditions(conditionRules) {
			result = append(result, groupStatus.ProcessGroupID)
		}
	}

	return result
}

// MatchesConditions checks if the provided conditionRules matches in the process group's conditions
//
// If a condition is mapped to true in the conditionRules map, this condition must be present in the process group.
// If a condition is mapped to false in the conditionRules map, the condition must be absent in the process group.
func (processGroupStatus *ProcessGroupStatus) MatchesConditions(
	conditionRules map[ProcessGroupConditionType]bool,
) bool {
	matchingConditions := make(map[ProcessGroupConditionType]bool, len(conditionRules))

	for conditionRule := range conditionRules {
		matchingConditions[conditionRule] = false
	}

	for _, condition := range processGroupStatus.ProcessGroupConditions {
		if _, hasRule := conditionRules[condition.ProcessGroupConditionType]; hasRule {
			matchingConditions[condition.ProcessGroupConditionType] = true
		}
	}

	return equality.Semantic.DeepEqual(matchingConditions, conditionRules)
}

// GetConditionTime returns the timestamp when we detected a condition on a
// process group.
// If there is no matching condition this will return nil.
func (processGroupStatus *ProcessGroupStatus) GetConditionTime(
	conditionType ProcessGroupConditionType,
) *int64 {
	for _, condition := range processGroupStatus.ProcessGroupConditions {
		if condition.ProcessGroupConditionType == conditionType {
			return &condition.Timestamp
		}
	}

	return nil
}

// IsUnderMaintenance checks if the process is in maintenance zone.
func (processGroupStatus *ProcessGroupStatus) IsUnderMaintenance(maintenanceZone FaultDomain) bool {
	// Only storage processes are affected by the maintenance zone.
	if processGroupStatus.ProcessClass != ProcessClassStorage {
		return false
	}

	// If the maintenanceZone is not set or the Process Group has not fault domain set, return false.
	if maintenanceZone == "" || processGroupStatus.FaultDomain == "" {
		return false
	}

	return processGroupStatus.FaultDomain == maintenanceZone
}

// GetCondition returns the ProcessGroupStatus's ProcessGroupCondition that matches the conditionType;
// It returns nil if the ProcessGroupStatus doesn't have a matching condition
func (processGroupStatus *ProcessGroupStatus) GetCondition(
	conditionType ProcessGroupConditionType,
) *ProcessGroupCondition {
	for _, condition := range processGroupStatus.ProcessGroupConditions {
		if condition.ProcessGroupConditionType == conditionType {
			return condition
		}
	}

	return nil
}

// NewProcessGroupCondition creates a new ProcessGroupCondition of the given time with the current timestamp.
func NewProcessGroupCondition(conditionType ProcessGroupConditionType) *ProcessGroupCondition {
	return &ProcessGroupCondition{
		ProcessGroupConditionType: conditionType,
		Timestamp:                 time.Now().Unix(),
	}
}

// ProcessGroupCondition represents a degraded condition that a process group is in.
type ProcessGroupCondition struct {
	// Name of the condition
	ProcessGroupConditionType ProcessGroupConditionType `json:"type,omitempty"`
	// Timestamp when the Condition was observed
	Timestamp int64 `json:"timestamp,omitempty"`
}

// String returns the string representation for the condition.
func (condition *ProcessGroupCondition) String() string {
	var sb strings.Builder

	sb.WriteString("Condition: ")
	sb.WriteString(string(condition.ProcessGroupConditionType))
	sb.WriteString(" Timestamp: ")
	sb.WriteString(time.Unix(condition.Timestamp, 0).String())

	return sb.String()
}

// ProcessGroupConditionType represents a concrete ProcessGroupCondition.
type ProcessGroupConditionType string

const (
	// IncorrectPodSpec represents a process group that has an incorrect Pod spec.
	IncorrectPodSpec ProcessGroupConditionType = "IncorrectPodSpec"
	// IncorrectPodMetadata represents a process group that has an incorrect Pod metadata (labels and annotations differ).
	IncorrectPodMetadata ProcessGroupConditionType = "IncorrectPodMetadata"
	// IncorrectConfigMap represents a process group that has an incorrect ConfigMap.
	IncorrectConfigMap ProcessGroupConditionType = "IncorrectConfigMap"
	// IncorrectCommandLine represents a process group that has an incorrect commandline configuration.
	IncorrectCommandLine ProcessGroupConditionType = "IncorrectCommandLine"
	// PodFailing represents a process group which Pod keeps failing.
	PodFailing ProcessGroupConditionType = "PodFailing"
	// MissingPod represents a process group that doesn't have a Pod assigned.
	MissingPod ProcessGroupConditionType = "MissingPod"
	// MissingPVC represents a process group that doesn't have a PVC assigned.
	MissingPVC ProcessGroupConditionType = "MissingPVC"
	// MissingService represents a process group that doesn't have a Service assigned.
	MissingService ProcessGroupConditionType = "MissingService"
	// MissingProcesses represents a process group that misses a process.
	MissingProcesses ProcessGroupConditionType = "MissingProcesses"
	// ResourcesTerminating represents a process group whose resources are being
	// terminated.
	ResourcesTerminating ProcessGroupConditionType = "ResourcesTerminating"
	// SidecarUnreachable represents a process group where the sidecar is not reachable
	// because of networking or TLS issues.
	SidecarUnreachable ProcessGroupConditionType = "SidecarUnreachable"
	// PodPending represents a process group where the pod is in a pending state.
	PodPending ProcessGroupConditionType = "PodPending"
	// ReadyCondition is currently only used in the metrics.
	ReadyCondition ProcessGroupConditionType = "Ready"
	// NodeTaintDetected represents a Pod's node is tainted but not long enough for operator to replace it.
	// If a node is tainted with a taint that shouldn't trigger replacements, NodeTaintDetected won't be added to the pod
	NodeTaintDetected ProcessGroupConditionType = "NodeTaintDetected"
	// NodeTaintReplacing represents a Pod whose node has been tainted and the operator should replace the Pod
	NodeTaintReplacing ProcessGroupConditionType = "NodeTaintReplacing"
	// ProcessIsMarkedAsExcluded represents a process group where at least one process is excluded.
	ProcessIsMarkedAsExcluded ProcessGroupConditionType = "ProcessIsMarkedAsExcluded"
	// ProcessHasIOError represents a process group that has an I/O error.
	ProcessHasIOError ProcessGroupConditionType = "ProcessHasIOError"
	// IncorrectSidecarImage represents a process group where the sidecar image does not have the desired image.
	// This condition can occur during the migration of the image type, the change of the image configuration
	// for the sidecar or during version incompatible upgrades until the sidecar is updated to the new desired version.
	IncorrectSidecarImage ProcessGroupConditionType = "IncorrectSidecarImage"
	// ProcessHasHighRunLoopBusy represents a process group that has a high run loop busy value. A high run loop busy
	// value can be caused by infrastructure issues or by overloaded processes.
	ProcessHasHighRunLoopBusy ProcessGroupConditionType = "ProcessHasHighRunLoopBusy"
)

// AllProcessGroupConditionTypes returns all ProcessGroupConditionType
func AllProcessGroupConditionTypes() []ProcessGroupConditionType {
	return []ProcessGroupConditionType{
		IncorrectPodSpec,
		IncorrectPodMetadata,
		IncorrectConfigMap,
		IncorrectCommandLine,
		PodFailing,
		MissingPod,
		MissingPVC,
		MissingService,
		MissingProcesses,
		SidecarUnreachable,
		PodPending,
		ReadyCondition,
		NodeTaintDetected,
		NodeTaintReplacing,
		ProcessIsMarkedAsExcluded,
		ProcessHasIOError,
		IncorrectSidecarImage,
		ProcessHasHighRunLoopBusy,
	}
}

// GetProcessGroupConditionType returns the ProcessGroupConditionType for the matching string or an error
func GetProcessGroupConditionType(
	processGroupConditionType string,
) (ProcessGroupConditionType, error) {
	switch processGroupConditionType {
	case "IncorrectPodSpec":
		return IncorrectPodSpec, nil
	case "IncorrectPodMetadata":
		return IncorrectPodMetadata, nil
	case "IncorrectConfigMap":
		return IncorrectConfigMap, nil
	case "IncorrectCommandLine":
		return IncorrectCommandLine, nil
	case "PodFailing":
		return PodFailing, nil
	case "MissingPod":
		return MissingPod, nil
	case "MissingPVC":
		return MissingPVC, nil
	case "MissingService":
		return MissingService, nil
	case "MissingProcesses":
		return MissingProcesses, nil
	case "ResourcesTerminating":
		return ResourcesTerminating, nil
	case "SidecarUnreachable":
		return SidecarUnreachable, nil
	case "PodPending":
		return PodPending, nil
	case "Ready":
		return ReadyCondition, nil
	case "NodeTaintDetected":
		return NodeTaintDetected, nil
	case "NodeTaintReplacing":
		return NodeTaintReplacing, nil
	case "ProcessIsMarkedAsExcluded":
		return ProcessIsMarkedAsExcluded, nil
	case "ProcessHasIOError":
		return ProcessHasIOError, nil
	case "IncorrectSidecarImage":
		return IncorrectSidecarImage, nil
	case "ProcessHasHighRunLoopBusy":
		return ProcessHasHighRunLoopBusy, nil
	}

	return "", fmt.Errorf("unknown process group condition type: %s", processGroupConditionType)
}

// ClusterGenerationStatus stores information on which generations have reached
// different stages in reconciliation for the cluster.
type ClusterGenerationStatus struct {
	// Reconciled provides the last generation that was fully reconciled.
	Reconciled int64 `json:"reconciled,omitempty"`

	// NeedsConfigurationChange provides the last generation that is pending
	// a change to configuration.
	NeedsConfigurationChange int64 `json:"needsConfigurationChange,omitempty"`

	// NeedsCoordinatorChange provides the last generation that is pending
	// a change to its coordinators.
	NeedsCoordinatorChange int64 `json:"needsCoordinatorChange,omitempty"`

	// NeedsBounce provides the last generation that is pending a bounce of
	// fdbserver.
	NeedsBounce int64 `json:"needsBounce,omitempty"`

	// NeedsPodDeletion provides the last generation that is pending pods being
	// deleted and recreated.
	NeedsPodDeletion int64 `json:"needsPodDeletion,omitempty"`

	// NeedsShrink provides the last generation that is pending pods being
	// excluded and removed.
	NeedsShrink int64 `json:"needsShrink,omitempty"`

	// NeedsGrow provides the last generation that is pending pods being
	// added.
	NeedsGrow int64 `json:"needsGrow,omitempty"`

	// NeedsMonitorConfUpdate provides the last generation that needs an update
	// through the fdbmonitor conf.
	NeedsMonitorConfUpdate int64 `json:"needsMonitorConfUpdate,omitempty"`

	// DatabaseUnavailable provides the last generation that could not
	// complete reconciliation due to the database being unavailable.
	DatabaseUnavailable int64 `json:"missingDatabaseStatus,omitempty"`

	// HasExtraListeners provides the last generation that could not
	// complete reconciliation because it has more listeners than it is supposed
	// to.
	HasExtraListeners int64 `json:"hasExtraListeners,omitempty"`

	// NeedsServiceUpdate provides the last generation that needs an update
	// to the service config.
	NeedsServiceUpdate int64 `json:"needsServiceUpdate,omitempty"`

	// HasPendingRemoval provides the last generation that has pods that have
	// been excluded but are pending being removed.
	//
	// A cluster in this state is considered reconciled, but we track this in
	// the status to allow users of the operator to track when the removal
	// is fully complete.
	HasPendingRemoval int64 `json:"hasPendingRemoval,omitempty"`

	// HasUnhealthyProcess provides the last generation that has at least one
	// process group with a negative condition.
	HasUnhealthyProcess int64 `json:"hasUnhealthyProcess,omitempty"`

	// NeedsLockConfigurationChanges provides the last generation that is
	// pending a change to the configuration of the locking system.
	NeedsLockConfigurationChanges int64 `json:"needsLockConfigurationChanges,omitempty"`
}

// ClusterHealth represents different views into health in the cluster status.
type ClusterHealth struct {
	// Available reports whether the database is accepting reads and writes.
	Available bool `json:"available,omitempty"`

	// Healthy reports whether the database is in a fully healthy state.
	Healthy bool `json:"healthy,omitempty"`

	// FullReplication reports whether all data are fully replicated according
	// to the current replication policy.
	FullReplication bool `json:"fullReplication,omitempty"`

	// DataMovementPriority reports the priority of the highest-priority data
	// movement in the cluster.
	DataMovementPriority int `json:"dataMovementPriority,omitempty"`
}

// DatabaseInteractionMode defines how the operator should interact with the FDB cluster.
// +kubebuilder:validation:MaxLength=256
type DatabaseInteractionMode string

const (
	// DatabaseInteractionModeFdbcli defines the interaction mode with fdbcli.
	DatabaseInteractionModeFdbcli DatabaseInteractionMode = "fdbcli"

	// DatabaseInteractionModeMgmtAPI defines the interaction mode with the management API.
	DatabaseInteractionModeMgmtAPI DatabaseInteractionMode = "managementapi"
)

// LogGroup represents a LogGroup used by a FoundationDB process to log trace events. The LogGroup can be used to filter
// clients during an upgrade.
// +kubebuilder:validation:MaxLength=256
type LogGroup string

// alphanum provides the characters that are used for the generation ID in the
// connection string.
var alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// ConnectionString models the contents of a cluster file in a structured way
type ConnectionString struct {
	// DatabaseName provides an identifier for the database which persists
	// across coordinator changes.
	DatabaseName string `json:"databaseName,omitempty"`

	// GenerationID provides a unique ID for the current generation of
	// coordinators.
	GenerationID string `json:"generationID,omitempty"`

	// Coordinators provides the addresses of the current coordinators.
	Coordinators []string `json:"coordinators,omitempty"`
}

// isAlphanumeric regex to validate: value containing alphanumeric characters (a-z, A-Z, 0-9).
var isAlphanumeric = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// isAlphanumericWithUnderScore regex to validate: value containing alphanumeric characters (a-z, A-Z, 0-9) and underscores.
var isAlphanumericWithUnderScore = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// ParseConnectionString parses a connection string from its string
// representation
func ParseConnectionString(str string) (ConnectionString, error) {
	firstSplit := strings.SplitN(str, ":", 2)
	if len(firstSplit) != 2 {
		return ConnectionString{}, fmt.Errorf(
			"invalid connection string: %s, could not split string to get database description",
			str,
		)
	}

	// The description is a logical description of the database using alphanumeric characters (a-z, A-Z, 0-9) and underscores.
	description := firstSplit[0]
	if !isAlphanumericWithUnderScore.MatchString(description) {
		return ConnectionString{}, fmt.Errorf(
			"invalid connection string: %s, database description can only contain alphanumeric characters (a-z, A-Z, 0-9) and underscores",
			str,
		)
	}

	secondSplit := strings.SplitN(firstSplit[1], "@", 2)
	if len(secondSplit) != 2 {
		return ConnectionString{}, fmt.Errorf(
			"invalid connection string: %s, could not split string to get generation ID",
			str,
		)
	}

	// The ID is an arbitrary value containing alphanumeric characters (a-z, A-Z, 0-9).
	generationID := secondSplit[0]
	if !isAlphanumeric.MatchString(generationID) {
		return ConnectionString{}, fmt.Errorf(
			"invalid connection string: %s, generation ID can only contain alphanumeric characters (a-z, A-Z, 0-9)",
			str,
		)
	}

	coordinatorsStrings := strings.Split(secondSplit[1], ",")
	coordinators := make([]string, 0, len(coordinatorsStrings))
	for _, coordinatorsString := range coordinatorsStrings {
		coordinatorAddress, err := ParseProcessAddress(coordinatorsString)
		if err != nil {
			return ConnectionString{}, fmt.Errorf(
				"invalid connection string: %s, could not parse coordinator address: %s, got error: %w",
				str,
				coordinatorAddress,
				err,
			)
		}

		coordinators = append(coordinators, coordinatorAddress.String())
	}

	if len(coordinators) == 0 {
		return ConnectionString{}, fmt.Errorf(
			"invalid connection string: %s, connection string must contain at least one coordinator",
			str,
		)
	}

	return ConnectionString{
		description,
		generationID,
		coordinators,
	}, nil
}

// SanitizeConnectionStringDescription will replace "-" to "_".
func SanitizeConnectionStringDescription(str string) string {
	return strings.ReplaceAll(str, "-", "_")
}

// Validate will return nil if the connection string is valid or an error if the connection string is not valid.
func (str *ConnectionString) Validate() error {
	_, err := ParseConnectionString(str.String())
	return err
}

// String formats a connection string as a string
func (str *ConnectionString) String() string {
	return fmt.Sprintf(
		"%s:%s@%s",
		str.DatabaseName,
		str.GenerationID,
		strings.Join(str.Coordinators, ","),
	)
}

// GenerateNewGenerationID builds a new generation ID
func (str *ConnectionString) GenerateNewGenerationID() error {
	id := strings.Builder{}
	for i := 0; i < 32; i++ {
		err := id.WriteByte(alphanum[rand.Intn(len(alphanum))])
		if err != nil {
			return err
		}
	}
	str.GenerationID = id.String()
	return nil
}

// ClearUnsetDatabaseConfigurationKnobs clears any knobs that are not set in the FoundationDBCluster spec
// but which are present in the DatabaseConfiguration returned from the FoundationDBStatus.
func (cluster *FoundationDBCluster) ClearUnsetDatabaseConfigurationKnobs(
	configuration *DatabaseConfiguration,
) {
	// We have to reset the excluded servers here otherwise we will trigger a reconfiguration if one or more servers
	// are excluded.
	configuration.ExcludedServers = nil
	// Remove any specific version flags
	cluster.ClearMissingVersionFlags(configuration)

	if cluster.Spec.DatabaseConfiguration.StorageMigrationType == nil {
		configuration.StorageMigrationType = nil
	}

	if cluster.Spec.DatabaseConfiguration.PerpetualStorageWiggleLocality == nil {
		configuration.PerpetualStorageWiggleLocality = nil
	}

	if cluster.Spec.DatabaseConfiguration.PerpetualStorageWiggleEngine == nil {
		configuration.PerpetualStorageWiggleEngine = nil
	}

	if cluster.Spec.DatabaseConfiguration.PerpetualStorageWiggle == nil {
		configuration.PerpetualStorageWiggle = nil
	}

	// In case of the proxies, those are always set, even thought if only the GRV/commit proxies should be configured.
	if cluster.Spec.DatabaseConfiguration.AreSeparatedProxiesConfigured() {
		configuration.Proxies = 0
	} else {
		configuration.CommitProxies = 0
		configuration.GrvProxies = 0
	}
}

// ClearMissingVersionFlags clears any version flags in the given configuration that are not
// set in the configuration in the cluster spec.
//
// This allows us to compare the spec to the live configuration while ignoring
// version flags that are unset in the spec.
func (cluster *FoundationDBCluster) ClearMissingVersionFlags(configuration *DatabaseConfiguration) {
	if cluster.Spec.DatabaseConfiguration.LogVersion == 0 {
		configuration.LogVersion = 0
	}
	if cluster.Spec.DatabaseConfiguration.LogSpill == 0 {
		configuration.LogSpill = 0
	}
}

// GetRunningVersion returns the running version of the cluster defined in the cluster status or if not defined the version
// defined in the cluster spec.
func (cluster *FoundationDBCluster) GetRunningVersion() string {
	// We have to use the running version here otherwise if the operator gets killed during an upgrade it will try to apply
	// the grv and commit proxies.
	versionString := cluster.Status.RunningVersion
	if versionString == "" {
		return cluster.Spec.Version
	}

	return versionString
}

// ShouldUseLocks determine whether we should use locks to coordinator global
// operations.
func (cluster *FoundationDBCluster) ShouldUseLocks() bool {
	return false
}

// GetLockPrefix gets the prefix for the keys where we store locking
// information.
func (cluster *FoundationDBCluster) GetLockPrefix() string {

	return "\xff\x02/org.foundationdb.kubernetes-operator"
}

// GetMaintenancePrefix returns the prefix that is used by the operator to store and read maintenance related information.
// The prefix will be the provided GetLockPrefix appended by "maintenance".
func (cluster *FoundationDBCluster) GetMaintenancePrefix() string {
	return fmt.Sprintf("%s/maintenance", cluster.GetLockPrefix())
}

// GetLockDuration determines how long we hold locks for.
func (cluster *FoundationDBCluster) GetLockDuration() time.Duration {
	minutes := 10
	return time.Duration(minutes) * time.Minute
}

func (cluster *FoundationDBCluster) IsBeingUpgraded() bool {
	// FIXME: Implement
	return false
}
func (cluster *FoundationDBCluster) IsBeingUpgradedWithVersionIncompatibleVersion() bool {
	// FIXME: Implement
	return false
}

func (cluster *FoundationDBCluster) GetDatabaseInteractionMode() DatabaseInteractionMode {
	// FIXME: Implement
	return DatabaseInteractionModeFdbcli
}

func (cluster *FoundationDBCluster) GetLockID() string {
	return "fdb-baremetal-operator"
}

var (
	// PodIPFamilyUnset represents an unset IP family.
	PodIPFamilyUnset = 0
	// PodIPFamilyIPv4 represents the desired IP family as IPv4.
	PodIPFamilyIPv4 = 4
	// PodIPFamilyIPv6 represents the desired IP family as IPv6.
	PodIPFamilyIPv6 = 6
)

func (cluster *FoundationDBCluster) GetPodIPFamily() int {
	return PodIPFamilyIPv4
}

func (cluster *FoundationDBCluster) GetLogGroup() string {
	return "fdb-cluster"
}

// GetRoleCountsWithDefaults gets the role counts from the cluster spec and
// fills in default values for any role counts that are 0.
//
// The default Storage value will be 2F + 1, where F is the cluster's fault
// tolerance.
//
// The default Logs value will be 3 or 4 for three_data_hall.
//
// The default Proxies value will be 3.
//
// The default Resolvers value will be 1.
//
// The default RemoteLogs value will be equal to the Logs value when the
// UsableRegions is greater than 1. It will be equal to -1 when the
// UsableRegions is less than or equal to 1.
//
// The default LogRouters value will be equal to 3 times the Logs value when
// the UsableRegions is greater than 1. It will be equal to -1 when the
// UsableRegions is less than or equal to 1.
func (cluster *FoundationDBCluster) GetRoleCountsWithDefaults() RoleCounts {
	return cluster.Spec.DatabaseConfiguration.GetRoleCountsWithDefaults(
		cluster.DesiredFaultTolerance(),
	)
}

// calculateProcessCount determines the process count from a given role count.
//
// alternatives provides a list of other process counts that can fulfill this
// role instead. If any of those process counts is positive, then this will
// return 0.
func (cluster *FoundationDBCluster) calculateProcessCountFromRole(
	count int,
	alternatives ...int,
) int {
	for _, value := range alternatives {
		if value > 0 {
			return 0
		}
	}
	if count < 0 {
		return 0
	}

	return count
}

// calculateProcessCount calculates the process count for a process class based
// on the counts for the roles it can fulfill.
//
// If addFaultTolerance is true, this will add the cluster's desired fault
// tolerance to the result.
//
// If the cluster is using multi-KC replication, this will divide the total
// count across the number of KCs in the data center.
func (cluster *FoundationDBCluster) calculateProcessCount(
	addFaultTolerance bool,
	counts ...int,
) int {
	var count = 0

	for _, possibleCount := range counts {
		if possibleCount > count {
			count = possibleCount
		}
	}
	if count > 0 {
		if addFaultTolerance {
			count += cluster.DesiredFaultTolerance()
		}
		return count
	}

	return -1
}

// GetProcessCountsWithDefaults gets the process counts from the cluster spec
// and fills in default values for any counts that are 0. The number of storage processes
// will only reflect the number of Pods that will be created to host storage server processes.
// If storageServersPerPod is set the total amount of storage server processes will be
// the storage process count multiplied by storageServersPerPod.
func (cluster *FoundationDBCluster) GetProcessCountsWithDefaults() (ProcessCounts, error) {
	DCID := "FIXME"
	roleCounts := cluster.GetRoleCountsWithDefaults()
	processCounts := cluster.Spec.DatabaseConfiguration.ProcessCounts

	var isSatellite, isMain bool
	satelliteLogs := 0
	for _, region := range cluster.Spec.DatabaseConfiguration.Regions {
		for _, dataCenter := range region.DataCenters {
			if dataCenter.ID == DCID {
				if dataCenter.Satellite == 0 {
					isMain = true
				} else {
					isSatellite = true
					if region.SatelliteLogs > satelliteLogs {
						satelliteLogs = region.SatelliteLogs
					}
				}
			}
		}
	}

	if isSatellite && !isMain {
		if processCounts.Log == 0 {
			processCounts.Log = 1 + satelliteLogs
			return processCounts, nil
		}
	}

	if processCounts.Storage == 0 {
		processCounts.Storage = cluster.calculateProcessCount(false,
			roleCounts.Storage)
	}

	if processCounts.Log == 0 {
		processCounts.Log = cluster.calculateProcessCount(true,
			cluster.calculateProcessCountFromRole(roleCounts.Logs+satelliteLogs, processCounts.Log),
			cluster.calculateProcessCountFromRole(roleCounts.RemoteLogs, processCounts.Log),
		)
	}

	if processCounts.Stateless == 0 {
		primaryStatelessCount := cluster.calculateProcessCountFromRole(1, processCounts.Master) +
			cluster.calculateProcessCountFromRole(1, processCounts.ClusterController) +
			cluster.calculateProcessCountFromRole(roleCounts.Resolvers, processCounts.Resolution)
		primaryStatelessCount += cluster.calculateProcessCountFromRole(
			1,
			processCounts.Ratekeeper,
		) +
			cluster.calculateProcessCountFromRole(
				1,
				processCounts.DataDistributor,
			)

		if cluster.Spec.DatabaseConfiguration.AreSeparatedProxiesConfigured() {
			primaryStatelessCount += cluster.calculateProcessCountFromRole(
				roleCounts.GrvProxies,
				processCounts.GrvProxy,
			)
			primaryStatelessCount += cluster.calculateProcessCountFromRole(
				roleCounts.CommitProxies,
				processCounts.CommitProxy,
			)
		} else {
			primaryStatelessCount += cluster.calculateProcessCountFromRole(roleCounts.Proxies, processCounts.Proxy)
		}

		processCounts.Stateless = cluster.calculateProcessCount(true,
			primaryStatelessCount,
			cluster.calculateProcessCountFromRole(roleCounts.LogRouters),
		)
	}

	return processCounts, nil
}

// DesiredFaultTolerance returns the number of replicas we should be able to
// lose when the cluster is at full replication health.
func (cluster *FoundationDBCluster) DesiredFaultTolerance() int {
	return DesiredFaultTolerance(cluster.Spec.DatabaseConfiguration.RedundancyMode)
}

// MinimumFaultDomains returns the number of fault domains the cluster needs
// to function.
func (cluster *FoundationDBCluster) MinimumFaultDomains() int {
	return MinimumFaultDomains(cluster.Spec.DatabaseConfiguration.RedundancyMode)
}

// DesiredCoordinatorCount returns the number of coordinators to recruit for a cluster.
func (cluster *FoundationDBCluster) DesiredCoordinatorCount() int {
	if cluster.Spec.DatabaseConfiguration.UsableRegions > 1 ||
		cluster.Spec.DatabaseConfiguration.RedundancyMode == RedundancyModeThreeDataHall {
		return 9
	}

	return cluster.MinimumFaultDomains() + cluster.DesiredFaultTolerance()
}

func (cluster *FoundationDBCluster) GetResourceLabels() map[string]string {
	labels := map[string]string{}
	return labels
}
