/*
 * admin_client_mock.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020-2022 Apple Inc. and the FoundationDB project authors
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

package mock

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal/metrics"

	"k8s.io/utils/ptr"

	"github.com/FoundationDB/fdb-kubernetes-operator/v2/pkg/fdbadminclient"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AdminClient provides a mock implementation of the cluster admin interface
type AdminClient struct {
	Cluster                                  *fdbv1beta2.FoundationDBBaremetalCluster
	KubeClient                               client.Client
	DatabaseConfiguration                    *fdbv1beta2.DatabaseConfiguration
	ExcludedAddresses                        map[string]fdbv1beta2.None
	KilledAddresses                          map[string]fdbv1beta2.None
	Knobs                                    map[string]fdbv1beta2.None
	missingLocalities                        map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None
	missingProcessGroups                     map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None
	incorrectCommandLines                    map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None
	pendingForRemoval                        map[fdbv1beta2.ProcessGroupID]time.Time
	pendingForExclusion                      map[fdbv1beta2.ProcessGroupID]time.Time
	pendingForInclusion                      map[fdbv1beta2.ProcessGroupID]time.Time
	pendingForRestart                        map[fdbv1beta2.ProcessGroupID]time.Time
	readyForExclusion                        map[fdbv1beta2.ProcessGroupID]time.Time
	readyForInclusion                        map[fdbv1beta2.ProcessGroupID]time.Time
	readyForRestart                          map[fdbv1beta2.ProcessGroupID]time.Time
	coordinationStateProcessAddresses        map[fdbv1beta2.ProcessGroupID][]string
	FrozenStatus                             *fdbv1beta2.FoundationDBStatus
	clientVersions                           map[string][]string
	currentCommandLines                      map[string]string
	VersionProcessGroups                     map[fdbv1beta2.ProcessGroupID]string
	ReincludedAddresses                      map[string]bool
	additionalProcesses                      []fdbv1beta2.ProcessGroupStatus
	localityInfo                             map[fdbv1beta2.ProcessGroupID]map[string]string
	MaxZoneFailuresWithoutLosingData         *int
	MaxZoneFailuresWithoutLosingAvailability *int
	MaintenanceZone                          fdbv1beta2.FaultDomain
	restoreURL                               string
	maintenanceZoneStartTimestamp            time.Time
	MockAdditionTimeForGlobalCoordination    time.Time
	uptimeSecondsForMaintenanceZone          float64
	TeamTracker                              []fdbv1beta2.FoundationDBStatusTeamTracker
	Logs                                     []fdbv1beta2.FoundationDBStatusLogInfo
	mockError                                error
	LagInfo                                  map[string]fdbv1beta2.FoundationDBStatusLagInfo
	processesUnderMaintenance                map[fdbv1beta2.ProcessGroupID]int64
	RunLoopBusy                              map[fdbv1beta2.ProcessGroupID]float64
	ProcessMessages                          map[fdbv1beta2.ProcessGroupID][]fdbv1beta2.FoundationDBStatusProcessMessage
}

var _ fdbadminclient.AdminClient = (*AdminClient)(nil)

// adminClientCache provides a cache of mock admin clients.
var adminClientCache = make(map[string]*AdminClient)
var adminClientMutex sync.Mutex

// NewMockAdminClient creates an admin client for a cluster.
func NewMockAdminClient(
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
	kubeClient client.Client,
) (fdbadminclient.AdminClient, error) {
	return NewMockAdminClientUncast(cluster, kubeClient)
}

// NewMockAdminClientUncast creates a mock admin client for a cluster.
// nolint:unparam
// is required because we always return a nil error
func NewMockAdminClientUncast(
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
	kubeClient client.Client,
) (*AdminClient, error) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	cachedClient := adminClientCache[cluster.Name]

	if cachedClient == nil {
		cachedClient = &AdminClient{
			Cluster:                           cluster.DeepCopy(),
			KubeClient:                        kubeClient,
			ExcludedAddresses:                 make(map[string]fdbv1beta2.None),
			ReincludedAddresses:               make(map[string]bool),
			KilledAddresses:                   make(map[string]fdbv1beta2.None),
			missingProcessGroups:              make(map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None),
			missingLocalities:                 make(map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None),
			incorrectCommandLines:             make(map[fdbv1beta2.ProcessGroupID]fdbv1beta2.None),
			pendingForRemoval:                 make(map[fdbv1beta2.ProcessGroupID]time.Time),
			pendingForExclusion:               make(map[fdbv1beta2.ProcessGroupID]time.Time),
			pendingForInclusion:               make(map[fdbv1beta2.ProcessGroupID]time.Time),
			pendingForRestart:                 make(map[fdbv1beta2.ProcessGroupID]time.Time),
			readyForExclusion:                 make(map[fdbv1beta2.ProcessGroupID]time.Time),
			readyForInclusion:                 make(map[fdbv1beta2.ProcessGroupID]time.Time),
			readyForRestart:                   make(map[fdbv1beta2.ProcessGroupID]time.Time),
			coordinationStateProcessAddresses: make(map[fdbv1beta2.ProcessGroupID][]string),
			localityInfo: make(
				map[fdbv1beta2.ProcessGroupID]map[string]string,
			),
			currentCommandLines:  make(map[string]string),
			Knobs:                make(map[string]fdbv1beta2.None),
			VersionProcessGroups: make(map[fdbv1beta2.ProcessGroupID]string),
			LagInfo: make(
				map[string]fdbv1beta2.FoundationDBStatusLagInfo,
			),
			processesUnderMaintenance: make(map[fdbv1beta2.ProcessGroupID]int64),
			RunLoopBusy:               make(map[fdbv1beta2.ProcessGroupID]float64),
			ProcessMessages: make(
				map[fdbv1beta2.ProcessGroupID][]fdbv1beta2.FoundationDBStatusProcessMessage,
			),
		}
		adminClientCache[cluster.Name] = cachedClient
	} else {
		cachedClient.Cluster = cluster.DeepCopy()
	}

	return cachedClient, nil
}

// ClearMockAdminClients clears the cache of mock Admin clients
func ClearMockAdminClients() {
	adminClientCache = map[string]*AdminClient{}
}

// GetStatus gets the database's status
func (client *AdminClient) GetStatus() (*fdbv1beta2.FoundationDBStatus, error) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	if client.mockError != nil {
		return nil, client.mockError
	}

	if client.FrozenStatus != nil {
		return client.FrozenStatus, nil
	}
	status := &fdbv1beta2.FoundationDBStatus{}

	return status, nil
}

// ConfigureDatabase changes the database configuration
func (client *AdminClient) ConfigureDatabase(
	configuration fdbv1beta2.DatabaseConfiguration,
	_ bool,
) error {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	if client.mockError != nil {
		return client.mockError
	}

	client.DatabaseConfiguration = configuration.DeepCopy()
	if configuration.StorageMigrationType == nil {
		migrationType := fdbv1beta2.StorageMigrationTypeDisabled
		client.DatabaseConfiguration.StorageMigrationType = &migrationType
	}

	if configuration.PerpetualStorageWiggleEngine == nil {
		storageEngineNone := fdbv1beta2.StorageEngineNone
		client.DatabaseConfiguration.PerpetualStorageWiggleEngine = &storageEngineNone
	}

	if configuration.PerpetualStorageWiggleLocality == nil {
		client.DatabaseConfiguration.PerpetualStorageWiggleLocality = ptr.To("0")
	}

	return nil
}

// ExcludeProcesses starts evacuating processes so that they can be removed
// from the database.
func (client *AdminClient) ExcludeProcesses(addresses []fdbv1beta2.ProcessAddress) error {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	if client.mockError != nil {
		return client.mockError
	}

	for _, pAddr := range addresses {
		address := pAddr.String()
		client.ExcludedAddresses[address] = fdbv1beta2.None{}
	}

	return nil
}

// ExcludeProcessesWithNoWait starts evacuating processes so that they can be removed from the database. If noWait is
// set to true, the exclude command will not block until all data is moved away from the processes.
func (client *AdminClient) ExcludeProcessesWithNoWait(
	addresses []fdbv1beta2.ProcessAddress,
	_ bool,
) error {
	return client.ExcludeProcesses(addresses)
}

// IncludeProcesses removes processes from the exclusion list and allows
// them to take on roles again.
func (client *AdminClient) IncludeProcesses(addresses []fdbv1beta2.ProcessAddress) error {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	for _, address := range addresses {
		addr := address.String()
		_, ok := client.ExcludedAddresses[addr]
		if ok {
			client.ReincludedAddresses[addr] = true
			delete(client.ExcludedAddresses, addr)
		}
	}

	return nil
}

// getExcludedAddresses will return the excluded addresses based on the client.ExcludedAddresses.
func (client *AdminClient) getExcludedAddresses() []fdbv1beta2.ProcessAddress {
	if len(client.ExcludedAddresses) == 0 {
		return []fdbv1beta2.ProcessAddress{}
	}

	excludedAddresses := make([]fdbv1beta2.ProcessAddress, 0, len(client.ExcludedAddresses))
	for addr := range client.ExcludedAddresses {
		ip := net.ParseIP(addr)
		if ip == nil {
			excludedAddresses = append(
				excludedAddresses,
				fdbv1beta2.ProcessAddress{StringAddress: addr},
			)
			continue
		}

		excludedAddresses = append(excludedAddresses, fdbv1beta2.ProcessAddress{
			IPAddress: net.ParseIP(addr),
			Port:      0,
			Flags:     nil,
		})
	}

	return excludedAddresses
}

// KillProcesses restarts processes
func (client *AdminClient) KillProcesses(addresses []fdbv1beta2.ProcessAddress) error {
	adminClientMutex.Lock()

	if client.mockError != nil {
		return client.mockError
	}

	for _, addr := range addresses {
		// Set the StringAddress to an empty string, to ensure the IP address will be picked. The kill command doesn't
		// support localities right now.
		if addr.IPAddress != nil {
			addr.StringAddress = ""
		}
		client.KilledAddresses[addr.String()] = fdbv1beta2.None{}
		// Remove the commandline from the cached status and let it be recomputed in the next GetStatus request.
		// This reflects that the commandline will only be updated if the processes are actually be restarted.
		delete(client.currentCommandLines, addr.StringWithoutFlags())
	}
	adminClientMutex.Unlock()

	if client.Cluster.Status.RunningVersion != client.Cluster.Spec.Version {
		// We have to do this in the mock client, in the real world the tryConnectionOptions in update_status,
		// will update the version.
		client.Cluster.Status.RunningVersion = client.Cluster.Spec.Version
		err := client.KubeClient.Status().Update(context.TODO(), client.Cluster)
		if err != nil {
			return err
		}
	}

	client.UnfreezeStatus()
	return nil
}

// GetExclusions gets a list of the addresses currently excluded from the database.
func (client *AdminClient) GetExclusions() ([]fdbv1beta2.ProcessAddress, error) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	if client.mockError != nil {
		return nil, client.mockError
	}

	return client.getExcludedAddresses(), nil
}

// KillProcessesForUpgrade restarts processes for upgrades, this will issue 2 kill commands to make sure all
// processes are restarted.
func (client *AdminClient) KillProcessesForUpgrade(addresses []fdbv1beta2.ProcessAddress) error {
	return client.KillProcesses(addresses)
}

// ChangeCoordinators changes the coordinator set
func (client *AdminClient) ChangeCoordinators(
	addresses []fdbv1beta2.ProcessAddress,
) (string, error) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	if client.mockError != nil {
		return "", client.mockError
	}

	connectionString, err := fdbv1beta2.ParseConnectionString(
		client.Cluster.Status.ConnectionString,
	)
	if err != nil {
		return "", err
	}
	err = connectionString.GenerateNewGenerationID()
	if err != nil {
		return "", err
	}
	newCoord := make([]string, len(addresses))
	for idx, coord := range addresses {
		newCoord[idx] = coord.String()
	}

	// Increment the coordinator changes counter for this cluster
	metrics.CoordinatorChangesCounter.WithLabelValues(client.Cluster.Namespace, client.Cluster.Name).
		Inc()
	connectionString.Coordinators = newCoord
	return connectionString.String(), err
}

// VersionSupported reports whether we can support a cluster with a given
// version.
func (client *AdminClient) VersionSupported(versionString string) (bool, error) {
	if client.mockError != nil {
		return false, client.mockError
	}

	version, err := fdbv1beta2.ParseFdbVersion(versionString)
	if err != nil {
		return false, err
	}

	if !version.IsSupported() {
		return false, nil
	}

	return true, nil
}

// GetProtocolVersion determines the protocol version that is used by a
// version of FDB.
func (client *AdminClient) GetProtocolVersion(version string) (string, error) {
	if client.mockError != nil {
		return "", client.mockError
	}

	return version, nil
}

// GetRestoreStatus gets the status of the current restore.
func (client *AdminClient) GetRestoreStatus() (string, error) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	if client.mockError != nil {
		return "", client.mockError
	}

	if client.restoreURL == "" {
		return "", nil
	}

	return fmt.Sprintf("%s, State: completed\n", client.restoreURL), nil
}

// MockClientVersion returns a mocked client version
func (client *AdminClient) MockClientVersion(version string, clients []string) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	if client.clientVersions == nil {
		client.clientVersions = make(map[string][]string)
	}
	client.clientVersions[version] = clients
}

// MockAdditionalProcesses adds additional processes to the cluster status.
func (client *AdminClient) MockAdditionalProcesses(processes []fdbv1beta2.ProcessGroupStatus) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	client.additionalProcesses = append(client.additionalProcesses, processes...)
}

// MockMissingProcessGroup updates the mock for whether a process group should
// be missing from the cluster status.
func (client *AdminClient) MockMissingProcessGroup(
	processGroupID fdbv1beta2.ProcessGroupID,
	missing bool,
) {
	if missing {
		client.missingProcessGroups[processGroupID] = fdbv1beta2.None{}
		return
	}

	delete(client.missingProcessGroups, processGroupID)
}

// MockLocalityInfo sets mock locality information for a process.
func (client *AdminClient) MockLocalityInfo(
	processGroupID fdbv1beta2.ProcessGroupID,
	locality map[string]string,
) {
	client.localityInfo[processGroupID] = locality
}

// MockIncorrectCommandLine updates the mock for whether a process group should
// be have an incorrect command-line.
func (client *AdminClient) MockIncorrectCommandLine(
	processGroupID fdbv1beta2.ProcessGroupID,
	incorrect bool,
) {
	if incorrect {
		client.incorrectCommandLines[processGroupID] = fdbv1beta2.None{}
		return
	}

	delete(client.incorrectCommandLines, processGroupID)
}

// MockMissingLocalities updates the mock to remove the localities for the provided process group.
func (client *AdminClient) MockMissingLocalities(
	processGroupID fdbv1beta2.ProcessGroupID,
	missingLocalities bool,
) {
	if missingLocalities {
		client.missingLocalities[processGroupID] = fdbv1beta2.None{}
		return
	}

	delete(client.missingLocalities, processGroupID)
}

// Close shuts down any resources for the client once it is no longer
// needed.
func (client *AdminClient) Close() error {
	return client.mockError
}

// FreezeStatus causes the GetStatus method to return its current value until
// UnfreezeStatus is called, or another method is called which would invalidate
// the status.
func (client *AdminClient) FreezeStatus() error {
	if client.mockError != nil {
		return client.mockError
	}

	status, err := client.GetStatus()
	if err != nil {
		return err
	}

	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	client.FrozenStatus = status
	return nil
}

// UnfreezeStatus causes the admin client to start recalculating the status
// on every call to GetStatus
func (client *AdminClient) UnfreezeStatus() {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	client.FrozenStatus = nil
}

// SetKnobs sets the knobs that should be used for the commandline call.
func (client *AdminClient) SetKnobs(knobs []string) {
	client.Knobs = make(map[string]fdbv1beta2.None, len(knobs))
	for _, knob := range knobs {
		client.Knobs[knob] = fdbv1beta2.None{}
	}
}

// SetMaintenanceZone places zone into maintenance mode
func (client *AdminClient) SetMaintenanceZone(zone string, _ int) error {
	if client.mockError != nil {
		return client.mockError
	}

	client.MaintenanceZone = fdbv1beta2.FaultDomain(zone)
	client.maintenanceZoneStartTimestamp = time.Now()
	return nil
}

// ResetMaintenanceMode resets the maintenance zone
func (client *AdminClient) ResetMaintenanceMode() error {
	if client.mockError != nil {
		return client.mockError
	}

	client.MaintenanceZone = ""
	return nil
}

// MockUptimeSecondsForMaintenanceZone mocks the uptime for maintenance zone
func (client *AdminClient) MockUptimeSecondsForMaintenanceZone(seconds float64) {
	client.uptimeSecondsForMaintenanceZone = seconds
}

// MockError mocks an error that will be returned when making any calls to the mock client. This can be reset by passing
// a nil value to this method.
func (client *AdminClient) MockError(err error) {
	client.mockError = err
}

// SetLimitingDurabilityLag sets/mocks the limiting durability lag of any storage server in the cluster.
func (client *AdminClient) SetLimitingDurabilityLag(lagInfo *fdbv1beta2.FoundationDBStatusLagInfo) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	client.LagInfo["limitingDurabilityLag"] = *lagInfo
}

// GetLimitingDurabilityLag returns the limiting durability lag of any storage server in the cluster.
func (client *AdminClient) GetLimitingDurabilityLag() (fdbv1beta2.FoundationDBStatusLagInfo, bool) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	lagInfo, ok := client.LagInfo["limitingDurabilityLag"]
	return lagInfo, ok
}

// SetWorstDataLag sets/mocks the worst data lag of any storage server in the cluster.
func (client *AdminClient) SetWorstDataLag(lagInfo *fdbv1beta2.FoundationDBStatusLagInfo) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	client.LagInfo["worstDataLag"] = *lagInfo
}

// GetWorstDataLag returns the (mocked) worst data lag of any storage server in the cluster.
func (client *AdminClient) GetWorstDataLag() (fdbv1beta2.FoundationDBStatusLagInfo, bool) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	lagInfo, ok := client.LagInfo["worstDataLag"]
	return lagInfo, ok
}

// SetWorstDurabilityLag sets/mocks the worst durability lag of any storage server in the cluster.
func (client *AdminClient) SetWorstDurabilityLag(lagInfo *fdbv1beta2.FoundationDBStatusLagInfo) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	client.LagInfo["worstDurabilityLag"] = *lagInfo
}

// GetWorstDurabilityLag returns the (mocked) worst durability lag of any storage server in the cluster.
func (client *AdminClient) GetWorstDurabilityLag() (fdbv1beta2.FoundationDBStatusLagInfo, bool) {
	adminClientMutex.Lock()
	defer adminClientMutex.Unlock()

	lagInfo, ok := client.LagInfo["worstDurabilityLag"]
	return lagInfo, ok
}

// WithValues will update the logger used by the current AdminClient to contain the provided key value pairs. The provided
// arguments must be even.
func (client *AdminClient) WithValues(_ ...interface{}) {}

// SetTimeout will overwrite the default timeout for interacting the FDB cluster.
func (client *AdminClient) SetTimeout(_ time.Duration) {}

// GetProcessesUnderMaintenance will return all process groups that are currently stored to be under maintenance.
// The result is a map with the process group ID as key and the start of the maintenance as value.
func (client *AdminClient) GetProcessesUnderMaintenance() (map[fdbv1beta2.ProcessGroupID]int64, error) {
	// We have to create a copy here as the map will be a pointer.
	res := make(map[fdbv1beta2.ProcessGroupID]int64, len(client.processesUnderMaintenance))

	for processGroupID, timestamp := range client.processesUnderMaintenance {
		res[processGroupID] = timestamp
	}

	return res, nil
}

// RemoveProcessesUnderMaintenance will remove the provided process groups from the list of processes that
// are planned to be taken down for maintenance.
func (client *AdminClient) RemoveProcessesUnderMaintenance(ids []fdbv1beta2.ProcessGroupID) error {
	for _, id := range ids {
		delete(client.processesUnderMaintenance, id)
	}

	return nil
}

// SetProcessesUnderMaintenance will add the provided process groups to the list of processes that will be taken
// down for maintenance. The value will be the provided time stamp.
func (client *AdminClient) SetProcessesUnderMaintenance(
	ids []fdbv1beta2.ProcessGroupID,
	timestamp int64,
) error {
	for _, id := range ids {
		client.processesUnderMaintenance[id] = timestamp
	}

	return nil
}

// GetVersionFromReachableCoordinators will return the running version based on the reachable coordinators. This method
// can be used during version incompatible upgrades and based on the responses of the coordinators, this method will
// assume the current running version of the cluster. If the fdbcli calls for none of the provided version return
// a majority of reachable coordinators, the default version from the cluster.Status.RunningVersion will be returned.
func (client *AdminClient) GetVersionFromReachableCoordinators() string {
	// TODO (johscheuer): In the future we could allow to mock another running version.
	return client.Cluster.Status.RunningVersion
}

func (client *AdminClient) getAdditionTime() time.Time {
	if client.MockAdditionTimeForGlobalCoordination.IsZero() {
		return time.Now()
	}

	return client.MockAdditionTimeForGlobalCoordination
}

// GetPendingForRemoval gets the process group IDs for all process groups that are marked for removal.
func (client *AdminClient) GetPendingForRemoval(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.handleGetReadyOrPending(prefix, client.pendingForRemoval)
}

// GetPendingForExclusion gets the process group IDs for all process groups that should be excluded.
func (client *AdminClient) GetPendingForExclusion(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.handleGetReadyOrPending(prefix, client.pendingForExclusion)
}

// GetPendingForInclusion gets the process group IDs for all the process groups that should be included.
func (client *AdminClient) GetPendingForInclusion(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.handleGetReadyOrPending(prefix, client.pendingForInclusion)
}

// GetPendingForRestart gets the process group IDs for all the process groups that should be restarted.
func (client *AdminClient) GetPendingForRestart(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.handleGetReadyOrPending(prefix, client.pendingForRestart)
}

// GetReadyForExclusion gets the process group IDs for all the process groups that are ready to be excluded.
func (client *AdminClient) GetReadyForExclusion(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.handleGetReadyOrPending(prefix, client.readyForExclusion)
}

// GetReadyForInclusion gets the process group IDs for all the process groups that are ready to be included.
func (client *AdminClient) GetReadyForInclusion(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.handleGetReadyOrPending(prefix, client.readyForInclusion)
}

// GetReadyForRestart gets the process group IDs for all the process groups that are ready to be restarted.
func (client *AdminClient) GetReadyForRestart(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	return client.handleGetReadyOrPending(prefix, client.readyForRestart)
}

// ClearReadyForRestart removes all the process group IDs for all the process groups that are ready to be restarted.
func (client *AdminClient) ClearReadyForRestart() error {
	client.readyForRestart = make(map[fdbv1beta2.ProcessGroupID]time.Time)
	return nil
}

func (client *AdminClient) handleGetReadyOrPending(
	prefix string,
	processes map[fdbv1beta2.ProcessGroupID]time.Time,
) (map[fdbv1beta2.ProcessGroupID]time.Time, error) {
	result := map[fdbv1beta2.ProcessGroupID]time.Time{}

	for processGroupID, timestamp := range processes {
		if !strings.HasPrefix(string(processGroupID), prefix) {
			continue
		}

		result[processGroupID] = timestamp
	}

	return result, nil
}

// UpdateProcessAddresses updates the process addresses for the specified process group ID. If the provided slice is empty or nil, the entry will be deleted.
func (client *AdminClient) UpdateProcessAddresses(
	updates map[fdbv1beta2.ProcessGroupID][]string,
) error {
	client.coordinationStateProcessAddresses = updates
	return nil
}

// GetProcessAddresses gets the process group IDs and their associated process addresses.
func (client *AdminClient) GetProcessAddresses(
	prefix string,
) (map[fdbv1beta2.ProcessGroupID][]string, error) {
	result := map[fdbv1beta2.ProcessGroupID][]string{}

	for processGroupID, timestamp := range client.coordinationStateProcessAddresses {
		if !strings.HasPrefix(string(processGroupID), prefix) {
			continue
		}

		result[processGroupID] = timestamp
	}

	return result, nil
}
