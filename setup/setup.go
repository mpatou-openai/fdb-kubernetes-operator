/*
 * setup.go
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

package setup

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/go-logr/logr"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/controllers"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/fdbclient"
	"gopkg.in/natefinch/lumberjack.v2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options provides all configuration Options for the operator
type Options struct {
	CleanUpOldLogFile               bool
	CompressOldFiles                bool
	PrintVersion                    bool
	ServerSideApply                 bool
	EnableRecoveryState             bool
	CacheDatabaseStatus             bool
	EnableNodeIndex                 bool
	ReplaceOnSecurityContextChange  bool
	MetricsAddr                     string
	LogFile                         string
	LogFilePermission               string
	ClusterLabelKeyForNodeTrigger   string
	HealthProbeBindAddress          string
	WatchNamespace                  string
	CliTimeout                      int
	MaxCliTimeout                   int
	MaxConcurrentReconciles         int
	LogFileMaxSize                  int
	LogFileMaxAge                   int
	MaxNumberOfOldLogFiles          int
	MinimumRecoveryTimeForExclusion float64
	MinimumRecoveryTimeForInclusion float64
	HighRunLoopBusyThreshold        float64
	LogFileMinAge                   time.Duration
	GetTimeout                      time.Duration
	PostTimeout                     time.Duration
	MaintenanceListStaleDuration    time.Duration
	MaintenanceListWaitDuration     time.Duration
	// GlobalSynchronizationWaitDuration is the wait time for the operator when the synchronization mode is set to
	// global. The wait time defines the period where no updates for the according action should happen. Increasing the
	// wait time will increase the chances that all updates are part of the list but will also delay the rollout of
	// the change.
	GlobalSynchronizationWaitDuration time.Duration
	MinimumRequiredUptimeCCBounce     time.Duration
	SaltMasterAddress                 string
	SecurityUpdateConfigPath          string
}

// BindFlags will parse the given flagset for the operator option flags
func (o *Options) BindFlags(fs *flag.FlagSet) {
	fs.StringVar(
		&o.MetricsAddr,
		"metrics-addr",
		":8080",
		"The address the metric endpoint binds to.",
	)
	fs.StringVar(&o.LogFile, "log-file", "", "The path to a file to write logs to.")
	fs.IntVar(&o.CliTimeout, "cli-timeout", 10, "The timeout to use for CLI commands in seconds.")
	fs.IntVar(
		&o.MaxCliTimeout,
		"max-cli-timeout",
		40,
		"The maximum timeout to use for CLI commands in seconds. This timeout is used for CLI requests that are known to be potentially slow like get status or exclude.",
	)
	fs.IntVar(
		&o.MaxConcurrentReconciles,
		"max-concurrent-reconciles",
		1,
		"Defines the maximum number of concurrent reconciles for all controllers.",
	)
	fs.BoolVar(
		&o.CleanUpOldLogFile,
		"cleanup-old-cli-logs",
		true,
		"Defines if the operator should delete old fdbcli log files.",
	)
	fs.DurationVar(
		&o.LogFileMinAge,
		"log-file-min-age",
		5*time.Minute,
		"Defines the minimum age of fdbcli log files before removing when \"--cleanup-old-cli-logs\" is set.",
	)
	fs.IntVar(
		&o.LogFileMaxAge,
		"log-file-max-age",
		28,
		"Defines the maximum age to retain old operator log file in number of days.",
	)
	fs.IntVar(
		&o.LogFileMaxSize,
		"log-file-max-size",
		250,
		"Defines the maximum size in megabytes of the operator log file before it gets rotated.",
	)
	fs.StringVar(
		&o.LogFilePermission,
		"log-file-permission",
		"0644",
		"The file permission for the log file. Only used if log-file is set. Only the octal representation is supported.",
	)
	fs.StringVar(&o.ClusterLabelKeyForNodeTrigger, "cluster-label-key-for-node-trigger", "",
		"The label key to use to trigger a reconciliation if a node resources changes.")
	fs.StringVar(
		&o.HealthProbeBindAddress,
		"health-probe-bind-address",
		"[::1]:9443",
		"The address the operator binds to for health / readiness / liveness probes.",
	)
	fs.IntVar(
		&o.MaxNumberOfOldLogFiles,
		"max-old-log-files",
		3,
		"Defines the maximum number of old operator log files to retain.",
	)
	fs.BoolVar(
		&o.CompressOldFiles,
		"compress",
		false,
		"Defines whether the rotated log files should be compressed using gzip or not.",
	)
	fs.BoolVar(&o.PrintVersion, "version", false, "Prints the version of the operator and exits.")
	fs.DurationVar(
		&o.GetTimeout,
		"get-timeout",
		5*time.Second,
		"http timeout for get requests to the FDB sidecar.",
	)
	fs.DurationVar(
		&o.PostTimeout,
		"post-timeout",
		10*time.Second,
		"http timeout for post requests to the FDB sidecar.",
	)
	fs.DurationVar(
		&o.MaintenanceListStaleDuration,
		"maintenance-list-stale-duration",
		4*time.Hour,
		"the duration after stale entries will be deleted form the maintenance list. Only has an affect if the operator is allowed to reset the maintenance zone.",
	)
	fs.DurationVar(
		&o.MaintenanceListWaitDuration,
		"maintenance-list-wait-duration",
		5*time.Minute,
		"the duration where a process in the maintenance list in a different zone will be assumed to block the maintenance zone reset. Only has an affect if the operator is allowed to reset the maintenance zone.",
	)
	fs.DurationVar(
		&o.MinimumRequiredUptimeCCBounce,
		"minimum-required-uptime-for-cc-bounce",
		1*time.Hour,
		"the minimum required uptime of the cluster before allowing the operator to restart the CC if there is a failed tester process.",
	)
	fs.DurationVar(
		&o.GlobalSynchronizationWaitDuration,
		"global-synchronization-wait-duration",
		30*time.Second,
		"the wait time for the global synchronization mode in multi-region deployments",
	)
	fs.BoolVar(
		&o.ServerSideApply,
		"server-side-apply",
		false,
		"This flag enables server side apply.",
	)
	fs.BoolVar(
		&o.EnableRecoveryState,
		"enable-recovery-state",
		true,
		"This flag enables the use of the recovery state for the minimum uptime between bounced if the FDB version supports it.",
	)
	fs.BoolVar(
		&o.CacheDatabaseStatus,
		"cache-database-status",
		true,
		"Defines the default value for caching the database status.",
	)
	fs.BoolVar(
		&o.EnableNodeIndex,
		"enable-node-index",
		false,
		"Deprecated, not used anymore. Defines if the operator should add an index for accessing node objects. This requires a ClusterRoleBinding with node access. If the taint feature should be used, this setting should be set to true.",
	)
	fs.BoolVar(
		&o.ReplaceOnSecurityContextChange,
		"replace-on-security-context-change",
		false,
		"This flag enables the operator"+
			" to automatically replace pods whose effective security context has one of the following fields change: "+
			"FSGroup, FSGroupChangePolicy, RunAsGroup, RunAsUser",
	)
	fs.Float64Var(
		&o.MinimumRecoveryTimeForInclusion,
		"minimum-recovery-time-for-inclusion",
		600.0,
		"Defines the minimum uptime of the cluster before inclusions are allowed. For clusters after 7.1 this will use the recovery state. This should reduce the risk of frequent recoveries because of inclusions.",
	)
	fs.Float64Var(
		&o.MinimumRecoveryTimeForExclusion,
		"minimum-recovery-time-for-exclusion",
		120.0,
		"Defines the minimum uptime of the cluster before exclusions are allowed. For clusters after 7.1 this will use the recovery state. This should reduce the risk of frequent recoveries because of exclusions.",
	)
	fs.Float64Var(
		&o.HighRunLoopBusyThreshold,
		"high-run-loop-busy-threshold",
		math.MaxFloat64,
		"Defines the threshold when a process will be considered to have a high run loop busy value. The value will be between 0.0 and 1.0. Setting it to a higher value will disable the high run loop busy check.",
	)
	fs.StringVar(
		&o.WatchNamespace,
		"watch-namespace",
		os.Getenv("WATCH_NAMESPACE"),
		"Defines which namespace the operator should watch.",
	)
	fs.StringVar(
		&o.SaltMasterAddress,
		"salt-master-ip",
		"",
		"Defines the salt master IP or host:port for security updates.",
	)
	fs.StringVar(
		&o.SecurityUpdateConfigPath,
		"security-update-config-path",
		"",
		"Defines the cloud config path for security update reconciliation.",
	)
}

// StartManager will start the FoundationDB operator manager.
// Each reconciler that is not nil will be added to the list of reconcilers
// For all reconcilers the Client, Recorder and if appropriate the namespace will be set.
func StartManager(
	scheme *runtime.Scheme,
	operatorOpts Options,
	logOpts zap.Options,
	clusterReconciler *controllers.FoundationDBBaremetalClusterReconciler,
	logr logr.Logger) (manager.Manager, *os.File) {
	if operatorOpts.PrintVersion {
		fmt.Printf("version: %s\n", operatorVersion)
		os.Exit(0)
	}

	logWriter, err := setupLogger(operatorOpts)
	if err != nil {
		_, _ = fmt.Fprintf(
			os.Stderr,
			"unable to setup logger: %s, got error: %s\n",
			operatorOpts.LogFile,
			err.Error(),
		)
		os.Exit(1)
	}

	logger := zap.New(
		zap.UseFlagOptions(&logOpts),
		zap.WriteTo(logWriter))
	ctrl.SetLogger(logger)
	log.SetLogger(logger)

	klog.SetLogger(logger)

	setupLog := logger.WithName("setup")
	fdbclient.DefaultCLITimeout = time.Duration(operatorOpts.CliTimeout) * time.Second
	fdbclient.MaxCliTimeout = time.Duration(operatorOpts.MaxCliTimeout) * time.Second

	cacheOptions := cache.Options{}
	if operatorOpts.WatchNamespace != "" {
		setupLog.Info(
			"Operator starting in single namespace mode",
			"namespace",
			operatorOpts.WatchNamespace,
		)
		cacheOptions.DefaultNamespaces = map[string]cache.Config{
			operatorOpts.WatchNamespace: {},
		}
	} else {
		setupLog.Info("Operator starting in Global mode")
	}

	options := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: operatorOpts.MetricsAddr,
		},
		Cache:                  cacheOptions,
		HealthProbeBindAddress: operatorOpts.HealthProbeBindAddress,
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := moveFDBBinaries(setupLog); err != nil {
		setupLog.Error(err, "unable to move FDB binaries")
		os.Exit(1)
	}

	if clusterReconciler != nil {
		clusterReconciler.Client = mgr.GetClient()
		clusterReconciler.Recorder = mgr.GetEventRecorderFor("foundationdbbaremetalcluster-controller")
		clusterReconciler.GetTimeout = operatorOpts.GetTimeout
		clusterReconciler.PostTimeout = operatorOpts.PostTimeout

		clusterReconciler.Log = logr.WithName("controllers").WithName("FoundationDBBaremetalCluster")
		clusterReconciler.Scheme = mgr.GetScheme()
		clusterReconciler.Namespace = operatorOpts.WatchNamespace
		clusterReconciler.SaltMasterAddress = operatorOpts.SaltMasterAddress
		clusterReconciler.SecurityUpdateConfigPath = operatorOpts.SecurityUpdateConfigPath

		if err := clusterReconciler.SetupWithManager(mgr, operatorOpts.MaxConcurrentReconciles); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "FoundationDBBaremetalCluster")
			os.Exit(1)
		}
	}

	return mgr, nil
}

func moveFDBBinaries(log logr.Logger) error {
	binFile, err := os.Open(os.Getenv("FDB_BINARY_DIR"))
	if err != nil {
		if os.IsNotExist(err) {
			log.Info("No FDB binaries mounted")
			return nil
		}

		return err
	}
	defer func() {
		_ = binFile.Close()
	}()

	libDir, err := os.Open(os.Getenv(fdbv1beta2.EnvNameFDBExternalClientDir))

	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(
				os.Getenv(fdbv1beta2.EnvNameFDBExternalClientDir),
				os.ModeDir|os.ModePerm,
			)
			if err != nil {
				return err
			}
		}
	} else {
		return err
	}

	defer func() {
		_ = libDir.Close()
	}()

	binDir, err := binFile.Readdir(0)
	if err != nil {
		return err
	}

	for _, binEntry := range binDir {
		if !binEntry.IsDir() {
			continue
		}

		version, err := fdbv1beta2.ParseFdbVersion(binEntry.Name())
		if err != nil {
			log.Info("Could not parse version from directory name", "name", binEntry.Name())
			continue
		}

		versionBinFile, err := os.Open(
			path.Join(binFile.Name(), binEntry.Name(), "bin", binEntry.Name()),
		)
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		if err == nil {
			minorVersionPath := path.Join(binFile.Name(), version.GetBinaryVersion())
			err = os.MkdirAll(minorVersionPath, os.ModeDir|os.ModePerm)
			if err != nil {
				return err
			}

			versionBinDir, err := versionBinFile.Readdir(0)
			if err != nil {
				return err
			}
			for _, versionBinEntry := range versionBinDir {
				currentPath := path.Join(versionBinFile.Name(), versionBinEntry.Name())
				newPath := path.Join(minorVersionPath, versionBinEntry.Name())
				log.Info("Moving FDB binary file", "currentPath", currentPath, "newPath", newPath)
				err = os.Rename(currentPath, newPath)
				if err != nil {
					return err
				}
			}
		}
		if versionBinFile != nil {
			_ = versionBinFile.Close()
		}

		versionLibFile, err := os.Open(
			path.Join(binFile.Name(), binEntry.Name(), "lib", "libfdb_c.so"),
		)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil {
			currentPath := path.Join(versionLibFile.Name())
			newPath := path.Join(libDir.Name(), fmt.Sprintf("libfdb_c_%s.so", version))
			log.Info("Moving FDB library file", "currentPath", currentPath, "newPath", newPath)
			err = os.Rename(currentPath, newPath)
			if err != nil {
				return err
			}
		}
		if versionLibFile != nil {
			_ = versionLibFile.Close()
		}
	}

	return nil
}

// setupLogger will return a MultiWriter if the operator should log to a file and stdout otherwise only the stdout
// io.Writer is returned. If the operator should log to a file the operator will make sure to create the file with
// the expected permissions.
func setupLogger(operatorOpts Options) (io.Writer, error) {
	if operatorOpts.LogFile != "" {
		expectedPermission := fs.FileMode(0644)

		if operatorOpts.LogFilePermission != "" {
			expectedPermissionUnit, err := strconv.ParseUint(operatorOpts.LogFilePermission, 8, 32)
			if err != nil {
				return nil, err
			}

			expectedPermission = fs.FileMode(expectedPermissionUnit)
		}

		// We have to create the original file by ourself since lumberjack doesn't support to pass down the expected permissions
		// see: https://github.com/natefinch/lumberjack/issues/82#issuecomment-482143273.
		stat, err := os.Stat(operatorOpts.LogFile)
		// File doesn't exist and must be created with the expected permission.
		if os.IsNotExist(err) {
			err := os.WriteFile(operatorOpts.LogFile, nil, expectedPermission)
			if err != nil {
				return nil, err
			}
		}

		if err == nil && stat.Mode() != expectedPermission {
			err = os.Chmod(operatorOpts.LogFile, expectedPermission)
			if err != nil {
				return nil, err
			}
		}

		lumberjackLogger := &lumberjack.Logger{
			Filename:   operatorOpts.LogFile,
			MaxSize:    operatorOpts.LogFileMaxSize,
			MaxAge:     operatorOpts.LogFileMaxAge,
			MaxBackups: operatorOpts.MaxNumberOfOldLogFiles,
			Compress:   operatorOpts.CompressOldFiles,
		}

		return io.MultiWriter(os.Stdout, lumberjackLogger), nil
	}

	return os.Stdout, nil
}
