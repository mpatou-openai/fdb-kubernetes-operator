/*
 * security_update_reconciler.go
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

package controllers

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/fdbclient"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal/securityupdate"
	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	securityUpdateInProgressPrefix = "security-updates/in-progress"
	securityUpdateCompletedPrefix  = "security-updates/completed"
	securityUpdateCooldown         = 10 * time.Minute
	securityUpdateRequeue          = 5 * time.Minute
)

type securityUpdateCandidate struct {
	cluster *fdbv1beta2.FoundationDBBaremetalCluster
	targets []securityupdate.MinionTarget
}

func (r *FoundationDBBaremetalClusterReconciler) reconcileSecurityUpdates(
	ctx context.Context,
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
	logger logr.Logger,
) (ctrl.Result, error) {
	if r.SaltMasterAddress == "" || r.SecurityUpdateConfigPath == "" {
		return ctrl.Result{}, nil
	}

	config, err := securityupdate.LoadCloudConfig(r.SecurityUpdateConfigPath)
	if err != nil {
		return ctrl.Result{}, err
	}

	authToken := os.Getenv("SALT_AUTH_TOKEN")
	saltClient := securityupdate.NewSaltClient(r.SaltMasterAddress, authToken, 0)

	candidates, err := r.collectSecurityUpdateCandidates(ctx, config, saltClient, logger)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(candidates) == 0 {
		return ctrl.Result{RequeueAfter: securityUpdateRequeue}, nil
	}

	selected := selectSecurityUpdateCandidate(candidates)
	if selected.cluster.Name != cluster.Name {
		return ctrl.Result{RequeueAfter: securityUpdateRequeue}, nil
	}

	if cluster.Status.ConnectionString == "" {
		logger.Info("Cluster has no connection string yet, skipping security updates")
		return ctrl.Result{RequeueAfter: securityUpdateRequeue}, nil
	}

	coordinationClient := fdbclient.NewCoordinationClient(cluster, logger, 0)
	if ok, err := hasRecentSecurityUpdate(coordinationClient, cluster.Name); err != nil {
		return ctrl.Result{}, err
	} else if ok {
		return ctrl.Result{RequeueAfter: securityUpdateRequeue}, nil
	}

	if err := coordinationClient.SetEntry(securityUpdateInProgressPrefix, cluster.Name, time.Now()); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.applySecurityUpdates(ctx, saltClient, selected.targets, logger); err != nil {
		_ = coordinationClient.ClearEntry(securityUpdateInProgressPrefix, cluster.Name)
		return ctrl.Result{}, err
	}

	if err := coordinationClient.ClearEntry(securityUpdateInProgressPrefix, cluster.Name); err != nil {
		return ctrl.Result{}, err
	}

	if err := coordinationClient.SetEntry(securityUpdateCompletedPrefix, cluster.Name, time.Now()); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: securityUpdateRequeue}, nil
}

func (r *FoundationDBBaremetalClusterReconciler) collectSecurityUpdateCandidates(
	ctx context.Context,
	config *securityupdate.CloudConfig,
	saltClient *securityupdate.SaltClient,
	logger logr.Logger,
) ([]securityUpdateCandidate, error) {
	clusters := &fdbv1beta2.FoundationDBBaremetalClusterList{}
	listOptions := make([]client.ListOption, 0, 1)
	if r.Namespace != "" {
		listOptions = append(listOptions, client.InNamespace(r.Namespace))
	}

	if err := r.List(ctx, clusters, listOptions...); err != nil {
		return nil, err
	}

	candidates := make([]securityUpdateCandidate, 0)
	for index := range clusters.Items {
		current := &clusters.Items[index]
		targets, err := r.securityUpdateTargets(ctx, current, config, saltClient, logger)
		if err != nil {
			return nil, err
		}
		if len(targets) == 0 {
			continue
		}

		candidates = append(candidates, securityUpdateCandidate{
			cluster: current,
			targets: targets,
		})
	}

	return candidates, nil
}

func (r *FoundationDBBaremetalClusterReconciler) securityUpdateTargets(
	ctx context.Context,
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
	config *securityupdate.CloudConfig,
	saltClient *securityupdate.SaltClient,
	logger logr.Logger,
) ([]securityupdate.MinionTarget, error) {
	targets, err := config.MinionTargets(cluster.Name)
	if err != nil {
		return nil, err
	}

	needsUpdate := make([]securityupdate.MinionTarget, 0, len(targets))
	for _, target := range targets {
		result, err := saltClient.NeedRestart(ctx, target.MinionName)
		if err != nil {
			return nil, fmt.Errorf("needrestart failed for %s: %w", target.MinionName, err)
		}

		if result.KernelNeedsRestart {
			needsUpdate = append(needsUpdate, target)
		}
	}

	if len(needsUpdate) == 0 {
		logger.V(1).Info("No kernel upgrades required", "cluster", cluster.Name)
	}

	return needsUpdate, nil
}

func selectSecurityUpdateCandidate(
	candidates []securityUpdateCandidate,
) securityUpdateCandidate {
	if len(candidates) == 1 {
		return candidates[0]
	}

	randSource := rand.New(rand.NewSource(time.Now().UnixNano()))
	return candidates[randSource.Intn(len(candidates))]
}

func hasRecentSecurityUpdate(
	coordinationClient *fdbclient.CoordinationClient,
	clusterName string,
) (bool, error) {
	inProgress, err := coordinationClient.GetEntries(securityUpdateInProgressPrefix)
	if err != nil {
		return false, err
	}

	if _, ok := inProgress[fdbv1beta2.ProcessGroupID(clusterName)]; ok {
		return true, nil
	}

	completed, err := coordinationClient.GetEntries(securityUpdateCompletedPrefix)
	if err != nil {
		return false, err
	}

	if timestamp, ok := completed[fdbv1beta2.ProcessGroupID(clusterName)]; ok {
		if time.Since(timestamp) < securityUpdateCooldown {
			return true, nil
		}
	}

	return false, nil
}

func (r *FoundationDBBaremetalClusterReconciler) applySecurityUpdates(
	ctx context.Context,
	saltClient *securityupdate.SaltClient,
	targets []securityupdate.MinionTarget,
	logger logr.Logger,
) error {
	byClass := map[string][]securityupdate.MinionTarget{}
	for _, target := range targets {
		class := normalizeSecurityClass(target.ProcessClass)
		byClass[class] = append(byClass[class], target)
	}

	classKeys := make([]string, 0, len(byClass))
	for class := range byClass {
		classKeys = append(classKeys, class)
	}
	sort.Strings(classKeys)

	for _, class := range classKeys {
		targetsForClass := byClass[class]
		sort.Slice(targetsForClass, func(i, j int) bool {
			return targetsForClass[i].MinionName < targetsForClass[j].MinionName
		})

		switch class {
		case "cd", "stg":
			for _, target := range targetsForClass {
				logger.Info(
					"Rebooting minion for kernel update",
					"minion",
					target.MinionName,
					"class",
					class,
				)
				if err := saltClient.RebootMinion(ctx, target.MinionName); err != nil {
					return fmt.Errorf("reboot failed for %s: %w", target.MinionName, err)
				}
			}
		case "log", "stless":
			logger.Info(
				"Skipping kernel update for process class pending automation details",
				"class",
				class,
				"minionCount",
				len(targetsForClass),
			)
		default:
			logger.Info(
				"Skipping kernel update for unknown process class",
				"class",
				class,
				"minionCount",
				len(targetsForClass),
			)
		}
	}

	return nil
}

func normalizeSecurityClass(processClass string) string {
	normalized := strings.ToLower(strings.TrimSpace(processClass))
	switch normalized {
	case "storage", "stg":
		return "stg"
	case "cluster_controller", "cluster-controller", "cd":
		return "cd"
	case "stateless", "stless":
		return "stless"
	case "log":
		return "log"
	default:
		return normalized
	}
}
