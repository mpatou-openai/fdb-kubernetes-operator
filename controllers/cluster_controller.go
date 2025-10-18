/*
 * cluster_controller.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020-2021 Apple Inc. and the FoundationDB project authors
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
	"time"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

// SimulationOptions are used to define how the cluster reconciler should behave in the unit tests.
type SimulationOptions struct {
	// SimulateZones if enabled the zone of pods will be replaced with a "simulation" zone.
	SimulateZones bool
	// SimulateTime is enabled the time checks in the exclude and update pods reconciler will be skipped.
	SimulateTime bool
}

type FoundationDBBaremetalClusterReconciler struct {
	GetTimeout  time.Duration
	PostTimeout time.Duration

	client.Client
	Scheme            *runtime.Scheme
	Log               logr.Logger
	SimulationOptions SimulationOptions
	Recorder          record.EventRecorder
	Namespace         string
}

// NewFoundationDBBaremetalClusterReconciler constructs a reconciler instance.
func NewFoundationDBBaremetalClusterReconciler() *FoundationDBBaremetalClusterReconciler {
	return &FoundationDBBaremetalClusterReconciler{}
}

// Reconcile currently contains placeholder logic for the bare-metal operator.
func (r *FoundationDBBaremetalClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cluster := &fdbv1beta2.FoundationDBBaremetalCluster{}

	err := r.Get(ctx, req.NamespacedName, cluster)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return ctrl.Result{}, err
	}

	clusterLog := globalControllerLogger.WithValues(
		"namespace",
		cluster.Namespace,
		"cluster",
		cluster.Name,
		"traceID",
		uuid.NewUUID(),
	)
	startTime := time.Now()
	defer func() {
		clusterLog.Info(
			"Reconciliation run finished",
			"duration_seconds",
			time.Since(startTime).Seconds(),
		)
	}()
	return ctrl.Result{}, nil
}

// SetupWithManager configures the reconciler with the controller-runtime manager.
func (r *FoundationDBBaremetalClusterReconciler) SetupWithManager(mgr ctrl.Manager, maxConcurrentReconciles int) error {
	builder := ctrl.NewControllerManagedBy(mgr).
		For(&fdbv1beta2.FoundationDBBaremetalCluster{}).
		Owns(&corev1.ConfigMap{})

	return builder.WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentReconciles}).Complete(r)
}
