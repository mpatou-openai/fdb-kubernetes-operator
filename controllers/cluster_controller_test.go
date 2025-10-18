/*
 * cluster_controller_test.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2018-2019 Apple Inc. and the FoundationDB project authors
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
	"sort"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/internal"
	"github.com/FoundationDB/fdb-kubernetes-operator/v2/pkg/fdbadminclient/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var firstStorageIndex = 13

// reloadCluster reloads FoundationDBBaremetalCluster object info to its local copy.
// It's necessary in unit test when reconciler changes FoundationDBBaremetalCluster object but the local's state is not updated.
// We reloadCluster to bring client state consistent with the latest FoundationDBBaremetalCluster object
func reloadCluster(cluster *fdbv1beta2.FoundationDBBaremetalCluster) (int64, error) {
	generations, err := reloadClusterGenerations(cluster)
	if generations.HasPendingRemoval > 0 {
		return 0, err
	}
	return generations.Reconciled, err
}

func reloadClusterGenerations(
	cluster *fdbv1beta2.FoundationDBBaremetalCluster,
) (fdbv1beta2.ClusterGenerationStatus, error) {
	key := client.ObjectKeyFromObject(cluster)
	*cluster = fdbv1beta2.FoundationDBBaremetalCluster{}
	err := k8sClient.Get(context.TODO(), key, cluster)
	if err != nil {
		return fdbv1beta2.ClusterGenerationStatus{}, err
	}
	return cluster.Status.Generations, err
}

func getListOptions(cluster *fdbv1beta2.FoundationDBBaremetalCluster) []client.ListOption {
	return []client.ListOption{
		client.InNamespace("my-ns"),
		client.MatchingLabels(map[string]string{
			fdbv1beta2.FDBClusterLabel: cluster.Name,
		}),
	}
}

func sortPodsByName(pods *corev1.PodList) {
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].ObjectMeta.Name < pods.Items[j].ObjectMeta.Name
	})
}

var _ = Describe("cluster_controller", func() {
	var cluster *fdbv1beta2.FoundationDBBaremetalCluster

	BeforeEach(func() {
		cluster = internal.CreateDefaultCluster()
	})

	Describe("Reconciliation", func() {
		var originalPods *corev1.PodList
		var originalVersion int64
		var err error
		var generationGap int64
		var shouldCompleteReconciliation bool

		BeforeEach(func() {
			Expect(setupClusterForTest(cluster)).To(Succeed())
			Expect(cluster.Generation).To(Equal(int64(1)))
			originalVersion = cluster.ObjectMeta.Generation

			originalPods = &corev1.PodList{}
			err = k8sClient.List(context.TODO(), originalPods, getListOptions(cluster)...)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(originalPods.Items)).To(Equal(17))

			sortPodsByName(originalPods)

			generationGap = 1
			shouldCompleteReconciliation = true
		})

		JustBeforeEach(func() {
			result, err := reconcileCluster(cluster)
			if err != nil && !shouldCompleteReconciliation {
				return
			}

			Expect(err).NotTo(HaveOccurred())
			if shouldCompleteReconciliation {
				Expect(result.RequeueAfter).To(BeZero())
			} else {
				Expect(result.RequeueAfter).NotTo(BeZero())
			}

			if shouldCompleteReconciliation {
				generation, err := reloadCluster(cluster)
				Expect(err).NotTo(HaveOccurred())
				Expect(generation).To(Equal(originalVersion + generationGap))
			}
		})

		AfterEach(func() {
			k8sClient.Clear()
		})

		Context("when reconciling a new cluster", func() {
			BeforeEach(func() {
				generationGap = 0
			})

			It("should create an headless service", func() {
				services := &corev1.ServiceList{}
				Expect(
					k8sClient.List(context.TODO(), services, getListOptions(cluster)...),
				).NotTo(HaveOccurred())
				Expect(services.Items).To(HaveLen(1))
			})

			It("should fill in the required fields in the configuration", func() {
				Expect(
					cluster.Status.DatabaseConfiguration.RedundancyMode,
				).To(Equal(fdbv1beta2.RedundancyModeDouble))
				Expect(
					cluster.Status.DatabaseConfiguration.StorageEngine,
				).To(Equal(fdbv1beta2.StorageEngineSSD2))
				Expect(cluster.Status.ConnectionString).NotTo(Equal(""))
			})

			It("should create a config map for the cluster", func() {
				configMap := &corev1.ConfigMap{}
				configMapName := types.NamespacedName{
					Namespace: "my-ns",
					Name:      fmt.Sprintf("%s-config", cluster.Name),
				}
				err = k8sClient.Get(context.TODO(), configMapName, configMap)
				Expect(err).NotTo(HaveOccurred())
				expectedConfigMap, _ := internal.GetConfigMap(cluster)
				Expect(configMap.Data).To(Equal(expectedConfigMap.Data))
			})

			It("should send the configuration to the cluster", func() {
				adminClient, err := mock.NewMockAdminClientUncast(cluster, k8sClient)
				Expect(err).NotTo(HaveOccurred())
				Expect(adminClient).NotTo(BeNil())
				Expect(
					adminClient.DatabaseConfiguration.RedundancyMode,
				).To(Equal(fdbv1beta2.RedundancyModeDouble))
				Expect(
					adminClient.DatabaseConfiguration.StorageEngine,
				).To(Equal(fdbv1beta2.StorageEngineSSD2))
				Expect(adminClient.DatabaseConfiguration.RoleCounts).To(Equal(fdbv1beta2.RoleCounts{
					Logs:          3,
					Proxies:       3,
					CommitProxies: 0,
					GrvProxies:    0,
					Resolvers:     1,
					RemoteLogs:    -1,
					LogRouters:    -1,
				}))
			})

			It("should send the configuration to the cluster", func() {
				adminClient, err := mock.NewMockAdminClientUncast(cluster, k8sClient)
				Expect(err).NotTo(HaveOccurred())
				Expect(adminClient).NotTo(BeNil())
				Expect(
					adminClient.DatabaseConfiguration.RedundancyMode,
				).To(Equal(fdbv1beta2.RedundancyModeDouble))
				Expect(
					adminClient.DatabaseConfiguration.StorageEngine,
				).To(Equal(fdbv1beta2.StorageEngineSSD2))
				Expect(adminClient.DatabaseConfiguration.RoleCounts).To(Equal(fdbv1beta2.RoleCounts{
					Logs:       3,
					Proxies:    3,
					Resolvers:  1,
					RemoteLogs: -1,
					LogRouters: -1,
				}))
			})
		})
	})

	Describe("GetPublicIPs", func() {
		// var pod *corev1.Pod

		// BeforeEach(func() {
		// 	err := internal.NormalizeClusterSpec(cluster, internal.DeprecationOptions{})
		// 	Expect(err).NotTo(HaveOccurred())
		// })

		// Context("with a default pod", func() {
		// 	BeforeEach(func() {
		// 		var err error
		// 		pod, err = internal.GetPod(cluster, &fdbv1beta2.ProcessGroupStatus{
		// 			ProcessClass:   fdbv1beta2.ProcessClassStorage,
		// 			ProcessGroupID: "storage-1",
		// 		})
		// 		Expect(err).NotTo(HaveOccurred())
		// 		pod.Status.PodIP = "1.1.1.1"
		// 		pod.Status.PodIPs = []corev1.PodIP{
		// 			{IP: "1.1.1.2"},
		// 			{IP: "2001:db8::ff00:42:8329"},
		// 		}
		// 	})

		// 	It("should be the public IP from the pod", func() {
		// 		result := podmanager.GetPublicIPs(pod, globalControllerLogger)
		// 		Expect(result).To(Equal([]string{"1.1.1.1"}))
		// 	})
		// })

		// Context("with a v6 pod IP family configured", func() {
		// 	BeforeEach(func() {
		// 		var err error
		// 		cluster.Spec.Routing.PodIPFamily = ptr.To(6)
		// 		pod, err = internal.GetPod(cluster, &fdbv1beta2.ProcessGroupStatus{
		// 			ProcessClass:   fdbv1beta2.ProcessClassStorage,
		// 			ProcessGroupID: "storage-1",
		// 		})
		// 		Expect(err).NotTo(HaveOccurred())
		// 		pod.Status.PodIP = "1.1.1.1"
		// 		pod.Status.PodIPs = []corev1.PodIP{
		// 			{IP: "1.1.1.2"},
		// 			{IP: "2001:db8::ff00:42:8329"},
		// 		}
		// 	})

		// 	It("should select the address based on the spec", func() {
		// 		result := podmanager.GetPublicIPs(pod, globalControllerLogger)
		// 		Expect(result).To(Equal([]string{"2001:db8::ff00:42:8329"}))
		// 	})

		// FIXME: Re-enable once we support it
		/*
			Context("with no matching IPs in the Pod IP list", func() {
				BeforeEach(func() {
					var err error
					pod, err = internal.GetPod(cluster, &fdbv1beta2.ProcessGroupStatus{
						ProcessClass:   fdbv1beta2.ProcessClassStorage,
						ProcessGroupID: "storage-1",
					})
					Expect(err).NotTo(HaveOccurred())
					pod.Status.PodIPs = []corev1.PodIP{
						{IP: "1.1.1.2"},
					}
				})

				It("should be empty", func() {
					result := podmanager.GetPublicIPs(pod, globalControllerLogger)
					Expect(result).To(BeEmpty())
				})
			})
		*/
	})

	// FIXME: Re-enable once we have proper support for getting IP from a node
	// Context("with a v4 pod IP family configured", func() {
	// 	BeforeEach(func() {
	// 		var err error
	// 		cluster.Spec.Routing.PodIPFamily = ptr.To(4)
	// 		pod, err = internal.GetPod(cluster, &fdbv1beta2.ProcessGroupStatus{
	// 			ProcessClass:   fdbv1beta2.ProcessClassStorage,
	// 			ProcessGroupID: "storage-1",
	// 		})
	// 		Expect(err).NotTo(HaveOccurred())
	// 		pod.Status.PodIP = "1.1.1.2"
	// 		pod.Status.PodIPs = []corev1.PodIP{
	// 			{IP: "1.1.1.2"},
	// 			{IP: "2001:db8::ff00:42:8329"},
	// 		}
	// 	})

	// 	It("should select the address based on the spec", func() {
	// 		result := podmanager.GetPublicIPs(pod, globalControllerLogger)
	// 		Expect(result).To(Equal([]string{"1.1.1.2"}))
	// 	})
	// })

	// Context("with no pod", func() {
	// 	It("should be empty", func() {
	// 		result := podmanager.GetPublicIPs(nil, globalControllerLogger)
	// 		Expect(result).To(BeEmpty())
	// 	})
})
