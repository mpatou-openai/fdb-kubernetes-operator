/*
 * configmap_helper_test.go
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

package internal

import (
	"encoding/json"
	"fmt"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	monitorapi "github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("configmap_helper", func() {
	var cluster *fdbv1beta2.FoundationDBBaremetalCluster
	var fakeConnectionString string
	var err error

	BeforeEach(func() {
		cluster = CreateDefaultCluster()
		err = NormalizeClusterSpec(cluster)
		Expect(err).NotTo(HaveOccurred())
		fakeConnectionString = "operator-test:asdfasf@127.0.0.1:4501"
	})

	Describe("GetConfigMap", func() {
		var configMap *corev1.ConfigMap
		var err error

		BeforeEach(func() {
			cluster.Status.ConnectionString = fakeConnectionString
			cluster.Status.RunningVersion = cluster.Spec.Version
			err = NormalizeClusterSpec(cluster)
			Expect(err).NotTo(HaveOccurred())
		})

		JustBeforeEach(func() {
			configMap, err = GetConfigMap(cluster)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("with a basic cluster", func() {
			It("should populate the metadata", func() {
				Expect(configMap.Namespace).To(Equal("my-ns"))
				Expect(configMap.Name).To(Equal(fmt.Sprintf("%s-config", cluster.Name)))
				Expect(configMap.Labels).To(Equal(map[string]string{
					fdbv1beta2.FDBClusterLabel: cluster.Name,
				}))
				Expect(configMap.Annotations).To(BeNil())
			})

			It("should have the basic files", func() {
				expectedConf, err := GetMonitorConf(
					cluster,
					fdbv1beta2.ProcessClassStorage,
					1,
					nil,
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(
					configMap.Data[fdbv1beta2.ClusterFileKey],
				).To(Equal("operator-test:asdfasf@127.0.0.1:4501"))
				Expect(configMap.Data["fdbmonitor-conf-storage"]).To(Equal(expectedConf))
				Expect(
					configMap.Data[fdbv1beta2.RunningVersionKey],
				).To(Equal(fdbv1beta2.Versions.Default.String()))
				Expect(configMap.Data[fdbv1beta2.SidecarConfKey]).To(Equal(""))
			})
		})

		When("only the unified image is enabled", func() {

			It("includes the data for the unified monitor conf", func() {
				jsonData, present := configMap.Data["fdbmonitor-conf-storage-json"]
				Expect(present).To(BeTrue())
				config := monitorapi.ProcessConfiguration{}
				err = json.Unmarshal([]byte(jsonData), &config)
				Expect(err).NotTo(HaveOccurred())
				expectedConfig := GetMonitorProcessConfiguration(
					cluster,
					fdbv1beta2.ProcessClassStorage,
					1,
					fdbv1beta2.ImageTypeUnified,
					nil,
				)
				Expect(config).To(Equal(expectedConfig))
			})

			It("does not include the data for the split monitor conf", func() {
				_, present := configMap.Data["fdbmonitor-conf-storage"]
				Expect(present).To(BeFalse())
			})
		})

		When("only the split image is enabled", func() {
			It("includes the data for the split monitor conf", func() {
				expectedConf, err := GetMonitorConf(
					cluster,
					fdbv1beta2.ProcessClassStorage,
					1,
					nil,
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(configMap.Data["fdbmonitor-conf-storage"]).To(Equal(expectedConf))
			})

			It("does not include the data for the unified monitor conf", func() {
				_, present := configMap.Data["fdbmonitor-conf-storage-json"]
				Expect(present).To(BeFalse())
			})
		})

		When("both image types are enabled", func() {
			It("includes the data for the both images", func() {
				_, present := configMap.Data["fdbmonitor-conf-storage-json"]
				Expect(present).To(BeTrue())
				_, present = configMap.Data["fdbmonitor-conf-storage"]
				Expect(present).To(BeTrue())
			})
		})

		Context("with multiple storage servers per disk", func() {
			When("using the split image", func() {
				It("includes the data for both configurations", func() {
					expectedConf, err := GetMonitorConf(
						cluster,
						fdbv1beta2.ProcessClassStorage,
						1,
						nil,
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(configMap.Data["fdbmonitor-conf-storage"]).To(Equal(expectedConf))

					expectedConf, err = GetMonitorConf(
						cluster,
						fdbv1beta2.ProcessClassStorage,
						2,
						nil,
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(
						configMap.Data["fdbmonitor-conf-storage-density-2"],
					).To(Equal(expectedConf))
				})
			})

			When("using the unified image", func() {

				It("includes the data for both configurations", func() {
					jsonData, present := configMap.Data["fdbmonitor-conf-storage-json"]
					Expect(present).To(BeTrue())
					config := monitorapi.ProcessConfiguration{}
					err = json.Unmarshal([]byte(jsonData), &config)
					Expect(err).NotTo(HaveOccurred())
					expectedConfig := GetMonitorProcessConfiguration(
						cluster,
						fdbv1beta2.ProcessClassStorage,
						1,
						fdbv1beta2.ImageTypeUnified,
						nil,
					)
					Expect(config).To(Equal(expectedConfig))

					jsonData, present = configMap.Data["fdbmonitor-conf-storage-json"]
					Expect(present).To(BeTrue())
					config = monitorapi.ProcessConfiguration{}
					err = json.Unmarshal([]byte(jsonData), &config)
					Expect(err).NotTo(HaveOccurred())
					expectedConfig = GetMonitorProcessConfiguration(
						cluster,
						fdbv1beta2.ProcessClassStorage,
						2,
						fdbv1beta2.ImageTypeUnified,
						nil,
					)
					Expect(config).To(Equal(expectedConfig))
				})
			})
		})

		Context("with multiple log servers per disk", func() {

			When("using the split image", func() {
				It("includes the data for both configurations", func() {
					expectedConf, err := GetMonitorConf(
						cluster,
						fdbv1beta2.ProcessClassLog,
						1,
						nil,
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(configMap.Data["fdbmonitor-conf-log"]).To(Equal(expectedConf))

					expectedConf, err = GetMonitorConf(
						cluster,
						fdbv1beta2.ProcessClassLog,
						2,
						nil,
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(configMap.Data["fdbmonitor-conf-log-density-2"]).To(Equal(expectedConf))
				})
			})

			When("using the unified image", func() {

				It("includes the data for both configurations", func() {
					jsonData, present := configMap.Data["fdbmonitor-conf-log-json"]
					Expect(present).To(BeTrue())
					config := monitorapi.ProcessConfiguration{}
					err = json.Unmarshal([]byte(jsonData), &config)
					Expect(err).NotTo(HaveOccurred())
					expectedConfig := GetMonitorProcessConfiguration(
						cluster,
						fdbv1beta2.ProcessClassLog,
						1,
						fdbv1beta2.ImageTypeUnified,
						nil,
					)
					Expect(config).To(Equal(expectedConfig))

					jsonData, present = configMap.Data["fdbmonitor-conf-log-json"]
					Expect(present).To(BeTrue())
					config = monitorapi.ProcessConfiguration{}
					err = json.Unmarshal([]byte(jsonData), &config)
					Expect(err).NotTo(HaveOccurred())
					expectedConfig = GetMonitorProcessConfiguration(
						cluster,
						fdbv1beta2.ProcessClassLog,
						2,
						fdbv1beta2.ImageTypeUnified,
						nil,
					)
					Expect(config).To(Equal(expectedConfig))
				})

			})
		})

		Context("with custom resource labels", func() {
			It("should populate the metadata", func() {
				Expect(configMap.Namespace).To(Equal("my-ns"))
				Expect(configMap.Name).To(Equal(fmt.Sprintf("%s-config", cluster.Name)))
				Expect(configMap.Labels).To(Equal(map[string]string{
					"fdb-custom-name":         cluster.Name,
					"fdb-new-custom-name":     cluster.Name,
					"fdb-managed-by-operator": "true",
				}))
				Expect(configMap.Annotations).To(BeNil())
			})
		})

		Context("with an empty connection string", func() {
			BeforeEach(func() {
				cluster.Status.ConnectionString = ""
			})

			It("should empty the monitor conf and cluster file", func() {
				Expect(configMap.Data[fdbv1beta2.ClusterFileKey]).To(Equal(""))
				Expect(configMap.Data["fdbmonitor-conf-storage"]).To(Equal(""))
			})
		})

		Context("with a custom label", func() {

			It("should put the label on the config map", func() {
				Expect(configMap.Labels).To(Equal(map[string]string{
					fdbv1beta2.FDBClusterLabel: cluster.Name,
					"fdb-label":                "value1",
				}))
			})
		})

		It("should put the annotation on the config map", func() {
			Expect(configMap.Annotations).To(Equal(map[string]string{
				"fdb-annotation": "value1",
			}))
		})
	})

})
