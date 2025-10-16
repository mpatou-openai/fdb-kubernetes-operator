/*
 * configmap_helper.go
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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	fdbv1beta2 "github.com/FoundationDB/fdb-kubernetes-operator/v2/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// GetConfigMap builds a config map for a cluster's dynamic config
func GetConfigMap(cluster *fdbv1beta2.FoundationDBCluster) (*corev1.ConfigMap, error) {
	data := make(map[string]string)

	connectionString := cluster.Status.ConnectionString
	data[fdbv1beta2.ClusterFileKey] = connectionString
	data[fdbv1beta2.RunningVersionKey] = cluster.Status.RunningVersion

	desiredCountStruct, err := cluster.GetProcessCountsWithDefaults()
	if err != nil {
		return nil, err
	}
	desiredCounts := desiredCountStruct.Map()

	for processClass, count := range desiredCounts {
		if count == 0 {
			continue
		}

		serversPerPodSlice := []int{1}

		for _, serversPerNode := range serversPerPodSlice {
			err := setMonitorConfForFilename(
				cluster,
				data,
				GetConfigMapMonitorConfEntry(
					processClass,
					fdbv1beta2.ImageTypeSplit,
					serversPerNode,
				),
				connectionString,
				processClass,
				serversPerNode,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	metadata := getConfigMapMetadata(cluster)
	metadata.OwnerReferences = BuildOwnerReference(cluster.TypeMeta, cluster.ObjectMeta)

	return &corev1.ConfigMap{
		ObjectMeta: metadata,
		Data:       data,
	}, nil
}

// GetJSONHash serializes an object to JSON and takes a hash of the resulting
// JSON.
func GetJSONHash(object interface{}) (string, error) {
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	err := encoder.Encode(object)
	if err != nil {
		return "", err
	}
	specHash := hash.Sum(make([]byte, 0))
	return hex.EncodeToString(specHash), nil
}

// BuildOwnerReference returns an OwnerReference for the provided input
func BuildOwnerReference(
	ownerType metav1.TypeMeta,
	ownerMetadata metav1.ObjectMeta,
) []metav1.OwnerReference {
	return []metav1.OwnerReference{{
		APIVersion: ownerType.APIVersion,
		Kind:       ownerType.Kind,
		Name:       ownerMetadata.Name,
		UID:        ownerMetadata.UID,
		Controller: ptr.To(true),
	}}
}

// GetObjectMetadata returns the ObjectMetadata for a process
func GetObjectMetadata(
	cluster *fdbv1beta2.FoundationDBCluster,
	base *metav1.ObjectMeta,
) metav1.ObjectMeta {
	var metadata *metav1.ObjectMeta

	if base != nil {
		metadata = base.DeepCopy()
	} else {
		metadata = &metav1.ObjectMeta{}
	}
	metadata.Namespace = cluster.Namespace

	if metadata.Labels == nil {
		metadata.Labels = make(map[string]string)
	}

	for label, value := range cluster.GetResourceLabels() {
		metadata.Labels[label] = value
	}

	return *metadata
}

func getConfigMapMetadata(cluster *fdbv1beta2.FoundationDBCluster) metav1.ObjectMeta {
	var metadata metav1.ObjectMeta
	metadata = GetObjectMetadata(cluster, nil)

	metadata.Name = getConfigMapName(cluster.Name)

	return metadata
}

func getConfigMapName(clusterName string) string {
	return fmt.Sprintf("%s-config", clusterName)
}

func getDataForMonitorConf(
	cluster *fdbv1beta2.FoundationDBCluster,
	imageType fdbv1beta2.ImageType,
	pClass fdbv1beta2.ProcessClass,
	serversPerPod int,
) (string, []byte, error) {
	config := GetMonitorProcessConfiguration(cluster, pClass, serversPerPod, imageType, nil)
	jsonData, err := json.Marshal(config)
	if err != nil {
		return "", nil, err
	}
	filename := GetConfigMapMonitorConfEntry(pClass, imageType, serversPerPod)
	return filename, jsonData, nil
}

func setMonitorConfForFilename(
	cluster *fdbv1beta2.FoundationDBCluster,
	data map[string]string,
	filename string,
	connectionString string,
	processClass fdbv1beta2.ProcessClass,
	serversPerPod int,
) error {
	if connectionString == "" {
		data[filename] = ""
	} else {
		conf, err := GetMonitorConf(cluster, processClass, serversPerPod, nil)
		if err != nil {
			return err
		}
		data[filename] = conf
	}

	return nil
}

// GetConfigMapMonitorConfEntry returns the specific key for the monitor conf in the ConfigMap
func GetConfigMapMonitorConfEntry(
	pClass fdbv1beta2.ProcessClass,
	imageType fdbv1beta2.ImageType,
	serversPerPod int,
) string {
	if imageType == fdbv1beta2.ImageTypeUnified {
		return fmt.Sprintf("fdbmonitor-conf-%s-json", pClass)
	}
	if serversPerPod > 1 {
		return fmt.Sprintf("fdbmonitor-conf-%s-density-%d", pClass, serversPerPod)
	}
	return fmt.Sprintf("fdbmonitor-conf-%s", pClass)
}

// GetDynamicConfHash gets a hash of the data from the config map holding the
// cluster's dynamic conf.
//
// This will omit keys that we do not expect the Pods to reference e.g. for storage Pods only include the storage config.
func GetDynamicConfHash(
	configMap *corev1.ConfigMap,
	pClass fdbv1beta2.ProcessClass,
	imageType fdbv1beta2.ImageType,
	serversPerPod int,
) (string, error) {
	fields := []string{
		fdbv1beta2.ClusterFileKey,
		GetConfigMapMonitorConfEntry(pClass, imageType, serversPerPod),
		fdbv1beta2.RunningVersionKey,
		fdbv1beta2.CaFileKey,
		fdbv1beta2.SidecarConfKey,
	}
	var data = make(map[string]string, len(fields))

	for _, field := range fields {
		if val, ok := configMap.Data[field]; ok {
			data[field] = val
		}
	}

	return GetJSONHash(data)
}
