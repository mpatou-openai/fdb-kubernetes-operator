/*
 * needrestart.go
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

package securityupdate

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	needRestartKernelPattern = regexp.MustCompile(`NEEDRESTART-KSTA:\\s*(\\d+)`)
)

func kernelUpgradeRequired(output string) bool {
	if output == "" {
		return false
	}

	match := needRestartKernelPattern.FindStringSubmatch(output)
	if len(match) > 1 {
		value, err := strconv.Atoi(match[1])
		if err == nil {
			return value != 0
		}
	}

	return strings.Contains(strings.ToLower(output), "kernel")
}
