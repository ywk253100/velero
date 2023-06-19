/*
Copyright the Velero contributors.

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

package config

import (
	"github.com/pkg/errors"

	"github.com/vmware-tanzu/velero/pkg/util/azure"
)

// GetAzureResticEnvVars gets the environment variables that restic
// relies on (AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY) based
// on info in the provided object storage location config map.
func GetAzureResticEnvVars(bslCfg map[string]string, credFile string) (map[string]string, error) {
	storageAccount := bslCfg[azure.BSLConfigStorageAccount]
	if storageAccount == "" {
		return nil, errors.New("storageAccount is required in the BSL")
	}

	credentials, err := azure.GetStorageAccountCredentials(bslCfg, credFile, false)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"AZURE_ACCOUNT_NAME": storageAccount,
		"AZURE_ACCOUNT_KEY":  credentials[azure.CredentialKeyStorageAccountAccessKey],
	}, nil
}
