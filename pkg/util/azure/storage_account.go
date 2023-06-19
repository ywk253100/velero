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

package azure

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	_ "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/pkg/errors"
)

const (
	// the keys of Azure BSL config:
	// https://github.com/vmware-tanzu/velero-plugin-for-microsoft-azure/blob/main/backupstoragelocation.md
	BSLConfigResourceGroup               = "resourceGroup"
	BSLConfigStorageAccount              = "storageAccount"
	BSLConfigStorageAccountAccessKeyName = "storageAccountKeyEnvVar"
	BSLConfigSubscriptionID              = "subscriptionId"
	BSLConfigStorageAccountURI           = "storageAccountURI"
	BSLConfigUseAAD                      = "useAAD"

	serviceNameBlob cloud.ServiceName = "blob"
)

func init() {
	cloud.AzureChina.Services[serviceNameBlob] = cloud.ServiceConfiguration{
		Endpoint: "blob.core.chinacloudapi.cn",
	}
	cloud.AzureGovernment.Services[serviceNameBlob] = cloud.ServiceConfiguration{
		Endpoint: "blob.core.usgovcloudapi.net",
	}
	cloud.AzurePublic.Services[serviceNameBlob] = cloud.ServiceConfiguration{
		Endpoint: "blob.core.windows.net/",
	}
}

// GetStorageAccountCredentials returns the credentials to interactive with storage account according to the config of BSL
// and credential file by the following order:
// 1. Return the storage account access key direclty if it is provided
// 2. Return the content of the credential file directly if "userAAD" is set in BSL config
// 3. Call Azure API to get the storage account access key

// TODO remove the userAAD param and read it from BSL config when Kopia support AAD and Restic is removed. Also update the related code in Azure plugin
func GetStorageAccountCredentials(bslCfg map[string]string, credFile string, useAAD bool) (map[string]string, error) {
	creds, err := LoadCredentials(credFile)
	if err != nil {
		return nil, err
	}

	// use storage account access key if specified
	if name := bslCfg[BSLConfigStorageAccountAccessKeyName]; name != "" {
		accessKey := creds[name]
		if accessKey == "" {
			return nil, errors.Errorf("no storage account access key with key %s found", name)
		}
		return map[string]string{CredentialKeyStorageAccountAccessKey: accessKey}, nil
	}

	/*
		TODO uncomment this block when Kopia support AAD and Restic is removed
		useAAD, err := strconv.ParseBool(bslCfg[udmrepo.StoreOptionAzureUseAAD])
		if err != nil {
			return nil, errors.Errorf("failed to parse bool for useAAD string: %s", bslCfg[udmrepo.StoreOptionAzureUseAAD])
		}
	*/

	// use AAD
	if useAAD {
		return creds, nil
	}

	// get the storage key
	key, err := getStorageAccountAccessKey(bslCfg, creds)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get storage account access key")
	}
	return map[string]string{CredentialKeyStorageAccountAccessKey: key}, nil
}

// GetStorageAccountURI returns the storage account URI by following order:
// 1. Return the storage account URI directly if it is specified in BSL config
// 2. Try to call Azure API to get the storage account URI if possible(Backgroud: https://github.com/vmware-tanzu/velero/issues/6163)
// 3. Fall back to return the default URI

// TODO https://github.com/vmware-tanzu/velero-plugin-for-microsoft-azure/pull/195/files
func GetStorageAccountURI(bslCfg map[string]string, credFile string) (string, error) {
	// if the URI is specified in the BSL, return it directly
	endpoint := bslCfg[BSLConfigStorageAccountURI]
	if endpoint != "" {
		return endpoint, nil
	}

	creds, err := LoadCredentials(credFile)
	if err != nil {
		return "", err
	}

	storageAccount := bslCfg[BSLConfigStorageAccount]
	if storageAccount == "" {
		return "", errors.New("storageAccount is required in the BSL")
	}

	cloudCfg, err := GetCloudConfiguration(creds[CredentialKeyCloudName])
	if err != nil {
		return "", err
	}

	uri := fmt.Sprintf("https:%s.%s", storageAccount, cloudCfg.Services[serviceNameBlob])

	// if storage account access key provided, the credential cannot be used to get the storage account properties,
	// so fallback to the default URI
	if name := bslCfg[BSLConfigStorageAccountAccessKeyName]; name != "" && creds[name] != "" {
		return uri, nil
	}

	client, err := newStorageAccountClient(bslCfg, creds)
	if err != nil {
		return "", err
	}

	resourceGroup := GetFromLocationConfigOrCredential(bslCfg, creds, BSLConfigResourceGroup, CredentialKeyResourceGroup)
	// we cannot get the storage account properties without the resource group, so fallback to the default URI
	if resourceGroup == "" {
		return uri, nil
	}

	properties, err := client.GetProperties(context.Background(), resourceGroup, storageAccount, nil)
	// get error, fallback to the default URI
	if err != nil {
		return uri, nil
	}

	return *properties.Account.Properties.PrimaryEndpoints.Blob, nil
}

// try to get the storage account access key with the provided credentials
func getStorageAccountAccessKey(bslCfg, creds map[string]string) (string, error) {
	client, err := newStorageAccountClient(bslCfg, creds)
	if err != nil {
		return "", err
	}

	resourceGroup := GetFromLocationConfigOrCredential(bslCfg, creds, BSLConfigResourceGroup, CredentialKeyResourceGroup)
	if resourceGroup == "" {
		return "", errors.New("resourceGroup is required")
	}
	storageAccount := bslCfg[BSLConfigStorageAccount]
	if storageAccount == "" {
		return "", errors.New("storageAccount is required in the BSL")
	}

	expand := "kerb"
	resp, err := client.ListKeys(context.Background(), resourceGroup, storageAccount, &armstorage.AccountsClientListKeysOptions{
		Expand: &expand,
	})
	if err != nil {
		return "", errors.Wrap(err, "failed to list storage account access keys")
	}
	for _, key := range resp.Keys {
		if key == nil || key.Permissions == nil {
			continue
		}
		if strings.EqualFold(string(*key.Permissions), string(armstorage.KeyPermissionFull)) {
			return *key.Value, nil
		}
	}
	return "", errors.New("no storage key with Full permissions found")
}

func newStorageAccountClient(bslCfg map[string]string, creds map[string]string) (*armstorage.AccountsClient, error) {
	clientOptions, err := GetClientOptions(creds[CredentialKeyCloudName])
	if err != nil {
		return nil, err
	}

	cred, err := NewCredential(creds, clientOptions)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create Azure credential")
	}

	subID := GetFromLocationConfigOrCredential(bslCfg, creds, BSLConfigSubscriptionID, CredentialKeySubscriptionID)
	if subID == "" {
		return nil, errors.New("subscription ID is required")
	}

	client, err := armstorage.NewAccountsClient(subID, cred, &arm.ClientOptions{
		ClientOptions: clientOptions,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create storage account client")
	}

	return client, nil
}
