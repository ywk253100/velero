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
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

const (
	// the keys of Azure variables in credential
	CredentialKeySubscriptionID             = "AZURE_SUBSCRIPTION_ID"
	CredentialKeyResourceGroup              = "AZURE_RESOURCE_GROUP"
	CredentialKeyCloudName                  = "AZURE_CLOUD_NAME"
	CredentialKeyStorageAccountAccessKey    = "AZURE_STORAGE_KEY"
	CredentialKeyAdditionallyAllowedTenants = "AZURE_ADDITIONALLY_ALLOWED_TENANTS"
	CredentialKeyTenantID                   = "AZURE_TENANT_ID"
	CredentialKeyClientID                   = "AZURE_CLIENT_ID"
	CredentialKeyClientSecret               = "AZURE_CLIENT_SECRET"
	CredentialKeyClientCertificatePath      = "AZURE_CLIENT_CERTIFICATE_PATH"
	CredentialKeyClientCertificatePassword  = "AZURE_CLIENT_CERTIFICATE_PASSWORD"
	CredentialKeySendCertChain              = "AZURE_CLIENT_SEND_CERTIFICATE_CHAIN"
	CredentialKeyUsername                   = "AZURE_USERNAME"
	CredentialKeyPassword                   = "AZURE_PASSWORD"
)

// LoadCredentials loads the credential file into a map
func LoadCredentials(credFile string) (map[string]string, error) {
	// if the credential isn't specified in the BSL spec, use the default credential
	if credFile == "" {
		credFile = os.Getenv("AZURE_CREDENTIALS_FILE")
	}
	// read the credentials from file and put into a map
	creds, err := godotenv.Read(credFile)
	if err != nil {
		return nil, errors.Errorf("failed to read credentials from file %s", credFile)
	}
	return creds, nil
}

// GetClientOptions returns the client options based on the cloud name
func GetClientOptions(cloudName string) (policy.ClientOptions, error) {
	cloudCfg, err := GetCloudConfiguration(cloudName)
	if err != nil {
		return policy.ClientOptions{}, err
	}
	return policy.ClientOptions{
		Cloud: cloudCfg,
	}, nil
}

// GetCloudConfiguration based on the cloud name
func GetCloudConfiguration(name string) (cloud.Configuration, error) {
	switch name {
	case "", "AZURECLOUD", "AZUREPUBLICCLOUD":
		return cloud.AzurePublic, nil
	case "AZURECHINACLOUD":
		return cloud.AzureChina, nil
	case "AZUREUSGOVERNMENT", "AZUREUSGOVERNMENTCLOUD":
		return cloud.AzureGovernment, nil
	default:
		return cloud.Configuration{}, errors.New(fmt.Sprintf("unknown cloud: %s", name))
	}
}

// GetFromLocationConfigOrCredential returns the value of the specified key from BSL/VSL config or credentials
// as some common configuration items can be set in BSL/VSL config or credential file(such as the subscription ID or resource group)
// Reading from BSL/VSL config takes first.
func GetFromLocationConfigOrCredential(cfg, creds map[string]string, cfgKey, credKey string) string {
	value := cfg[cfgKey]
	if value != "" {
		return value
	}
	return creds[credKey]
}
