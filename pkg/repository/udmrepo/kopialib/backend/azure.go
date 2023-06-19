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

package backend

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	azureutil "github.com/vmware-tanzu/velero/pkg/util/azure"
)

type AzureBackend struct {
	options azure.Options
	client  *azblob.Client
}

func (c *AzureBackend) Setup(ctx context.Context, flags map[string]string) error {
	var err error
	c.options.Container, err = mustHaveString(udmrepo.StoreOptionOssBucket, flags)
	if err != nil {
		return err
	}
	c.options.StorageAccount, err = mustHaveString(udmrepo.StoreOptionAzureStorageAccount, flags)
	if err != nil {
		return err
	}
	c.options.StorageDomain, err = mustHaveString(udmrepo.StoreOptionAzureDomain, flags)
	if err != nil {
		return err
	}
	c.options.Prefix = optionalHaveString(udmrepo.StoreOptionPrefix, flags)
	c.options.Limits = setupLimits(ctx, flags)

	clientOptions, err := azureutil.GetClientOptions(flags[azureutil.CredentialKeyCloudName])
	if err != nil {
		return err
	}

	// auth with storage account access key
	if flags[azureutil.CredentialKeyStorageAccountAccessKey] != "" {
		cred, err := azblob.NewSharedKeyCredential(c.options.StorageAccount, flags[azureutil.CredentialKeyStorageAccountAccessKey])
		if err != nil {
			return err
		}
		c.client, err = azblob.NewClientWithSharedKeyCredential(c.options.StorageDomain, cred, &azblob.ClientOptions{
			ClientOptions: clientOptions,
		})
		return err
	}

	// auth with Azure AD
	cred, err := azureutil.NewCredential(flags, clientOptions)
	if err != nil {
		return err
	}
	c.client, err = azblob.NewClient(c.options.StorageDomain, cred, &azblob.ClientOptions{
		ClientOptions: clientOptions,
	})
	return err
}

func (c *AzureBackend) Connect(ctx context.Context, isCreate bool) (blob.Storage, error) {
	return azure.NewWithBlobClient(ctx, &c.options, c.client)
}
