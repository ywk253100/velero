package azure

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"
)

// NewBlobClient creates a blob client with the provided config which contains BSL config and the credential file name.
// The returned azblob.SharedKeyCredential is needed for Azure plugin to generate the SAS URL when auth with storage
// account access key
func NewBlobClient(config map[string]string) (*azblob.Client, *azblob.SharedKeyCredential, error) {
	// rename to bslCfg for easy understanding
	bslCfg := config

	// storage account is required
	storageAccount := bslCfg[BSLConfigStorageAccount]
	if storageAccount == "" {
		return nil, nil, errors.Errorf("%s is required in BSL", BSLConfigStorageAccount)
	}

	// read the credentials provided by users
	creds, err := LoadCredentials(config)
	if err != nil {
		return nil, nil, err
	}
	// exchange the storage account access key if needed
	creds, err = GetStorageAccountCredentials(bslCfg, creds)
	if err != nil {
		return nil, nil, err
	}

	// get the storage account URI
	uri, err := getStorageAccountURI(bslCfg, creds)
	if err != nil {
		return nil, nil, err
	}

	clientOptions, err := GetClientOptions(creds[CredentialKeyCloudName])
	if err != nil {
		return nil, nil, err
	}
	blobClientOptions := &azblob.ClientOptions{
		ClientOptions: clientOptions,
	}

	// auth with storage account access key
	accessKey := creds[CredentialKeyStorageAccountAccessKey]
	if accessKey != "" {
		cred, err := azblob.NewSharedKeyCredential(storageAccount, accessKey)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to create storage account access key credential")
		}
		client, err := azblob.NewClientWithSharedKeyCredential(uri, cred, blobClientOptions)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to create blob client with the storage account access key")
		}
		return client, cred, nil
	}

	// auth with Azure AD
	cred, err := NewCredential(creds, clientOptions)
	if err != nil {
		return nil, nil, err
	}
	client, err := azblob.NewClient(uri, cred, blobClientOptions)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create blob client with the Azure AD credential")
	}
	return client, nil, nil
}
