package backend

import (
	"context"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	azureutil "github.com/vmware-tanzu/velero/pkg/util/azure"
)

const (
	azStorageType = "azure"
)

func init() {
	blob.AddSupportedStorage(azStorageType, map[string]string{}, NewAzureStorage)
}

type AzureStorage struct {
	*azure.AzStorage
	config map[string]string
}

func (az *AzureStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   azStorageType,
		Config: az.config,
	}
}

func NewAzureStorage(ctx context.Context, config *map[string]string, isCreate bool) (blob.Storage, error) {
	cfg := *config

	client, _, err := azureutil.NewBlobClient(cfg)
	// client, err := azureutil.NewBlobClient(cfg)
	if err != nil {
		return nil, err
	}

	az := &AzureStorage{
		config: cfg,
		AzStorage: &azure.AzStorage{
			Options: azure.Options{
				Container: cfg[udmrepo.StoreOptionOssBucket],
				Prefix:    cfg[udmrepo.StoreOptionPrefix],
				Limits:    setupLimits(ctx, cfg),
			},
			Container: cfg[udmrepo.StoreOptionOssBucket],
			Service:   client,
		},
	}
	return az, nil
}
