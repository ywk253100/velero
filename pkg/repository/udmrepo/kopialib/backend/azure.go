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
	"encoding/base64"

	"github.com/kopia/kopia/repo/blob"
	"github.com/pkg/errors"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo/kopialib/backend/azure"
)

type AzureBackend struct {
	option azure.Option
}

func (c *AzureBackend) Setup(ctx context.Context, flags map[string]string) error {
	// As pkg/util/azure.NewStorageClient(config) is used in both repository and plugin,
	// the caCert isn't encoded when passing to the plugin, so we need to decode the caCert
	// before passing the config into the NewStorageClient()
	if flags[udmrepo.StoreOptionCACert] != "" {
		caCert, err := base64.StdEncoding.DecodeString(flags[udmrepo.StoreOptionCACert])
		if err != nil {
			return errors.Wrapf(err, "failed to decode the CA cert")
		}
		flags[udmrepo.StoreOptionCACert] = string(caCert)
	}
	c.option = azure.Option{
		Config: flags,
		Limits: setupLimits(ctx, flags),
	}
	return nil
}

func (c *AzureBackend) Connect(ctx context.Context, isCreate bool) (blob.Storage, error) {
	return azure.NewStorage(ctx, &c.option, false)
}
