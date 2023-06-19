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
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/util/azure"
)

func TestConnectionInfo(t *testing.T) {
	option := &Option{
		Config: map[string]string{"key": "value"},
	}
	storage := &Storage{
		Option: option,
	}

	info := storage.ConnectionInfo()
	assert.Equal(t, storageType, info.Type)
	assert.EqualValues(t, option, info.Config)
}

func TestNewStorage(t *testing.T) {
	limits := throttling.Limits{
		ReadsPerSecond:       100,
		UploadBytesPerSecond: 200,
	}

	name := filepath.Join(os.TempDir(), "credential")
	file, err := os.Create(name)
	require.Nil(t, err)
	defer file.Close()
	defer os.Remove(name)
	_, err = file.WriteString("AccessKey: YWNjZXNza2V5")
	require.Nil(t, err)

	option := &Option{
		Config: map[string]string{
			azure.BSLConfigStorageAccount:              "storage-account",
			azure.BSLConfigStorageAccountAccessKeyName: "AccessKey",
			udmrepo.StoreOptionOssBucket:               "bucket",
			udmrepo.StoreOptionPrefix:                  "prefix",
			"credentialsFile":                          name,
		},
		Limits: limits,
	}

	storage, err := NewStorage(context.Background(), option, false)
	require.Nil(t, err)
	s, ok := storage.(*Storage)
	require.True(t, ok)
	assert.Equal(t, "bucket", s.container)
	assert.Equal(t, "bucket", s.Options.Container)
	assert.Equal(t, "prefix", s.Options.Prefix)
	assert.Equal(t, limits, s.Options.Limits)
}
