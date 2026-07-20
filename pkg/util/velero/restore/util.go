package restore

import (
	api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

func IsResourcePolicyValid(resourcePolicy string) bool {
	return resourcePolicy == string(api.ResourcePolicyTypeNone) || resourcePolicy == string(api.ResourcePolicyTypeUpdate)
}

func IsVolumeDataPolicyValid(volumeDataPolicy string) bool {
	return volumeDataPolicy == string(api.VolumeDataPolicyTypeNone) ||
		volumeDataPolicy == string(api.VolumeDataPolicyTypeOverwrite) ||
		volumeDataPolicy == string(api.VolumeDataPolicyTypeIncremental)
}
