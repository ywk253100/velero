package restore

import (
	api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

func IsResourcePolicyValid(resourcePolicy string) bool {
	if resourcePolicy == string(api.ResourcePolicyTypeNone) || resourcePolicy == string(api.ResourcePolicyTypeUpdate) {
		return true
	}
	return false
}

func IsVolumeDataPolicyValid(volumeDataPolicy string) bool {
	if volumeDataPolicy == string(api.VolumeDataPolicyTypeNone) || volumeDataPolicy == string(api.VolumeDataPolicyTypeOverwrite) ||
		volumeDataPolicy == string(api.VolumeDataPolicyTypeIncremental) {
		return true
	}
	return false
}
