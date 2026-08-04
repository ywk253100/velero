/*
Copyright The Velero Contributors.

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

package exposer

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/vmware-tanzu/velero/pkg/nodeagent"
)

const (
	// hostPluginsVolumeName is the name of the node-agent volume that mounts the kubelet
	// plugins directory from the host.
	hostPluginsVolumeName = "host-plugins"
)

// hostPathVolumesOfNodeAgent lists the node-agent volumes that expose the kubelet root
// directory of the host. They are only required by fs-backup, which resolves and accesses
// pod volume data through the kubelet pod directory. Other exposers access data through
// PVCs only, so they must exclude these volumes from the inherited pod info to avoid
// granting data mover pods unnecessary access to the host file system.
var hostPathVolumesOfNodeAgent = []string{nodeagent.HostPodVolumeMount, hostPluginsVolumeName}

type inheritedPodInfo struct {
	image            string
	serviceAccount   string
	env              []corev1api.EnvVar
	envFrom          []corev1api.EnvFromSource
	volumeMounts     []corev1api.VolumeMount
	volumes          []corev1api.Volume
	logLevelArgs     []string
	logFormatArgs    []string
	dnsPolicy        corev1api.DNSPolicy
	dnsConfig        *corev1api.PodDNSConfig
	imagePullSecrets []corev1api.LocalObjectReference
}

// getInheritedPodInfo collects the pod info to be inherited by the hosting pods from the
// node-agent pod template. Volumes whose name is listed in excludedVolumes, together with
// their volume mounts, are dropped from the result. Names that are not found in the
// node-agent pod template are ignored.
func getInheritedPodInfo(ctx context.Context, client kubernetes.Interface, veleroNamespace string, osType string, excludedVolumes ...string) (inheritedPodInfo, error) {
	podInfo := inheritedPodInfo{}

	podSpec, err := nodeagent.GetPodSpec(ctx, client, veleroNamespace, osType)
	if err != nil {
		return podInfo, errors.Wrap(err, "error to get node-agent pod template")
	}

	if len(podSpec.Containers) != 1 {
		return podInfo, errors.New("unexpected pod template from node-agent")
	}

	podInfo.image = podSpec.Containers[0].Image
	podInfo.serviceAccount = podSpec.ServiceAccountName

	podInfo.env = podSpec.Containers[0].Env
	podInfo.envFrom = podSpec.Containers[0].EnvFrom
	podInfo.volumeMounts, podInfo.volumes = excludeVolumes(podSpec.Containers[0].VolumeMounts, podSpec.Volumes, excludedVolumes)

	podInfo.dnsPolicy = podSpec.DNSPolicy
	podInfo.dnsConfig = podSpec.DNSConfig

	args := podSpec.Containers[0].Args
	for i, arg := range args {
		if arg == "--log-format" {
			podInfo.logFormatArgs = append(podInfo.logFormatArgs, args[i:i+2]...)
		} else if strings.HasPrefix(arg, "--log-format") {
			podInfo.logFormatArgs = append(podInfo.logFormatArgs, arg)
		} else if arg == "--log-level" {
			podInfo.logLevelArgs = append(podInfo.logLevelArgs, args[i:i+2]...)
		} else if strings.HasPrefix(arg, "--log-level") {
			podInfo.logLevelArgs = append(podInfo.logLevelArgs, arg)
		}
	}

	podInfo.imagePullSecrets = podSpec.ImagePullSecrets

	return podInfo, nil
}

// excludeVolumes removes the volumes matching the given names, as well as the volume mounts
// referring to them, from the given volumes and volume mounts. An excluded name that doesn't
// match any volume is a no-op, so callers don't need to know how the node-agent daemonset is
// configured. Volumes that are not excluded, including the ones customized by users, are kept
// as is.
func excludeVolumes(volumeMounts []corev1api.VolumeMount, volumes []corev1api.Volume, excludedVolumes []string) ([]corev1api.VolumeMount, []corev1api.Volume) {
	if len(excludedVolumes) == 0 {
		return volumeMounts, volumes
	}

	excluded := make(map[string]struct{}, len(excludedVolumes))
	for _, name := range excludedVolumes {
		excluded[name] = struct{}{}
	}

	var retainedMounts []corev1api.VolumeMount
	for _, volumeMount := range volumeMounts {
		if _, found := excluded[volumeMount.Name]; found {
			continue
		}

		retainedMounts = append(retainedMounts, volumeMount)
	}

	var retainedVolumes []corev1api.Volume
	for _, volume := range volumes {
		if _, found := excluded[volume.Name]; found {
			continue
		}

		retainedVolumes = append(retainedVolumes, volume)
	}

	return retainedMounts, retainedVolumes
}
