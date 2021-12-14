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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/labels"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/restic"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
)

// TODO add printcolume

type PodVolumeRestoreReconciler struct {
	client.Client
	logger               logrus.FieldLogger
	nodeName             string
	clock                clock.Clock
	credentialsFileStore credentials.FileStore
	fileSystem           filesystem.Interface
}

func NewPodVolumeRestoreReconciler(client client.Client, logger logrus.FieldLogger, nodeName string,
	credentialsFileStore credentials.FileStore) *PodVolumeRestoreReconciler {
	return &PodVolumeRestoreReconciler{
		Client:               client,
		logger:               logger,
		nodeName:             nodeName,
		clock:                &clock.RealClock{},
		credentialsFileStore: credentialsFileStore,
		fileSystem:           filesystem.NewFileSystem(),
	}
}

func (p *PodVolumeRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&velerov1api.PodVolumeRestore{}).Complete(p)
}

type PodReconciler struct {
	*PodVolumeRestoreReconciler
}

func NewPodReconciler(client client.Client, logger logrus.FieldLogger, nodeName string,
	credentialsFileStore credentials.FileStore) *PodReconciler {
	return &PodReconciler{
		PodVolumeRestoreReconciler: NewPodVolumeRestoreReconciler(client, logger, nodeName, credentialsFileStore),
	}
}

func (c *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&corev1api.Pod{}).Complete(p)
}

func (c *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := c.logger.WithField("controller", "pod").WithField("pod", req.NamespacedName.String())

	// TODO sometimes the NodeName of pod is null
	pod := &corev1api.Pod{}
	if err := c.Get(ctx, req.NamespacedName, pod); err != nil {
		if apierrors.IsNotFound(err) {
			log.WithError(err).Debug("Pod not found, skip")
			return ctrl.Result{}, nil
		}
		log.WithError(err).Error("Unable to get the pod, skip")
		return ctrl.Result{}, err
	}

	pvrs := &velerov1api.PodVolumeRestoreList{}
	selector := labels.Set(map[string]string{
		velerov1api.PodUIDLabel: string(pod.UID),
	}).AsSelector()
	c.List(ctx, pvrs, selector)
	if err := c.Get(ctx, req.NamespacedName, pvr); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Pod volume restore not found, skip")
			return ctrl.Result{}, nil
		}
		log.WithError(err).Errorf("Unable to get the pod volume restore")
		return ctrl.Result{}, err
	}
	log = log.WithField("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
	if len(pvr.OwnerReferences) == 1 {
		log = log.WithField("restore", fmt.Sprintf("%s/%s", req.Namespace, pvr.OwnerReferences[0].Name))
	}

	return ctrl.Result{}, c.processRestore(ctx, pvr, pod, log)
}

func (c *PodVolumeRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := c.logger.WithField("controller", "pod-volume-restore").
		WithField("pod-volume-restore", req.NamespacedName.String())

	pvr := &velerov1api.PodVolumeRestore{}
	if err := c.Get(ctx, req.NamespacedName, pvr); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Pod volume restore not found, skip")
			return ctrl.Result{}, nil
		}
		log.WithError(err).Errorf("Unable to get the pod volume restore")
		return ctrl.Result{}, err
	}
	if len(pvr.OwnerReferences) == 1 {
		log = log.WithField("restore", fmt.Sprintf("%s/%s", req.Namespace, pvr.OwnerReferences[0].Name))
	}

	// TODO sometimes the NodeName of pod is null
	pod := &corev1api.Pod{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: pvr.Spec.Pod.Namespace, Name: pvr.Spec.Pod.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			log.WithError(err).Debugf("Restore's pod %s/%s not found, skip", pvr.Spec.Pod.Namespace, pvr.Spec.Pod.Name)
			return ctrl.Result{}, nil
		}
		log.WithError(err).Errorf("Unable to get restore's pod %s/%s, skip", pvr.Spec.Pod.Namespace, pvr.Spec.Pod.Name)
		return ctrl.Result{}, err
	}
	log = log.WithField("pod", fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))

	return ctrl.Result{}, c.processRestore(ctx, pvr, pod, log)
}

func isPVRNew(pvr *velerov1api.PodVolumeRestore) bool {
	return pvr.Status.Phase == "" || pvr.Status.Phase == velerov1api.PodVolumeRestorePhaseNew
}

func isPodOnNode(pod *corev1api.Pod, node string) bool {
	fmt.Printf("############### %s %s\n", pod.Spec.NodeName, node)
	return pod.Spec.NodeName == node
}

func isResticInitContainerRunning(pod *corev1api.Pod) bool {
	// Restic wait container can be anywhere in the list of init containers, but must be running.
	i := getResticInitContainerIndex(pod)
	return i >= 0 &&
		len(pod.Status.InitContainerStatuses)-1 >= i &&
		pod.Status.InitContainerStatuses[i].State.Running != nil
}

func getResticInitContainerIndex(pod *corev1api.Pod) int {
	// Restic wait container can be anywhere in the list of init containers so locate it.
	for i, initContainer := range pod.Spec.InitContainers {
		if initContainer.Name == restic.InitContainer {
			return i
		}
	}

	return -1
}

func (c *PodVolumeRestoreReconciler) processRestore(ctx context.Context, pvr *velerov1api.PodVolumeRestore, pod *corev1api.Pod, log *logrus.Entry) error {
	if !isPVRNew(pvr) {
		log.Debug("Restore is not new, skip")
		return nil
	}

	if !isPodOnNode(pod, c.nodeName) {
		log.Debug("Restore's pod is not on this node, skip")
		return nil
	}

	if !isResticInitContainerRunning(pod) {
		log.Debug("Restore's pod is not running restic-wait init container, skip")
		return nil
	}

	resticInitContainerIndex := getResticInitContainerIndex(pod)
	if resticInitContainerIndex > 0 {
		log.Warnf(`Init containers before the %s container may cause issues
		          if they interfere with volumes being restored: %s index %d`, restic.InitContainer, restic.InitContainer, resticInitContainerIndex)
	}

	log.Info("Restore starting")

	var err error

	// update status to InProgress
	pvr, err = c.patchPodVolumeRestore(ctx, pvr, func(r *velerov1api.PodVolumeRestore) {
		r.Status.Phase = velerov1api.PodVolumeRestorePhaseInProgress
		r.Status.StartTimestamp = &metav1.Time{Time: c.clock.Now()}
	})
	if err != nil {
		log.WithError(err).Error("Error setting PodVolumeRestore startTimestamp and phase to InProgress")
		return errors.WithStack(err)
	}

	volumeDir, err := kube.GetVolumeDirectory2(pod, pvr.Spec.Volume, c.Client)
	if err != nil {
		log.WithError(err).Error("Error getting volume directory name")
		return c.failRestore(ctx, pvr, errors.Wrap(err, "error getting volume directory name").Error(), log)
	}

	// execute the restore process
	if err := c.restorePodVolume(ctx, pvr, volumeDir, log); err != nil {
		log.WithError(err).Error("Error restoring volume")
		return c.failRestore(ctx, pvr, errors.Wrap(err, "error restoring volume").Error(), log)
	}

	// update status to Completed
	if _, err = c.patchPodVolumeRestore(ctx, pvr, func(r *velerov1api.PodVolumeRestore) {
		r.Status.Phase = velerov1api.PodVolumeRestorePhaseCompleted
		r.Status.CompletionTimestamp = &metav1.Time{Time: c.clock.Now()}
	}); err != nil {
		log.WithError(err).Error("Error setting PodVolumeRestore completionTimestamp and phase to Completed")
		return err
	}

	log.Info("Restore completed")

	return nil
}

func (c *PodVolumeRestoreReconciler) restorePodVolume(ctx context.Context, req *velerov1api.PodVolumeRestore, volumeDir string, log logrus.FieldLogger) error {
	// Get the full path of the new volume's directory as mounted in the daemonset pod, which
	// will look like: /host_pods/<new-pod-uid>/volumes/<volume-plugin-name>/<volume-dir>
	volumePath, err := singlePathMatch(fmt.Sprintf("/host_pods/%s/volumes/*/%s", string(req.Spec.Pod.UID), volumeDir))
	if err != nil {
		return errors.Wrap(err, "error identifying path of volume")
	}

	credsFile, err := c.credentialsFileStore.Path(restic.RepoKeySelector())
	if err != nil {
		log.WithError(err).Error("Error creating temp restic credentials file")
		return c.failRestore(ctx, req, errors.Wrap(err, "error creating temp restic credentials file").Error(), log)
	}
	// ignore error since there's nothing we can do and it's a temp file.
	defer os.Remove(credsFile)

	resticCmd := restic.RestoreCommand(
		req.Spec.RepoIdentifier,
		credsFile,
		req.Spec.SnapshotID,
		volumePath,
	)

	backupLocation := &velerov1api.BackupStorageLocation{}
	if err := c.Get(context.Background(), client.ObjectKey{
		Namespace: req.Namespace,
		Name:      req.Spec.BackupStorageLocation,
	}, backupLocation); err != nil {
		return c.failRestore(ctx, req, errors.Wrap(err, "error getting backup storage location").Error(), log)
	}

	// if there's a caCert on the ObjectStorage, write it to disk so that it can be passed to restic
	var caCertFile string
	if backupLocation.Spec.ObjectStorage != nil && backupLocation.Spec.ObjectStorage.CACert != nil {
		caCertFile, err = restic.TempCACertFile(backupLocation.Spec.ObjectStorage.CACert, req.Spec.BackupStorageLocation, c.fileSystem)
		if err != nil {
			log.WithError(err).Error("Error creating temp cacert file")
		}
		// ignore error since there's nothing we can do and it's a temp file.
		defer os.Remove(caCertFile)
	}
	resticCmd.CACertFile = caCertFile

	env, err := restic.CmdEnv(backupLocation, c.credentialsFileStore)
	if err != nil {
		return c.failRestore(ctx, req, errors.Wrap(err, "error setting restic cmd env").Error(), log)
	}
	resticCmd.Env = env

	var stdout, stderr string

	if stdout, stderr, err = restic.RunRestore(resticCmd, log, c.updateRestoreProgressFunc(ctx, req, log)); err != nil {
		return errors.Wrapf(err, "error running restic restore, cmd=%s, stdout=%s, stderr=%s", resticCmd.String(), stdout, stderr)
	}
	log.Debugf("Ran command=%s, stdout=%s, stderr=%s", resticCmd.String(), stdout, stderr)

	// Remove the .velero directory from the restored volume (it may contain done files from previous restores
	// of this volume, which we don't want to carry over). If this fails for any reason, log and continue, since
	// this is non-essential cleanup (the done files are named based on restore UID and the init container looks
	// for the one specific to the restore being executed).
	if err := os.RemoveAll(filepath.Join(volumePath, ".velero")); err != nil {
		log.WithError(err).Warnf("error removing .velero directory from directory %s", volumePath)
	}

	var restoreUID types.UID
	for _, owner := range req.OwnerReferences {
		if boolptr.IsSetToTrue(owner.Controller) {
			restoreUID = owner.UID
			break
		}
	}

	// Create the .velero directory within the volume dir so we can write a done file
	// for this restore.
	if err := os.MkdirAll(filepath.Join(volumePath, ".velero"), 0755); err != nil {
		return errors.Wrap(err, "error creating .velero directory for done file")
	}

	// Write a done file with name=<restore-uid> into the just-created .velero dir
	// within the volume. The velero restic init container on the pod is waiting
	// for this file to exist in each restored volume before completing.
	if err := ioutil.WriteFile(filepath.Join(volumePath, ".velero", string(restoreUID)), nil, 0644); err != nil {
		return errors.Wrap(err, "error writing done file")
	}

	return nil
}

func (c *PodVolumeRestoreReconciler) patchPodVolumeRestore(ctx context.Context, req *velerov1api.PodVolumeRestore, mutate func(*velerov1api.PodVolumeRestore)) (*velerov1api.PodVolumeRestore, error) {
	// Record original json
	oldData, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original PodVolumeRestore")
	}

	// Mutate
	mutate(req)

	// Record new json
	newData, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated PodVolumeRestore")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(oldData, newData)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for PodVolumeRestore")
	}

	err = c.Patch(ctx, req, client.RawPatch(types.MergePatchType, patchBytes))
	if err != nil {
		return nil, errors.Wrap(err, "error patching PodVolumeRestore")
	}

	return req, nil
}

func (c *PodVolumeRestoreReconciler) failRestore(ctx context.Context, req *velerov1api.PodVolumeRestore, msg string, log logrus.FieldLogger) error {
	if _, err := c.patchPodVolumeRestore(ctx, req, func(pvr *velerov1api.PodVolumeRestore) {
		pvr.Status.Phase = velerov1api.PodVolumeRestorePhaseFailed
		pvr.Status.Message = msg
		pvr.Status.CompletionTimestamp = &metav1.Time{Time: c.clock.Now()}
	}); err != nil {
		log.WithError(err).Error("Error setting PodVolumeRestore phase to Failed")
		return err
	}
	return nil
}

// updateRestoreProgressFunc returns a func that takes progress info and patches
// the PVR with the new progress
func (c *PodVolumeRestoreReconciler) updateRestoreProgressFunc(ctx context.Context, req *velerov1api.PodVolumeRestore, log logrus.FieldLogger) func(velerov1api.PodVolumeOperationProgress) {
	return func(progress velerov1api.PodVolumeOperationProgress) {
		if _, err := c.patchPodVolumeRestore(ctx, req, func(r *velerov1api.PodVolumeRestore) {
			r.Status.Progress = progress
		}); err != nil {
			log.WithError(err).Error("error updating PodVolumeRestore progress")
		}
	}
}
