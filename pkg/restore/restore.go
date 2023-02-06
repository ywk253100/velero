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

package restore

import (
	go_context "context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubeerrs "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	"github.com/vmware-tanzu/velero/internal/hook"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/archive"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/discovery"
	"github.com/vmware-tanzu/velero/pkg/features"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/framework"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	riav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/restoreitemaction/v2"
	vsv1 "github.com/vmware-tanzu/velero/pkg/plugin/velero/volumesnapshotter/v1"
	"github.com/vmware-tanzu/velero/pkg/podexec"
	"github.com/vmware-tanzu/velero/pkg/podvolume"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
	"github.com/vmware-tanzu/velero/pkg/util/collections"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
	"github.com/vmware-tanzu/velero/pkg/volume"
)

type VolumeSnapshotterGetter interface {
	GetVolumeSnapshotter(name string) (vsv1.VolumeSnapshotter, error)
}

type Request struct {
	*velerov1api.Restore

	Log              logrus.FieldLogger
	Backup           *velerov1api.Backup
	PodVolumeBackups []*velerov1api.PodVolumeBackup
	VolumeSnapshots  []*volume.Snapshot
	BackupReader     io.Reader
}

// Restorer knows how to restore a backup.
type Restorer interface {
	// Restore restores the backup data from backupReader, returning warnings and errors.
	Restore(req Request,
		actions []riav2.RestoreItemAction,
		volumeSnapshotterGetter VolumeSnapshotterGetter,
	) (Result, Result)
	RestoreWithResolvers(
		req Request,
		restoreItemActionResolver framework.RestoreItemActionResolverV2,
		itemSnapshotterResolver framework.ItemSnapshotterResolver,
		volumeSnapshotterGetter VolumeSnapshotterGetter,
	) (Result, Result)
}

// kubernetesRestorer implements Restorer for restoring into a Kubernetes cluster.
type kubernetesRestorer struct {
	discoveryHelper            discovery.Helper
	dynamicFactory             client.DynamicFactory
	namespaceClient            corev1.NamespaceInterface
	podVolumeRestorerFactory   podvolume.RestorerFactory
	podVolumeTimeout           time.Duration
	resourceTerminatingTimeout time.Duration
	resourcePriorities         Priorities
	fileSystem                 filesystem.Interface
	pvRenamer                  func(string) (string, error)
	logger                     logrus.FieldLogger
	podCommandExecutor         podexec.PodCommandExecutor
	podGetter                  cache.Getter
	credentialFileStore        credentials.FileStore
	kbClient                   crclient.Client
}

// NewKubernetesRestorer creates a new kubernetesRestorer.
func NewKubernetesRestorer(
	discoveryHelper discovery.Helper,
	dynamicFactory client.DynamicFactory,
	resourcePriorities Priorities,
	namespaceClient corev1.NamespaceInterface,
	podVolumeRestorerFactory podvolume.RestorerFactory,
	podVolumeTimeout time.Duration,
	resourceTerminatingTimeout time.Duration,
	logger logrus.FieldLogger,
	podCommandExecutor podexec.PodCommandExecutor,
	podGetter cache.Getter,
	credentialStore credentials.FileStore,
	kbClient crclient.Client,
) (Restorer, error) {
	return &kubernetesRestorer{
		discoveryHelper:            discoveryHelper,
		dynamicFactory:             dynamicFactory,
		namespaceClient:            namespaceClient,
		podVolumeRestorerFactory:   podVolumeRestorerFactory,
		podVolumeTimeout:           podVolumeTimeout,
		resourceTerminatingTimeout: resourceTerminatingTimeout,
		resourcePriorities:         resourcePriorities,
		logger:                     logger,
		pvRenamer: func(string) (string, error) {
			veleroCloneUuid, err := uuid.NewRandom()
			if err != nil {
				return "", errors.WithStack(err)
			}
			veleroCloneName := "velero-clone-" + veleroCloneUuid.String()
			return veleroCloneName, nil
		},
		fileSystem:          filesystem.NewFileSystem(),
		podCommandExecutor:  podCommandExecutor,
		podGetter:           podGetter,
		credentialFileStore: credentialStore,
		kbClient:            kbClient,
	}, nil
}

// Restore executes a restore into the target Kubernetes cluster according to the restore spec
// and using data from the provided backup/backup reader. Returns a warnings and errors RestoreResult,
// respectively, summarizing info about the restore.
func (kr *kubernetesRestorer) Restore(
	req Request,
	actions []riav2.RestoreItemAction,
	volumeSnapshotterGetter VolumeSnapshotterGetter,
) (Result, Result) {
	resolver := framework.NewRestoreItemActionResolverV2(actions)
	snapshotItemResolver := framework.NewItemSnapshotterResolver(nil)
	return kr.RestoreWithResolvers(req, resolver, snapshotItemResolver, volumeSnapshotterGetter)
}

func (kr *kubernetesRestorer) RestoreWithResolvers(
	req Request,
	restoreItemActionResolver framework.RestoreItemActionResolverV2,
	itemSnapshotterResolver framework.ItemSnapshotterResolver,
	volumeSnapshotterGetter VolumeSnapshotterGetter,
) (Result, Result) {
	// metav1.LabelSelectorAsSelector converts a nil LabelSelector to a
	// Nothing Selector, i.e. a selector that matches nothing. We want
	// a selector that matches everything. This can be accomplished by
	// passing a non-nil empty LabelSelector.
	ls := req.Restore.Spec.LabelSelector
	if ls == nil {
		ls = &metav1.LabelSelector{}
	}

	var OrSelectors []labels.Selector
	if req.Restore.Spec.OrLabelSelectors != nil {
		for _, s := range req.Restore.Spec.OrLabelSelectors {
			labelAsSelector, err := metav1.LabelSelectorAsSelector(s)
			if err != nil {
				return Result{}, Result{Velero: []string{err.Error()}}
			}
			OrSelectors = append(OrSelectors, labelAsSelector)
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(ls)
	if err != nil {
		return Result{}, Result{Velero: []string{err.Error()}}
	}

	// Get resource includes-excludes.
	resourceIncludesExcludes := collections.GetResourceIncludesExcludes(
		kr.discoveryHelper,
		req.Restore.Spec.IncludedResources,
		req.Restore.Spec.ExcludedResources,
	)

	// Get resource status includes-excludes. Defaults to excluding all resources
	var restoreStatusIncludesExcludes *collections.IncludesExcludes

	if req.Restore.Spec.RestoreStatus != nil {
		restoreStatusIncludesExcludes = collections.GetResourceIncludesExcludes(
			kr.discoveryHelper,
			req.Restore.Spec.RestoreStatus.IncludedResources,
			req.Restore.Spec.RestoreStatus.ExcludedResources,
		)
	}

	// Get namespace includes-excludes.
	namespaceIncludesExcludes := collections.NewIncludesExcludes().
		Includes(req.Restore.Spec.IncludedNamespaces...).
		Excludes(req.Restore.Spec.ExcludedNamespaces...)

	resolvedActions, err := restoreItemActionResolver.ResolveActions(kr.discoveryHelper, kr.logger)
	if err != nil {
		return Result{}, Result{Velero: []string{err.Error()}}
	}

	resolvedItemSnapshotterActions, err := itemSnapshotterResolver.ResolveActions(kr.discoveryHelper, kr.logger)
	if err != nil {
		return Result{}, Result{Velero: []string{err.Error()}}
	}

	podVolumeTimeout := kr.podVolumeTimeout
	if val := req.Restore.Annotations[velerov1api.PodVolumeOperationTimeoutAnnotation]; val != "" {
		parsed, err := time.ParseDuration(val)
		if err != nil {
			req.Log.WithError(errors.WithStack(err)).Errorf(
				"Unable to parse pod volume timeout annotation %s, using server value.",
				val,
			)
		} else {
			podVolumeTimeout = parsed
		}
	}

	ctx, cancelFunc := go_context.WithTimeout(go_context.Background(), podVolumeTimeout)
	defer cancelFunc()

	var podVolumeRestorer podvolume.Restorer
	if kr.podVolumeRestorerFactory != nil {
		podVolumeRestorer, err = kr.podVolumeRestorerFactory.NewRestorer(ctx, req.Restore)
		if err != nil {
			return Result{}, Result{Velero: []string{err.Error()}}
		}
	}

	resourceRestoreHooks, err := hook.GetRestoreHooksFromSpec(&req.Restore.Spec.Hooks)
	if err != nil {
		return Result{}, Result{Velero: []string{err.Error()}}
	}
	hooksCtx, hooksCancelFunc := go_context.WithCancel(go_context.Background())
	waitExecHookHandler := &hook.DefaultWaitExecHookHandler{
		PodCommandExecutor: kr.podCommandExecutor,
		ListWatchFactory: &hook.DefaultListWatchFactory{
			PodsGetter: kr.podGetter,
		},
	}

	pvRestorer := &pvRestorer{
		logger:                  req.Log,
		backup:                  req.Backup,
		snapshotVolumes:         req.Backup.Spec.SnapshotVolumes,
		restorePVs:              req.Restore.Spec.RestorePVs,
		volumeSnapshots:         req.VolumeSnapshots,
		volumeSnapshotterGetter: volumeSnapshotterGetter,
		kbclient:                kr.kbClient,
		credentialFileStore:     kr.credentialFileStore,
	}

	restoreCtx := &restoreContext{
		backup:                         req.Backup,
		backupReader:                   req.BackupReader,
		restore:                        req.Restore,
		resourceIncludesExcludes:       resourceIncludesExcludes,
		resourceStatusIncludesExcludes: restoreStatusIncludesExcludes,
		namespaceIncludesExcludes:      namespaceIncludesExcludes,
		chosenGrpVersToRestore:         make(map[string]ChosenGroupVersion),
		selector:                       selector,
		OrSelectors:                    OrSelectors,
		log:                            req.Log,
		dynamicFactory:                 kr.dynamicFactory,
		fileSystem:                     kr.fileSystem,
		namespaceClient:                kr.namespaceClient,
		restoreItemActions:             resolvedActions,
		itemSnapshotterActions:         resolvedItemSnapshotterActions,
		volumeSnapshotterGetter:        volumeSnapshotterGetter,
		podVolumeRestorer:              podVolumeRestorer,
		podVolumeErrs:                  make(chan error),
		pvsToProvision:                 sets.NewString(),
		pvRestorer:                     pvRestorer,
		volumeSnapshots:                req.VolumeSnapshots,
		podVolumeBackups:               req.PodVolumeBackups,
		resourceTerminatingTimeout:     kr.resourceTerminatingTimeout,
		resourceClients:                make(map[resourceClientKey]client.Dynamic),
		restoredItems:                  make(map[velero.ResourceIdentifier]struct{}),
		renamedPVs:                     make(map[string]string),
		pvRenamer:                      kr.pvRenamer,
		discoveryHelper:                kr.discoveryHelper,
		resourcePriorities:             kr.resourcePriorities,
		resourceRestoreHooks:           resourceRestoreHooks,
		hooksErrs:                      make(chan error),
		waitExecHookHandler:            waitExecHookHandler,
		hooksContext:                   hooksCtx,
		hooksCancelFunc:                hooksCancelFunc,
		kbClient:                       kr.kbClient,
	}

	return restoreCtx.execute()
}

type restoreContext struct {
	backup                         *velerov1api.Backup
	backupReader                   io.Reader
	restore                        *velerov1api.Restore
	restoreDir                     string
	resourceIncludesExcludes       *collections.IncludesExcludes
	resourceStatusIncludesExcludes *collections.IncludesExcludes
	namespaceIncludesExcludes      *collections.IncludesExcludes
	chosenGrpVersToRestore         map[string]ChosenGroupVersion
	selector                       labels.Selector
	OrSelectors                    []labels.Selector
	log                            logrus.FieldLogger
	dynamicFactory                 client.DynamicFactory
	fileSystem                     filesystem.Interface
	namespaceClient                corev1.NamespaceInterface
	restoreItemActions             []framework.RestoreItemResolvedActionV2
	itemSnapshotterActions         []framework.ItemSnapshotterResolvedAction
	volumeSnapshotterGetter        VolumeSnapshotterGetter
	podVolumeRestorer              podvolume.Restorer
	podVolumeWaitGroup             sync.WaitGroup
	podVolumeErrs                  chan error
	pvsToProvision                 sets.String
	pvRestorer                     PVRestorer
	volumeSnapshots                []*volume.Snapshot
	podVolumeBackups               []*velerov1api.PodVolumeBackup
	resourceTerminatingTimeout     time.Duration
	resourceClients                map[resourceClientKey]client.Dynamic
	restoredItems                  map[velero.ResourceIdentifier]struct{}
	renamedPVs                     map[string]string
	pvRenamer                      func(string) (string, error)
	discoveryHelper                discovery.Helper
	resourcePriorities             Priorities
	hooksWaitGroup                 sync.WaitGroup
	hooksErrs                      chan error
	resourceRestoreHooks           []hook.ResourceRestoreHook
	waitExecHookHandler            hook.WaitExecHookHandler
	hooksContext                   go_context.Context
	hooksCancelFunc                go_context.CancelFunc
	kbClient                       crclient.Client
}

type resourceClientKey struct {
	resource  schema.GroupVersionResource
	namespace string
}

// getOrderedResources returns an ordered list of resource identifiers to restore,
// based on the provided resource priorities and backup contents. The returned list
// begins with all of the high prioritized resources (in order), ends with all of
// the low prioritized resources(in order), and an alphabetized list of resources
// in the backup(pick out the prioritized resources) is put in the middle.
func getOrderedResources(resourcePriorities Priorities, backupResources map[string]*archive.ResourceItems) []string {
	priorities := map[string]struct{}{}
	for _, priority := range resourcePriorities.HighPriorities {
		priorities[priority] = struct{}{}
	}
	for _, priority := range resourcePriorities.LowPriorities {
		priorities[priority] = struct{}{}
	}

	// pick the prioritized resources out
	var orderedBackupResources []string
	for resource := range backupResources {
		if _, exist := priorities[resource]; exist {
			continue
		}
		orderedBackupResources = append(orderedBackupResources, resource)
	}
	// alphabetize resources in the backup
	sort.Strings(orderedBackupResources)

	list := append(resourcePriorities.HighPriorities, orderedBackupResources...)
	return append(list, resourcePriorities.LowPriorities...)
}

type progressUpdate struct {
	totalItems, itemsRestored int
}

func (ctx *restoreContext) execute() (Result, Result) {
	warnings, errs := Result{}, Result{}

	ctx.log.Infof("Starting restore of backup %s", kube.NamespaceAndName(ctx.backup))

	dir, err := archive.NewExtractor(ctx.log, ctx.fileSystem).UnzipAndExtractBackup(ctx.backupReader)
	if err != nil {
		ctx.log.Infof("error unzipping and extracting: %v", err)
		errs.AddVeleroError(err)
		return warnings, errs
	}
	defer ctx.fileSystem.RemoveAll(dir)

	// Need to set this for additionalItems to be restored.
	ctx.restoreDir = dir

	backupResources, err := archive.NewParser(ctx.log, ctx.fileSystem).Parse(ctx.restoreDir)
	// If ErrNotExist occurs, it implies that the backup to be restored includes zero items.
	// Need to add a warning about it and jump out of the function.
	if errors.Cause(err) == archive.ErrNotExist {
		warnings.AddVeleroError(errors.Wrap(err, "zero items to be restored"))
		return warnings, errs
	}
	if err != nil {
		errs.AddVeleroError(errors.Wrap(err, "error parsing backup contents"))
		return warnings, errs
	}

	// TODO: Remove outer feature flag check to make this feature a default in Velero.
	if features.IsEnabled(velerov1api.APIGroupVersionsFeatureFlag) {
		if ctx.backup.Status.FormatVersion >= "1.1.0" {
			if err := ctx.chooseAPIVersionsToRestore(); err != nil {
				errs.AddVeleroError(errors.Wrap(err, "choosing API version to restore"))
				return warnings, errs
			}
		}
	}

	update := make(chan progressUpdate)

	quit := make(chan struct{})

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		var lastUpdate *progressUpdate
		for {
			select {
			case <-quit:
				ticker.Stop()
				return
			case val := <-update:
				lastUpdate = &val
			case <-ticker.C:
				if lastUpdate != nil {
					updated := ctx.restore.DeepCopy()
					if updated.Status.Progress == nil {
						updated.Status.Progress = &velerov1api.RestoreProgress{}
					}
					updated.Status.Progress.TotalItems = lastUpdate.totalItems
					updated.Status.Progress.ItemsRestored = lastUpdate.itemsRestored
					err = kube.PatchResource(ctx.restore, updated, ctx.kbClient)
					if err != nil {
						ctx.log.WithError(errors.WithStack((err))).
							Warn("Got error trying to update restore's status.progress")
					}
					lastUpdate = nil
				}
			}
		}
	}()

	// totalItems: previously discovered items, i: iteration counter.
	totalItems, processedItems, existingNamespaces := 0, 0, sets.NewString()

	// First restore CRDs. This is needed so that they are available in the cluster
	// when getOrderedResourceCollection is called again on the whole backup and
	// needs to validate all resources listed.
	crdResourceCollection, processedResources, w, e := ctx.getOrderedResourceCollection(
		backupResources,
		make([]restoreableResource, 0),
		sets.NewString(),
		Priorities{HighPriorities: []string{"customresourcedefinitions"}},
		false,
	)
	warnings.Merge(&w)
	errs.Merge(&e)

	for _, selectedResource := range crdResourceCollection {
		totalItems += selectedResource.totalItems
	}

	for _, selectedResource := range crdResourceCollection {
		var w, e Result
		// Restore this resource
		processedItems, w, e = ctx.processSelectedResource(
			selectedResource,
			totalItems,
			processedItems,
			existingNamespaces,
			update,
		)
		warnings.Merge(&w)
		errs.Merge(&e)
	}

	// Restore everything else
	selectedResourceCollection, _, w, e := ctx.getOrderedResourceCollection(
		backupResources,
		crdResourceCollection,
		processedResources,
		ctx.resourcePriorities,
		true,
	)
	warnings.Merge(&w)
	errs.Merge(&e)

	// reset processedItems and totalItems before processing full resource list
	processedItems = 0
	totalItems = 0
	for _, selectedResource := range selectedResourceCollection {
		totalItems += selectedResource.totalItems
	}

	for _, selectedResource := range selectedResourceCollection {
		var w, e Result
		// Restore this resource
		processedItems, w, e = ctx.processSelectedResource(
			selectedResource,
			totalItems,
			processedItems,
			existingNamespaces,
			update,
		)
		warnings.Merge(&w)
		errs.Merge(&e)
	}

	// Close the progress update channel.
	quit <- struct{}{}

	// Do a final progress update as stopping the ticker might have left last few
	// updates from taking place.
	updated := ctx.restore.DeepCopy()
	if updated.Status.Progress == nil {
		updated.Status.Progress = &velerov1api.RestoreProgress{}
	}
	updated.Status.Progress.TotalItems = len(ctx.restoredItems)
	updated.Status.Progress.ItemsRestored = len(ctx.restoredItems)

	err = kube.PatchResource(ctx.restore, updated, ctx.kbClient)
	if err != nil {
		ctx.log.WithError(errors.WithStack((err))).Warn("Updating restore status.progress")
	}

	// Wait for all of the pod volume restore goroutines to be done, which is
	// only possible once all of their errors have been received by the loop
	// below, then close the podVolumeErrs channel so the loop terminates.
	go func() {
		ctx.log.Info("Waiting for all pod volume restores to complete")

		// TODO timeout?
		ctx.podVolumeWaitGroup.Wait()
		close(ctx.podVolumeErrs)
	}()

	// This loop will only terminate when the ctx.podVolumeErrs channel is closed
	// in the above goroutine, *after* all errors from the goroutines have been
	// received by this loop.
	for err := range ctx.podVolumeErrs {
		// TODO: not ideal to be adding these to Velero-level errors
		// rather than a specific namespace, but don't have a way
		// to track the namespace right now.
		errs.Velero = append(errs.Velero, err.Error())
	}
	ctx.log.Info("Done waiting for all pod volume restores to complete")

	// Wait for all post-restore exec hooks with same logic as pod volume wait above.
	go func() {
		ctx.log.Info("Waiting for all post-restore-exec hooks to complete")

		ctx.hooksWaitGroup.Wait()
		close(ctx.hooksErrs)
	}()
	for err := range ctx.hooksErrs {
		errs.Velero = append(errs.Velero, err.Error())
	}
	ctx.log.Info("Done waiting for all post-restore exec hooks to complete")

	return warnings, errs
}

// Process and restore one restoreableResource from the backup and update restore progress
// metadata. At this point, the resource has already been validated and counted for inclusion
// in the expected total restore count.
func (ctx *restoreContext) processSelectedResource(
	selectedResource restoreableResource,
	totalItems int,
	processedItems int,
	existingNamespaces sets.String,
	update chan progressUpdate,
) (int, Result, Result) {
	warnings, errs := Result{}, Result{}
	groupResource := schema.ParseGroupResource(selectedResource.resource)

	for namespace, selectedItems := range selectedResource.selectedItemsByNamespace {
		for _, selectedItem := range selectedItems {
			// If we don't know whether this namespace exists yet, attempt to create
			// it in order to ensure it exists. Try to get it from the backup tarball
			// (in order to get any backed-up metadata), but if we don't find it there,
			// create a blank one.
			if namespace != "" && !existingNamespaces.Has(selectedItem.targetNamespace) {
				logger := ctx.log.WithField("namespace", namespace)

				ns := getNamespace(
					logger,
					archive.GetItemFilePath(ctx.restoreDir, "namespaces", "", namespace),
					selectedItem.targetNamespace,
				)
				_, nsCreated, err := kube.EnsureNamespaceExistsAndIsReady(
					ns,
					ctx.namespaceClient,
					ctx.resourceTerminatingTimeout,
				)
				if err != nil {
					errs.AddVeleroError(err)
					continue
				}

				// Add the newly created namespace to the list of restored items.
				if nsCreated {
					itemKey := velero.ResourceIdentifier{
						GroupResource: kuberesource.Namespaces,
						Namespace:     ns.Namespace,
						Name:          ns.Name,
					}
					ctx.restoredItems[itemKey] = struct{}{}
				}

				// Keep track of namespaces that we know exist so we don't
				// have to try to create them multiple times.
				existingNamespaces.Insert(selectedItem.targetNamespace)
			}

			obj, err := archive.Unmarshal(ctx.fileSystem, selectedItem.path)
			if err != nil {
				errs.Add(
					selectedItem.targetNamespace,
					fmt.Errorf(
						"error decoding %q: %v",
						strings.Replace(selectedItem.path, ctx.restoreDir+"/", "", -1),
						err,
					),
				)
				continue
			}

			w, e := ctx.restoreItem(obj, groupResource, selectedItem.targetNamespace)
			warnings.Merge(&w)
			errs.Merge(&e)
			processedItems++

			// totalItems keeps the count of items previously known. There
			// may be additional items restored by plugins. We want to include
			// the additional items by looking at restoredItems at the same
			// time, we don't want previously known items counted twice as
			// they are present in both restoredItems and totalItems.
			actualTotalItems := len(ctx.restoredItems) + (totalItems - processedItems)
			update <- progressUpdate{
				totalItems:    actualTotalItems,
				itemsRestored: len(ctx.restoredItems),
			}
			ctx.log.WithFields(map[string]interface{}{
				"progress":  "",
				"resource":  groupResource.String(),
				"namespace": selectedItem.targetNamespace,
				"name":      selectedItem.name,
			}).Infof("Restored %d items out of an estimated total of %d (estimate will change throughout the restore)", len(ctx.restoredItems), actualTotalItems)
		}
	}

	// If we just restored custom resource definitions (CRDs), refresh
	// discovery because the restored CRDs may have created new APIs that
	// didn't previously exist in the cluster, and we want to be able to
	// resolve & restore instances of them in subsequent loop iterations.
	if groupResource == kuberesource.CustomResourceDefinitions {
		if err := ctx.discoveryHelper.Refresh(); err != nil {
			warnings.Add("", errors.Wrap(err, "refresh discovery after restoring CRDs"))
		}
	}
	return processedItems, warnings, errs
}

// getNamespace returns a namespace API object that we should attempt to
// create before restoring anything into it. It will come from the backup
// tarball if it exists, else will be a new one. If from the tarball, it
// will retain its labels, annotations, and spec.
func getNamespace(logger logrus.FieldLogger, path, remappedName string) *v1.Namespace {
	var nsBytes []byte
	var err error

	if nsBytes, err = ioutil.ReadFile(path); err != nil {
		return &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: remappedName,
			},
		}
	}

	var backupNS v1.Namespace
	if err := json.Unmarshal(nsBytes, &backupNS); err != nil {
		logger.Warnf("Error unmarshalling namespace from backup, creating new one.")
		return &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: remappedName,
			},
		}
	}

	return &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        remappedName,
			Labels:      backupNS.Labels,
			Annotations: backupNS.Annotations,
		},
		Spec: backupNS.Spec,
	}
}

func (ctx *restoreContext) getApplicableActions(groupResource schema.GroupResource, namespace string) []framework.RestoreItemResolvedActionV2 {
	var actions []framework.RestoreItemResolvedActionV2
	for _, action := range ctx.restoreItemActions {
		if action.ShouldUse(groupResource, namespace, nil, ctx.log) {
			actions = append(actions, action)
		}
	}
	return actions
}

func (ctx *restoreContext) getApplicableItemSnapshotters(groupResource schema.GroupResource, namespace string) []framework.ItemSnapshotterResolvedAction {
	var actions []framework.ItemSnapshotterResolvedAction
	for _, action := range ctx.itemSnapshotterActions {
		if action.ShouldUse(groupResource, namespace, nil, ctx.log) {
			actions = append(actions, action)
		}
	}

	return actions
}

func (ctx *restoreContext) shouldRestore(name string, pvClient client.Dynamic) (bool, error) {
	pvLogger := ctx.log.WithField("pvName", name)

	var shouldRestore bool
	err := wait.PollImmediate(time.Second, ctx.resourceTerminatingTimeout, func() (bool, error) {
		unstructuredPV, err := pvClient.Get(name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			pvLogger.Debug("PV not found, safe to restore")
			// PV not found, can safely exit loop and proceed with restore.
			shouldRestore = true
			return true, nil
		}
		if err != nil {
			return false, errors.Wrapf(err, "could not retrieve in-cluster copy of PV %s", name)
		}

		clusterPV := new(v1.PersistentVolume)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPV.Object, clusterPV); err != nil {
			return false, errors.Wrap(err, "error converting PV from unstructured")
		}

		if clusterPV.Status.Phase == v1.VolumeReleased || clusterPV.DeletionTimestamp != nil {
			// PV was found and marked for deletion, or it was released; wait for it to go away.
			pvLogger.Debugf("PV found, but marked for deletion, waiting")
			return false, nil
		}

		// Check for the namespace and PVC to see if anything that's referencing the PV is deleting.
		// If either the namespace or PVC is in a deleting/terminating state, wait for them to finish before
		// trying to restore the PV
		// Not doing so may result in the underlying PV disappearing but not restoring due to timing issues,
		// then the PVC getting restored and showing as lost.
		if clusterPV.Spec.ClaimRef == nil {
			pvLogger.Debugf("PV is not marked for deletion and is not claimed by a PVC")
			return true, nil
		}

		namespace := clusterPV.Spec.ClaimRef.Namespace
		pvcName := clusterPV.Spec.ClaimRef.Name

		// Have to create the PVC client here because we don't know what namespace we're using til we get to this point.
		// Using a dynamic client since it's easier to mock for testing
		pvcResource := metav1.APIResource{Name: "persistentvolumeclaims", Namespaced: true}
		pvcClient, err := ctx.dynamicFactory.ClientForGroupVersionResource(schema.GroupVersion{Group: "", Version: "v1"}, pvcResource, namespace)
		if err != nil {
			return false, errors.Wrapf(err, "error getting pvc client")
		}

		pvc, err := pvcClient.Get(pvcName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			pvLogger.Debugf("PVC %s for PV not found, waiting", pvcName)
			// PVC wasn't found, but the PV still exists, so continue to wait.
			return false, nil
		}
		if err != nil {
			return false, errors.Wrapf(err, "error getting claim %s for persistent volume", pvcName)
		}

		if pvc != nil && pvc.GetDeletionTimestamp() != nil {
			pvLogger.Debugf("PVC for PV marked for deletion, waiting")
			// PVC is still deleting, continue to wait.
			return false, nil
		}

		// Check the namespace associated with the claimRef to see if it's
		// deleting/terminating before proceeding.
		ns, err := ctx.namespaceClient.Get(go_context.TODO(), namespace, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			pvLogger.Debugf("namespace %s for PV not found, waiting", namespace)
			// Namespace not found but the PV still exists, so continue to wait.
			return false, nil
		}
		if err != nil {
			return false, errors.Wrapf(err, "error getting namespace %s associated with PV %s", namespace, name)
		}

		if ns != nil && (ns.GetDeletionTimestamp() != nil || ns.Status.Phase == v1.NamespaceTerminating) {
			pvLogger.Debugf("namespace %s associated with PV is deleting, waiting", namespace)
			// Namespace is in the process of deleting, keep looping.
			return false, nil
		}

		// None of the PV, PVC, or NS are marked for deletion, break the loop.
		pvLogger.Debug("PV, associated PVC and namespace are not marked for deletion")
		return true, nil
	})

	if err == wait.ErrWaitTimeout {
		pvLogger.Warn("timeout reached waiting for persistent volume to delete")
	}

	return shouldRestore, err
}

// crdAvailable waits for a CRD to be available for use before letting the
// restore continue.
func (ctx *restoreContext) crdAvailable(name string, crdClient client.Dynamic) (bool, error) {
	crdLogger := ctx.log.WithField("crdName", name)

	var available bool
	// Wait 1 minute rather than the standard resource timeout, since each CRD
	// will transition fairly quickly.
	err := wait.PollImmediate(time.Second, time.Minute*1, func() (bool, error) {
		unstructuredCRD, err := crdClient.Get(name, metav1.GetOptions{})
		if err != nil {
			return true, err
		}
		available, err = kube.IsCRDReady(unstructuredCRD)
		if err != nil {
			return true, err
		}

		if !available {
			crdLogger.Debug("CRD not yet ready for use")
		}

		// If the CRD is not available, keep polling (false, nil).
		// If the CRD is available, break the poll and return to caller (true, nil).
		return available, nil
	})

	if err == wait.ErrWaitTimeout {
		crdLogger.Debug("timeout reached waiting for custom resource definition to be ready")
	}

	return available, err
}

func (ctx *restoreContext) getResourceClient(groupResource schema.GroupResource, obj *unstructured.Unstructured, namespace string) (client.Dynamic, error) {
	key := resourceClientKey{
		resource:  groupResource.WithVersion(obj.GroupVersionKind().Version),
		namespace: namespace,
	}

	if client, ok := ctx.resourceClients[key]; ok {
		return client, nil
	}

	// Initialize client for this resource. We need metadata from an object to
	// do this.
	ctx.log.Infof("Getting client for %v", obj.GroupVersionKind())

	resource := metav1.APIResource{
		Namespaced: len(namespace) > 0,
		Name:       groupResource.Resource,
	}

	client, err := ctx.dynamicFactory.ClientForGroupVersionResource(obj.GroupVersionKind().GroupVersion(), resource, namespace)
	if err != nil {
		return nil, err
	}

	ctx.resourceClients[key] = client
	return client, nil
}

func getResourceID(groupResource schema.GroupResource, namespace, name string) string {
	if namespace == "" {
		return fmt.Sprintf("%s/%s", groupResource.String(), name)
	}

	return fmt.Sprintf("%s/%s/%s", groupResource.String(), namespace, name)
}

func (ctx *restoreContext) restoreItem(obj *unstructured.Unstructured, groupResource schema.GroupResource, namespace string) (Result, Result) {
	warnings, errs := Result{}, Result{}
	resourceID := getResourceID(groupResource, namespace, obj.GetName())

	// Check if group/resource should be restored. We need to do this here since
	// this method may be getting called for an additional item which is a group/resource
	// that's excluded.
	if !ctx.resourceIncludesExcludes.ShouldInclude(groupResource.String()) {
		ctx.log.WithFields(logrus.Fields{
			"namespace":     obj.GetNamespace(),
			"name":          obj.GetName(),
			"groupResource": groupResource.String(),
		}).Info("Not restoring item because resource is excluded")
		return warnings, errs
	}

	// Check if namespace/cluster-scoped resource should be restored. We need
	// to do this here since this method may be getting called for an additional
	// item which is in a namespace that's excluded, or which is cluster-scoped
	// and should be excluded. Note that we're checking the object's namespace (
	// via obj.GetNamespace()) instead of the namespace parameter, because we want
	// to check the *original* namespace, not the remapped one if it's been remapped.
	if namespace != "" {
		if !ctx.namespaceIncludesExcludes.ShouldInclude(obj.GetNamespace()) {
			ctx.log.WithFields(logrus.Fields{
				"namespace":     obj.GetNamespace(),
				"name":          obj.GetName(),
				"groupResource": groupResource.String(),
			}).Info("Not restoring item because namespace is excluded")
			return warnings, errs
		}

		// If the namespace scoped resource should be restored, ensure that the
		// namespace into which the resource is being restored into exists.
		// This is the *remapped* namespace that we are ensuring exists.
		nsToEnsure := getNamespace(ctx.log, archive.GetItemFilePath(ctx.restoreDir, "namespaces", "", obj.GetNamespace()), namespace)
		if _, nsCreated, err := kube.EnsureNamespaceExistsAndIsReady(nsToEnsure, ctx.namespaceClient, ctx.resourceTerminatingTimeout); err != nil {
			errs.AddVeleroError(err)
			return warnings, errs
		} else {
			// Add the newly created namespace to the list of restored items.
			if nsCreated {
				itemKey := velero.ResourceIdentifier{
					GroupResource: kuberesource.Namespaces,
					Namespace:     nsToEnsure.Namespace,
					Name:          nsToEnsure.Name,
				}
				ctx.restoredItems[itemKey] = struct{}{}
			}
		}
	} else {
		if boolptr.IsSetToFalse(ctx.restore.Spec.IncludeClusterResources) {
			ctx.log.WithFields(logrus.Fields{
				"namespace":     obj.GetNamespace(),
				"name":          obj.GetName(),
				"groupResource": groupResource.String(),
			}).Info("Not restoring item because it's cluster-scoped")
			return warnings, errs
		}
	}

	// Make a copy of object retrieved from backup to make it available unchanged
	//inside restore actions.
	itemFromBackup := obj.DeepCopy()

	complete, err := isCompleted(obj, groupResource)
	if err != nil {
		errs.Add(namespace, fmt.Errorf("error checking completion of %q: %v", resourceID, err))
		return warnings, errs
	}
	if complete {
		ctx.log.Infof("%s is complete - skipping", kube.NamespaceAndName(obj))
		return warnings, errs
	}

	name := obj.GetName()

	// Check if we've already restored this itemKey.
	itemKey := velero.ResourceIdentifier{
		GroupResource: groupResource,
		Namespace:     namespace,
		Name:          name,
	}
	if _, exists := ctx.restoredItems[itemKey]; exists {
		ctx.log.Infof("Skipping %s because it's already been restored.", resourceID)
		return warnings, errs
	}
	ctx.restoredItems[itemKey] = struct{}{}

	// TODO: move to restore item action if/when we add a ShouldRestore() method
	// to the interface.
	if groupResource == kuberesource.Pods && obj.GetAnnotations()[v1.MirrorPodAnnotationKey] != "" {
		ctx.log.Infof("Not restoring pod because it's a mirror pod")
		return warnings, errs
	}

	resourceClient, err := ctx.getResourceClient(groupResource, obj, namespace)
	if err != nil {
		errs.AddVeleroError(fmt.Errorf("error getting resource client for namespace %q, resource %q: %v", namespace, &groupResource, err))
		return warnings, errs
	}

	if groupResource == kuberesource.PersistentVolumes {
		switch {
		case hasSnapshot(name, ctx.volumeSnapshots):
			oldName := obj.GetName()
			shouldRenamePV, err := shouldRenamePV(ctx, obj, resourceClient)
			if err != nil {
				errs.Add(namespace, err)
				return warnings, errs
			}

			// Check to see if the claimRef.namespace field needs to be remapped,
			// and do so if necessary.
			_, err = remapClaimRefNS(ctx, obj)
			if err != nil {
				errs.Add(namespace, err)
				return warnings, errs
			}

			var shouldRestoreSnapshot bool
			if !shouldRenamePV {
				// Check if the PV exists in the cluster before attempting to create
				// a volume from the snapshot, in order to avoid orphaned volumes (GH #609)
				shouldRestoreSnapshot, err = ctx.shouldRestore(name, resourceClient)
				if err != nil {
					errs.Add(namespace, errors.Wrapf(err, "error waiting on in-cluster persistentvolume %s", name))
					return warnings, errs
				}
			} else {
				// If we're renaming the PV, we're going to give it a new random name,
				// so we can assume it doesn't already exist in the cluster and therefore
				// we should proceed with restoring from snapshot.
				shouldRestoreSnapshot = true
			}

			if shouldRestoreSnapshot {
				// Reset the PV's binding status so that Kubernetes can properly
				// associate it with the restored PVC.
				obj = resetVolumeBindingInfo(obj)

				// Even if we're renaming the PV, obj still has the old name here, because the pvRestorer
				// uses the original name to look up metadata about the snapshot.
				ctx.log.Infof("Restoring persistent volume from snapshot.")
				updatedObj, err := ctx.pvRestorer.executePVAction(obj)
				if err != nil {
					errs.Add(namespace, fmt.Errorf("error executing PVAction for %s: %v", resourceID, err))
					return warnings, errs
				}
				obj = updatedObj

				// VolumeSnapshotter has modified the PV name, we should rename the PV.
				if oldName != obj.GetName() {
					shouldRenamePV = true
				}
			}

			if shouldRenamePV {
				var pvName string
				if oldName == obj.GetName() {
					// pvRestorer hasn't modified the PV name, we need to rename the PV.
					pvName, err = ctx.pvRenamer(oldName)
					if err != nil {
						errs.Add(namespace, errors.Wrapf(err, "error renaming PV"))
						return warnings, errs
					}
				} else {
					// VolumeSnapshotter could have modified the PV name through
					// function `SetVolumeID`,
					pvName = obj.GetName()
				}

				ctx.renamedPVs[oldName] = pvName
				obj.SetName(pvName)

				// Add the original PV name as an annotation.
				annotations := obj.GetAnnotations()
				if annotations == nil {
					annotations = map[string]string{}
				}
				annotations["velero.io/original-pv-name"] = oldName
				obj.SetAnnotations(annotations)
			}

		case hasPodVolumeBackup(obj, ctx):
			ctx.log.Infof("Dynamically re-provisioning persistent volume because it has a pod volume backup to be restored.")
			ctx.pvsToProvision.Insert(name)

			// Return early because we don't want to restore the PV itself, we
			// want to dynamically re-provision it.
			return warnings, errs

		case hasDeleteReclaimPolicy(obj.Object):
			ctx.log.Infof("Dynamically re-provisioning persistent volume because it doesn't have a snapshot and its reclaim policy is Delete.")
			ctx.pvsToProvision.Insert(name)

			// Return early because we don't want to restore the PV itself, we
			// want to dynamically re-provision it.
			return warnings, errs

		default:
			ctx.log.Infof("Restoring persistent volume as-is because it doesn't have a snapshot and its reclaim policy is not Delete.")

			// Check to see if the claimRef.namespace field needs to be remapped, and do so if necessary.
			_, err = remapClaimRefNS(ctx, obj)
			if err != nil {
				errs.Add(namespace, err)
				return warnings, errs
			}
			obj = resetVolumeBindingInfo(obj)
			// We call the pvRestorer here to clear out the PV's claimRef.UID,
			// so it can be re-claimed when its PVC is restored and gets a new UID.
			updatedObj, err := ctx.pvRestorer.executePVAction(obj)
			if err != nil {
				errs.Add(namespace, fmt.Errorf("error executing PVAction for %s: %v", resourceID, err))
				return warnings, errs
			}
			obj = updatedObj
		}
	}

	objStatus, statusFieldExists, statusFieldErr := unstructured.NestedFieldCopy(obj.Object, "status")
	// Clear out non-core metadata fields and status.
	if obj, err = resetMetadataAndStatus(obj); err != nil {
		errs.Add(namespace, err)
		return warnings, errs
	}

	ctx.log.Infof("restore status includes excludes: %+v", ctx.resourceStatusIncludesExcludes)

	for _, action := range ctx.getApplicableActions(groupResource, namespace) {
		if !action.Selector.Matches(labels.Set(obj.GetLabels())) {
			continue
		}

		ctx.log.Infof("Executing item action for %v", &groupResource)
		executeOutput, err := action.RestoreItemAction.Execute(&velero.RestoreItemActionExecuteInput{
			Item:           obj,
			ItemFromBackup: itemFromBackup,
			Restore:        ctx.restore,
		})
		if err != nil {
			errs.Add(namespace, fmt.Errorf("error preparing %s: %v", resourceID, err))
			return warnings, errs
		}

		if executeOutput.SkipRestore {
			ctx.log.Infof("Skipping restore of %s: %v because a registered plugin discarded it", obj.GroupVersionKind().Kind, name)
			return warnings, errs
		}
		unstructuredObj, ok := executeOutput.UpdatedItem.(*unstructured.Unstructured)
		if !ok {
			errs.Add(namespace, fmt.Errorf("%s: unexpected type %T", resourceID, executeOutput.UpdatedItem))
			return warnings, errs
		}

		obj = unstructuredObj

		for _, additionalItem := range executeOutput.AdditionalItems {
			itemPath := archive.GetItemFilePath(ctx.restoreDir, additionalItem.GroupResource.String(), additionalItem.Namespace, additionalItem.Name)

			if _, err := ctx.fileSystem.Stat(itemPath); err != nil {
				ctx.log.WithError(err).WithFields(logrus.Fields{
					"additionalResource":          additionalItem.GroupResource.String(),
					"additionalResourceNamespace": additionalItem.Namespace,
					"additionalResourceName":      additionalItem.Name,
				}).Warn("unable to restore additional item")
				warnings.Add(additionalItem.Namespace, err)

				continue
			}

			additionalResourceID := getResourceID(additionalItem.GroupResource, additionalItem.Namespace, additionalItem.Name)
			additionalObj, err := archive.Unmarshal(ctx.fileSystem, itemPath)
			if err != nil {
				errs.Add(namespace, errors.Wrapf(err, "error restoring additional item %s", additionalResourceID))
			}

			additionalItemNamespace := additionalItem.Namespace
			if additionalItemNamespace != "" {
				if remapped, ok := ctx.restore.Spec.NamespaceMapping[additionalItemNamespace]; ok {
					additionalItemNamespace = remapped
				}
			}

			w, e := ctx.restoreItem(additionalObj, additionalItem.GroupResource, additionalItemNamespace)
			warnings.Merge(&w)
			errs.Merge(&e)
		}
	}

	// This comes after running item actions because we have built-in actions that restore
	// a PVC's associated PV (if applicable). As part of the PV being restored, the 'pvsToProvision'
	// set may be inserted into, and this needs to happen *before* running the following block of logic.
	//
	// The side effect of this is that it's impossible for a user to write a restore item action that
	// adjusts this behavior (i.e. of resetting the PVC for dynamic provisioning if it claims a PV with
	// a reclaim policy of Delete and no snapshot). If/when that becomes an issue for users, we can
	// revisit. This would be easier with a multi-pass restore process.
	if groupResource == kuberesource.PersistentVolumeClaims {
		pvc := new(v1.PersistentVolumeClaim)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), pvc); err != nil {
			errs.Add(namespace, err)
			return warnings, errs
		}

		if pvc.Spec.VolumeName != "" {
			// This used to only happen with PVB volumes, but now always remove this binding metadata
			obj = resetVolumeBindingInfo(obj)

			// This is the case for PVB volumes, where we need to actually have an empty volume created instead of restoring one.
			// The assumption is that any PV in pvsToProvision doesn't have an associated snapshot.
			if ctx.pvsToProvision.Has(pvc.Spec.VolumeName) {
				ctx.log.Infof("Resetting PersistentVolumeClaim %s/%s for dynamic provisioning", namespace, name)
				unstructured.RemoveNestedField(obj.Object, "spec", "volumeName")
			}
		}

		if newName, ok := ctx.renamedPVs[pvc.Spec.VolumeName]; ok {
			ctx.log.Infof("Updating persistent volume claim %s/%s to reference renamed persistent volume (%s -> %s)", namespace, name, pvc.Spec.VolumeName, newName)
			if err := unstructured.SetNestedField(obj.Object, newName, "spec", "volumeName"); err != nil {
				errs.Add(namespace, err)
				return warnings, errs
			}
		}
	}

	// Necessary because we may have remapped the namespace if the namespace is
	// blank, don't create the key.
	originalNamespace := obj.GetNamespace()
	if namespace != "" {
		obj.SetNamespace(namespace)
	}

	// Label the resource with the restore's name and the restored backup's name
	// for easy identification of all cluster resources created by this restore
	// and which backup they came from.
	addRestoreLabels(obj, ctx.restore.Name, ctx.restore.Spec.BackupName)

	ctx.log.Infof("Attempting to restore %s: %v", obj.GroupVersionKind().Kind, name)
	createdObj, restoreErr := resourceClient.Create(obj)
	isAlreadyExistsError, err := isAlreadyExistsError(ctx, obj, restoreErr, resourceClient)
	if err != nil {
		errs.Add(namespace, err)
		return warnings, errs
	}

	// check if we want to treat the error as a warning, in some cases the creation call might not get executed due to object API validations
	// and Velero might not get the already exists error type but in reality the object already exists
	var fromCluster *unstructured.Unstructured

	if restoreErr != nil {
		// check for the existence of the object in cluster, if no error then it implies that object exists
		// and if err then we want to judge whether there is an existing error in the previous creation.
		// if so, we will return the 'get' error.
		// otherwise, we will return the original creation error.
		fromCluster, err = resourceClient.Get(name, metav1.GetOptions{})
		if err != nil && isAlreadyExistsError {
			ctx.log.Errorf("Error retrieving in-cluster version of %s: %v", kube.NamespaceAndName(obj), err)
			errs.Add(namespace, err)
			return warnings, errs
		}
	}

	if fromCluster != nil {
		// Remove insubstantial metadata.
		fromCluster, err = resetMetadataAndStatus(fromCluster)
		if err != nil {
			ctx.log.Infof("Error trying to reset metadata for %s: %v", kube.NamespaceAndName(obj), err)
			warnings.Add(namespace, err)
			return warnings, errs
		}

		// We know the object from the cluster won't have the backup/restore name
		// labels, so copy them from the object we attempted to restore.
		labels := obj.GetLabels()
		addRestoreLabels(fromCluster, labels[velerov1api.RestoreNameLabel], labels[velerov1api.BackupNameLabel])
		fromClusterWithLabels := fromCluster.DeepCopy() // saving the in-cluster object so that we can create label patch if overall patch fails

		if !equality.Semantic.DeepEqual(fromCluster, obj) {
			switch groupResource {
			case kuberesource.ServiceAccounts:
				desired, err := mergeServiceAccounts(fromCluster, obj)
				if err != nil {
					ctx.log.Infof("error merging secrets for ServiceAccount %s: %v", kube.NamespaceAndName(obj), err)
					warnings.Add(namespace, err)
					return warnings, errs
				}

				patchBytes, err := generatePatch(fromCluster, desired)
				if err != nil {
					ctx.log.Infof("error generating patch for ServiceAccount %s: %v", kube.NamespaceAndName(obj), err)
					warnings.Add(namespace, err)
					return warnings, errs
				}

				if patchBytes == nil {
					// In-cluster and desired state are the same, so move on to
					// the next item.
					return warnings, errs
				}

				_, err = resourceClient.Patch(name, patchBytes)
				if err != nil {
					warnings.Add(namespace, err)
					// check if there is existingResourcePolicy and if it is set to update policy
					if len(ctx.restore.Spec.ExistingResourcePolicy) > 0 && ctx.restore.Spec.ExistingResourcePolicy == velerov1api.PolicyTypeUpdate {
						// remove restore labels so that we apply the latest backup/restore names on the object via patch
						removeRestoreLabels(fromCluster)
						//try patching just the backup/restore labels
						warningsFromUpdate, errsFromUpdate := ctx.updateBackupRestoreLabels(fromCluster, fromClusterWithLabels, namespace, resourceClient)
						warnings.Merge(&warningsFromUpdate)
						errs.Merge(&errsFromUpdate)
					}
				} else {
					ctx.log.Infof("ServiceAccount %s successfully updated", kube.NamespaceAndName(obj))
				}
			default:
				// check for the presence of existingResourcePolicy
				if len(ctx.restore.Spec.ExistingResourcePolicy) > 0 {
					resourcePolicy := ctx.restore.Spec.ExistingResourcePolicy
					ctx.log.Infof("restore API has resource policy defined %s , executing restore workflow accordingly for changed resource %s %s", resourcePolicy, fromCluster.GroupVersionKind().Kind, kube.NamespaceAndName(fromCluster))

					// existingResourcePolicy is set as none, add warning
					if resourcePolicy == velerov1api.PolicyTypeNone {
						e := errors.Errorf("could not restore, %s %q already exists. Warning: the in-cluster version is different than the backed-up version.",
							obj.GetKind(), obj.GetName())
						warnings.Add(namespace, e)
						// existingResourcePolicy is set as update, attempt patch on the resource and add warning if it fails
					} else if resourcePolicy == velerov1api.PolicyTypeUpdate {
						// processing update as existingResourcePolicy
						warningsFromUpdateRP, errsFromUpdateRP := ctx.processUpdateResourcePolicy(fromCluster, fromClusterWithLabels, obj, namespace, resourceClient)
						warnings.Merge(&warningsFromUpdateRP)
						errs.Merge(&errsFromUpdateRP)
					}
				} else {
					// Preserved Velero behavior when existingResourcePolicy is not specified by the user
					e := errors.Errorf("could not restore, %s %q already exists. Warning: the in-cluster version is different than the backed-up version.",
						obj.GetKind(), obj.GetName())
					warnings.Add(namespace, e)
				}
			}
			return warnings, errs
		}

		//update backup/restore labels on the unchanged resources if existingResourcePolicy is set as update
		if ctx.restore.Spec.ExistingResourcePolicy == velerov1api.PolicyTypeUpdate {
			resourcePolicy := ctx.restore.Spec.ExistingResourcePolicy
			ctx.log.Infof("restore API has resource policy defined %s , executing restore workflow accordingly for unchanged resource %s %s ", resourcePolicy, obj.GroupVersionKind().Kind, kube.NamespaceAndName(fromCluster))
			// remove restore labels so that we apply the latest backup/restore names on the object via patch
			removeRestoreLabels(fromCluster)
			// try updating the backup/restore labels for the in-cluster object
			warningsFromUpdate, errsFromUpdate := ctx.updateBackupRestoreLabels(fromCluster, obj, namespace, resourceClient)
			warnings.Merge(&warningsFromUpdate)
			errs.Merge(&errsFromUpdate)
		}

		ctx.log.Infof("Restore of %s, %v skipped: it already exists in the cluster and is the same as the backed up version", obj.GroupVersionKind().Kind, name)
		return warnings, errs
	}

	// Error was something other than an AlreadyExists.
	if restoreErr != nil {
		ctx.log.Errorf("error restoring %s: %+v", name, restoreErr)
		errs.Add(namespace, fmt.Errorf("error restoring %s: %v", resourceID, restoreErr))
		return warnings, errs
	}

	shouldRestoreStatus := ctx.resourceStatusIncludesExcludes != nil && ctx.resourceStatusIncludesExcludes.ShouldInclude(groupResource.String())
	if shouldRestoreStatus && statusFieldErr != nil {
		err := fmt.Errorf("could not get status to be restored %s: %v", kube.NamespaceAndName(obj), statusFieldErr)
		ctx.log.Errorf(err.Error())
		errs.Add(namespace, err)
		return warnings, errs
	}
	ctx.log.Debugf("status field for %s: exists: %v, should restore: %v", groupResource, statusFieldExists, shouldRestoreStatus)
	// if it should restore status, run a UpdateStatus
	if statusFieldExists && shouldRestoreStatus {
		if err := unstructured.SetNestedField(obj.Object, objStatus, "status"); err != nil {
			ctx.log.Errorf("could not set status field %s: %v", kube.NamespaceAndName(obj), err)
			errs.Add(namespace, err)
			return warnings, errs
		}
		obj.SetResourceVersion(createdObj.GetResourceVersion())
		updated, err := resourceClient.UpdateStatus(obj, metav1.UpdateOptions{})
		if err != nil {
			ctx.log.Infof("status field update failed %s: %v", kube.NamespaceAndName(obj), err)
			warnings.Add(namespace, err)
		} else {
			createdObj = updated
		}
	}

	// restore the managedFields
	withoutManagedFields := createdObj.DeepCopy()
	createdObj.SetManagedFields(obj.GetManagedFields())
	patchBytes, err := generatePatch(withoutManagedFields, createdObj)
	if err != nil {
		ctx.log.Errorf("error generating patch for managed fields %s: %v", kube.NamespaceAndName(obj), err)
		errs.Add(namespace, err)
		return warnings, errs
	}
	if patchBytes != nil {
		if _, err = resourceClient.Patch(name, patchBytes); err != nil {
			ctx.log.Errorf("error patch for managed fields %s: %v", kube.NamespaceAndName(obj), err)
			errs.Add(namespace, err)
			return warnings, errs
		}
		ctx.log.Infof("the managed fields for %s is patched", kube.NamespaceAndName(obj))
	}

	if groupResource == kuberesource.Pods {
		pod := new(v1.Pod)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), pod); err != nil {
			errs.Add(namespace, err)
			return warnings, errs
		}

		// Do not create podvolumerestore when current restore excludes pv/pvc
		if ctx.resourceIncludesExcludes.ShouldInclude(kuberesource.PersistentVolumeClaims.String()) &&
			ctx.resourceIncludesExcludes.ShouldInclude(kuberesource.PersistentVolumes.String()) &&
			len(podvolume.GetVolumeBackupsForPod(ctx.podVolumeBackups, pod, originalNamespace)) > 0 {
			restorePodVolumeBackups(ctx, createdObj, originalNamespace)
		}
	}

	if groupResource == kuberesource.Pods {
		ctx.waitExec(createdObj)
	}

	// Wait for a CRD to be available for instantiating resources
	// before continuing.
	if groupResource == kuberesource.CustomResourceDefinitions {
		available, err := ctx.crdAvailable(name, resourceClient)
		if err != nil {
			errs.Add(namespace, errors.Wrapf(err, "error verifying custom resource definition is ready to use"))
		} else if !available {
			errs.Add(namespace, fmt.Errorf("CRD %s is not available to use for custom resources.", name))
		}
	}

	return warnings, errs
}

func isAlreadyExistsError(ctx *restoreContext, obj *unstructured.Unstructured, err error, client client.Dynamic) (bool, error) {
	if err == nil {
		return false, nil
	}
	if apierrors.IsAlreadyExists(err) {
		return true, nil
	}
	// The "invalid value error" or "internal error" rather than "already exists" error returns when restoring nodePort service in the following two cases:
	// 1. For NodePort service, the service has nodePort preservation while the same nodePort service already exists. - Get invalid value error
	// 2. For LoadBalancer service, the "healthCheckNodePort" already exists. - Get internal error
	// If this is the case, the function returns true to avoid reporting error.
	// Refer to https://github.com/vmware-tanzu/velero/issues/2308 for more details
	if obj.GetKind() != "Service" {
		return false, nil
	}
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok || statusErr.Status().Details == nil || len(statusErr.Status().Details.Causes) == 0 {
		return false, nil
	}
	// make sure all the causes are "port allocated" error
	for _, cause := range statusErr.Status().Details.Causes {
		if !strings.Contains(cause.Message, "provided port is already allocated") {
			return false, nil
		}
	}

	// the "already allocated" error may be caused by other services, check whether the expected service exists or not
	if _, err = client.Get(obj.GetName(), metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			ctx.log.Debugf("Service %s not found", kube.NamespaceAndName(obj))
			return false, nil
		}
		return false, errors.Wrapf(err, "Unable to get the service %s while checking the NodePort is already allocated error", kube.NamespaceAndName(obj))
	}

	ctx.log.Infof("Service %s exists, ignore the provided port is already allocated error", kube.NamespaceAndName(obj))
	return true, nil
}

// shouldRenamePV returns a boolean indicating whether a persistent volume should
// be given a new name before being restored, or an error if this cannot be determined.
// A persistent volume will be given a new name if and only if (a) a PV with the
// original name already exists in-cluster, and (b) in the backup, the PV is claimed
// by a PVC in a namespace that's being remapped during the restore.
func shouldRenamePV(ctx *restoreContext, obj *unstructured.Unstructured, client client.Dynamic) (bool, error) {
	if len(ctx.restore.Spec.NamespaceMapping) == 0 {
		ctx.log.Debugf("Persistent volume does not need to be renamed because restore is not remapping any namespaces")
		return false, nil
	}

	pv := new(v1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, pv); err != nil {
		return false, errors.Wrapf(err, "error converting persistent volume to structured")
	}

	if pv.Spec.ClaimRef == nil {
		ctx.log.Debugf("Persistent volume does not need to be renamed because it's not claimed")
		return false, nil
	}

	if _, ok := ctx.restore.Spec.NamespaceMapping[pv.Spec.ClaimRef.Namespace]; !ok {
		ctx.log.Debugf("Persistent volume does not need to be renamed because it's not claimed by a PVC in a namespace that's being remapped")
		return false, nil
	}

	_, err := client.Get(pv.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		ctx.log.Debugf("Persistent volume does not need to be renamed because it does not exist in the cluster")
		return false, nil
	case err != nil:
		return false, errors.Wrapf(err, "error checking if persistent volume exists in the cluster")
	}

	// No error returned: the PV was found in-cluster, so we need to rename it.
	return true, nil
}

// remapClaimRefNS remaps a PersistentVolume's claimRef.Namespace based on a
// restore's NamespaceMappings, if necessary. Returns true if the namespace was
// remapped, false if it was not required.
func remapClaimRefNS(ctx *restoreContext, obj *unstructured.Unstructured) (bool, error) { //nolint:unparam
	if len(ctx.restore.Spec.NamespaceMapping) == 0 {
		ctx.log.Debug("Persistent volume does not need to have the claimRef.namespace remapped because restore is not remapping any namespaces")
		return false, nil
	}

	// Conversion to the real type here is more readable than all the error checking
	// involved with reading each field individually.
	pv := new(v1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, pv); err != nil {
		return false, errors.Wrapf(err, "error converting persistent volume to structured")
	}

	if pv.Spec.ClaimRef == nil {
		ctx.log.Debugf("Persistent volume does not need to have the claimRef.namespace remapped because it's not claimed")
		return false, nil
	}

	targetNS, ok := ctx.restore.Spec.NamespaceMapping[pv.Spec.ClaimRef.Namespace]

	if !ok {
		ctx.log.Debugf("Persistent volume does not need to have the claimRef.namespace remapped because it's not claimed by a PVC in a namespace that's being remapped")
		return false, nil
	}

	err := unstructured.SetNestedField(obj.Object, targetNS, "spec", "claimRef", "namespace")
	if err != nil {
		return false, err
	}
	ctx.log.Debug("Persistent volume's namespace was updated")
	return true, nil
}

// restorePodVolumeBackups restores the PodVolumeBackups for the given restored pod
func restorePodVolumeBackups(ctx *restoreContext, createdObj *unstructured.Unstructured, originalNamespace string) {
	if ctx.podVolumeRestorer == nil {
		ctx.log.Warn("No pod volume restorer, not restoring pod's volumes")
	} else {
		ctx.podVolumeWaitGroup.Add(1)
		go func() {
			// Done() will only be called after all errors have been successfully
			// sent on the ctx.podVolumeErrs channel
			defer ctx.podVolumeWaitGroup.Done()

			pod := new(v1.Pod)
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(createdObj.UnstructuredContent(), &pod); err != nil {
				ctx.log.WithError(err).Error("error converting unstructured pod")
				ctx.podVolumeErrs <- err
				return
			}

			data := podvolume.RestoreData{
				Restore:          ctx.restore,
				Pod:              pod,
				PodVolumeBackups: ctx.podVolumeBackups,
				SourceNamespace:  originalNamespace,
				BackupLocation:   ctx.backup.Spec.StorageLocation,
			}
			if errs := ctx.podVolumeRestorer.RestorePodVolumes(data); errs != nil {
				ctx.log.WithError(kubeerrs.NewAggregate(errs)).Error("unable to successfully complete pod volume restores of pod's volumes")

				for _, err := range errs {
					ctx.podVolumeErrs <- err
				}
			}
		}()
	}
}

// waitExec executes hooks in a restored pod's containers when they become ready.
func (ctx *restoreContext) waitExec(createdObj *unstructured.Unstructured) {
	ctx.hooksWaitGroup.Add(1)
	go func() {
		// Done() will only be called after all errors have been successfully sent
		// on the ctx.podVolumeErrs channel.
		defer ctx.hooksWaitGroup.Done()

		pod := new(v1.Pod)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(createdObj.UnstructuredContent(), &pod); err != nil {
			ctx.log.WithError(err).Error("error converting unstructured pod")
			ctx.hooksErrs <- err
			return
		}
		execHooksByContainer, err := hook.GroupRestoreExecHooks(
			ctx.resourceRestoreHooks,
			pod,
			ctx.log,
		)
		if err != nil {
			ctx.log.WithError(err).Errorf("error getting exec hooks for pod %s/%s", pod.Namespace, pod.Name)
			ctx.hooksErrs <- err
			return
		}

		if errs := ctx.waitExecHookHandler.HandleHooks(ctx.hooksContext, ctx.log, pod, execHooksByContainer); len(errs) > 0 {
			ctx.log.WithError(kubeerrs.NewAggregate(errs)).Error("unable to successfully execute post-restore hooks")
			ctx.hooksCancelFunc()

			for _, err := range errs {
				// Errors are already logged in the HandleHooks method.
				ctx.hooksErrs <- err
			}
		}
	}()
}

func hasSnapshot(pvName string, snapshots []*volume.Snapshot) bool {
	for _, snapshot := range snapshots {
		if snapshot.Spec.PersistentVolumeName == pvName {
			return true
		}
	}

	return false
}

func hasPodVolumeBackup(unstructuredPV *unstructured.Unstructured, ctx *restoreContext) bool {
	if len(ctx.podVolumeBackups) == 0 {
		return false
	}

	pv := new(v1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPV.Object, pv); err != nil {
		ctx.log.WithError(err).Warnf("Unable to convert PV from unstructured to structured")
		return false
	}

	if pv.Spec.ClaimRef == nil {
		return false
	}

	var found bool
	for _, pvb := range ctx.podVolumeBackups {
		if pvb.Spec.Pod.Namespace == pv.Spec.ClaimRef.Namespace && pvb.GetAnnotations()[podvolume.PVCNameAnnotation] == pv.Spec.ClaimRef.Name {
			found = true
			break
		}
	}

	return found
}

func hasDeleteReclaimPolicy(obj map[string]interface{}) bool {
	policy, _, _ := unstructured.NestedString(obj, "spec", "persistentVolumeReclaimPolicy")
	return policy == string(v1.PersistentVolumeReclaimDelete)
}

// resetVolumeBindingInfo clears any necessary metadata out of a PersistentVolume
// or PersistentVolumeClaim that would make it ineligible to be re-bound by Velero.
func resetVolumeBindingInfo(obj *unstructured.Unstructured) *unstructured.Unstructured {
	// Clean out ClaimRef UID and resourceVersion, since this information is
	// highly unique.
	unstructured.RemoveNestedField(obj.Object, "spec", "claimRef", "uid")
	unstructured.RemoveNestedField(obj.Object, "spec", "claimRef", "resourceVersion")

	// Clear out any annotations used by the Kubernetes PV controllers to track
	// bindings.
	annotations := obj.GetAnnotations()

	// Upon restore, this new PV will look like a statically provisioned, manually-
	// bound volume rather than one bound by the controller, so remove the annotation
	// that signals that a controller bound it.
	delete(annotations, kube.KubeAnnBindCompleted)
	// Remove the annotation that signals that the PV is already bound; we want
	// the PV(C) controller to take the two objects and bind them again.
	delete(annotations, kube.KubeAnnBoundByController)

	// GetAnnotations returns a copy, so we have to set them again.
	obj.SetAnnotations(annotations)

	return obj
}

func resetMetadata(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	res, ok := obj.Object["metadata"]
	if !ok {
		return nil, errors.New("metadata not found")
	}
	metadata, ok := res.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("metadata was of type %T, expected map[string]interface{}", res)
	}

	for k := range metadata {
		switch k {
		case "generateName", "selfLink", "uid", "resourceVersion", "generation", "creationTimestamp", "deletionTimestamp",
			"deletionGracePeriodSeconds", "ownerReferences":
			delete(metadata, k)
		}
	}

	return obj, nil
}

func resetStatus(obj *unstructured.Unstructured) {
	unstructured.RemoveNestedField(obj.UnstructuredContent(), "status")
}

func resetMetadataAndStatus(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	_, err := resetMetadata(obj)
	if err != nil {
		return nil, err
	}
	resetStatus(obj)
	return obj, nil
}

// addRestoreLabels labels the provided object with the restore name and the
// restored backup's name.
func addRestoreLabels(obj metav1.Object, restoreName, backupName string) {
	labels := obj.GetLabels()

	if labels == nil {
		labels = make(map[string]string)
	}

	labels[velerov1api.BackupNameLabel] = label.GetValidName(backupName)
	labels[velerov1api.RestoreNameLabel] = label.GetValidName(restoreName)

	obj.SetLabels(labels)
}

// isCompleted returns whether or not an object is considered completed. Used to
// identify whether or not an object should be restored. Only Jobs or Pods are
// considered.
func isCompleted(obj *unstructured.Unstructured, groupResource schema.GroupResource) (bool, error) {
	switch groupResource {
	case kuberesource.Pods:
		phase, _, err := unstructured.NestedString(obj.UnstructuredContent(), "status", "phase")
		if err != nil {
			return false, errors.WithStack(err)
		}
		if phase == string(v1.PodFailed) || phase == string(v1.PodSucceeded) {
			return true, nil
		}

	case kuberesource.Jobs:
		ct, found, err := unstructured.NestedString(obj.UnstructuredContent(), "status", "completionTime")
		if err != nil {
			return false, errors.WithStack(err)
		}
		if found && ct != "" {
			return true, nil
		}
	}
	// Assume any other resource isn't complete and can be restored.
	return false, nil
}

// restoreableResource represents map of individual items of each resource
// identifier grouped by their original namespaces.
type restoreableResource struct {
	resource                 string
	selectedItemsByNamespace map[string][]restoreableItem
	totalItems               int
}

// restoreableItem represents an item by its target namespace contains enough
// information required to restore the item.
type restoreableItem struct {
	path            string
	targetNamespace string
	name            string
}

// getOrderedResourceCollection iterates over list of ordered resource
// identifiers, applies resource include/exclude criteria, and Kubernetes
// selectors to make a list of resources to be actually restored preserving the
// original order.
func (ctx *restoreContext) getOrderedResourceCollection(
	backupResources map[string]*archive.ResourceItems,
	restoreResourceCollection []restoreableResource,
	processedResources sets.String,
	resourcePriorities Priorities,
	includeAllResources bool,
) ([]restoreableResource, sets.String, Result, Result) {
	var warnings, errs Result
	// Iterate through an ordered list of resources to restore, checking each
	// one to see if it should be restored. Note that resources *may* be in this
	// list twice, i.e. once due to being a prioritized resource, and once due
	// to being in the backup tarball. We can't de-dupe this upfront, because
	// it's possible that items in the prioritized resources list may not be
	// fully resolved group-resource strings (e.g. may be specified as "po"
	// instead of "pods"), and we don't want to fully resolve them via discovery
	// until we reach them in the loop, because it is possible that the
	// resource/API itself is being restored via a custom resource definition,
	// meaning it's not available via discovery prior to beginning the restore.
	//
	// Since we keep track of the fully-resolved group-resources that we *have*
	// restored, we won't try to restore a resource twice even if it's in the
	// ordered list twice.
	var resourceList []string
	if includeAllResources {
		resourceList = getOrderedResources(resourcePriorities, backupResources)
	} else {
		resourceList = resourcePriorities.HighPriorities
	}
	for _, resource := range resourceList {
		// try to resolve the resource via discovery to a complete group/version/resource
		gvr, _, err := ctx.discoveryHelper.ResourceFor(schema.ParseGroupResource(resource).WithVersion(""))
		if err != nil {
			ctx.log.WithField("resource", resource).Infof("Skipping restore of resource because it cannot be resolved via discovery")
			continue
		}
		groupResource := gvr.GroupResource()

		// Check if we've already restored this resource (this would happen if
		// the resource we're currently looking at was already restored because
		// it was a prioritized resource, and now we're looking at it as part of
		// the backup contents).
		if processedResources.Has(groupResource.String()) {
			ctx.log.WithField("resource", groupResource.String()).Debugf("Skipping restore of resource because it's already been processed")
			continue
		}

		// Check if the resource should be restored according to the resource
		// includes/excludes.
		if !ctx.resourceIncludesExcludes.ShouldInclude(groupResource.String()) {
			ctx.log.WithField("resource", groupResource.String()).Infof("Skipping restore of resource because the restore spec excludes it")
			continue
		}

		// We don't want to explicitly restore namespace API objs because we'll handle
		// them as a special case prior to restoring anything into them
		if groupResource == kuberesource.Namespaces {
			continue
		}

		// Check if the resource is present in the backup
		resourceList := backupResources[groupResource.String()]
		if resourceList == nil {
			ctx.log.WithField("resource", groupResource.String()).Debugf("Skipping restore of resource because it's not present in the backup tarball")
			continue
		}

		// Iterate through each namespace that contains instances of the
		// resource and append to the list of to-be restored resources.
		for namespace, items := range resourceList.ItemsByNamespace {
			if namespace != "" && !ctx.namespaceIncludesExcludes.ShouldInclude(namespace) {
				ctx.log.Infof("Skipping namespace %s", namespace)
				continue
			}

			// get target namespace to restore into, if different
			// from source namespace
			targetNamespace := namespace
			if target, ok := ctx.restore.Spec.NamespaceMapping[namespace]; ok {
				targetNamespace = target
			}

			if targetNamespace == "" && boolptr.IsSetToFalse(ctx.restore.Spec.IncludeClusterResources) {
				ctx.log.Infof("Skipping resource %s because it's cluster-scoped", resource)
				continue
			}

			if targetNamespace == "" && !boolptr.IsSetToTrue(ctx.restore.Spec.IncludeClusterResources) && !ctx.namespaceIncludesExcludes.IncludeEverything() {
				ctx.log.Infof("Skipping resource %s because it's cluster-scoped and only specific namespaces are included in the restore", resource)
				continue
			}

			res, w, e := ctx.getSelectedRestoreableItems(groupResource.String(), targetNamespace, namespace, items)
			warnings.Merge(&w)
			errs.Merge(&e)

			restoreResourceCollection = append(restoreResourceCollection, res)
		}

		// record that we've restored the resource
		processedResources.Insert(groupResource.String())
	}
	return restoreResourceCollection, processedResources, warnings, errs
}

// getSelectedRestoreableItems applies Kubernetes selectors on individual items
// of each resource type to create a list of items which will be actually
// restored.
func (ctx *restoreContext) getSelectedRestoreableItems(resource, targetNamespace, originalNamespace string, items []string) (restoreableResource, Result, Result) {
	warnings, errs := Result{}, Result{}

	restorable := restoreableResource{
		resource: resource,
	}

	if restorable.selectedItemsByNamespace == nil {
		restorable.selectedItemsByNamespace = make(map[string][]restoreableItem)
	}

	if targetNamespace != "" {
		ctx.log.Infof("Resource '%s' will be restored into namespace '%s'", resource, targetNamespace)
	} else {
		ctx.log.Infof("Resource '%s' will be restored at cluster scope", resource)
	}

	// If the APIGroupVersionsFeatureFlag is enabled, the item path will be
	// updated to include the API group version that was chosen for restore. For
	// example, for "horizontalpodautoscalers.autoscaling", if v2beta1 is chosen
	// to be restored, then "horizontalpodautoscalers.autoscaling/v2beta1" will
	// be part of item path. Different versions would only have been stored
	// if the APIGroupVersionsFeatureFlag was enabled during backup. The
	// chosenGrpVersToRestore map would only be populated if
	// APIGroupVersionsFeatureFlag was enabled for restore and the minimum
	// required backup format version has been met.
	cgv, ok := ctx.chosenGrpVersToRestore[resource]
	if ok {
		resource = filepath.Join(resource, cgv.Dir)
	}

	for _, item := range items {
		itemPath := archive.GetItemFilePath(ctx.restoreDir, resource, originalNamespace, item)

		obj, err := archive.Unmarshal(ctx.fileSystem, itemPath)
		if err != nil {
			errs.Add(
				targetNamespace,
				fmt.Errorf(
					"error decoding %q: %v",
					strings.Replace(itemPath, ctx.restoreDir+"/", "", -1),
					err,
				),
			)
			continue
		}

		if !ctx.selector.Matches(labels.Set(obj.GetLabels())) {
			continue
		}

		// Processing OrLabelSelectors when specified in the restore request. LabelSelectors as well as OrLabelSelectors
		// cannot co-exist, only one of them can be specified
		var skipItem = false
		var skip = 0
		ctx.log.Debugf("orSelectors specified: %s for item: %s", ctx.OrSelectors, item)
		for _, s := range ctx.OrSelectors {
			if !s.Matches(labels.Set(obj.GetLabels())) {
				skip++
			}

			if len(ctx.OrSelectors) == skip && skip > 0 {
				ctx.log.Infof("setting skip flag to true for item: %s", item)
				skipItem = true
			}
		}

		if skipItem {
			ctx.log.Infof("restore orSelector labels did not match, skipping restore of item: %s", skipItem, item)
			continue
		}

		selectedItem := restoreableItem{
			path:            itemPath,
			name:            item,
			targetNamespace: targetNamespace,
		}
		restorable.selectedItemsByNamespace[originalNamespace] =
			append(restorable.selectedItemsByNamespace[originalNamespace], selectedItem)
		restorable.totalItems++
	}
	return restorable, warnings, errs
}

// removeRestoreLabels removes the restore name and the
// restored backup's name.
func removeRestoreLabels(obj metav1.Object) {
	labels := obj.GetLabels()

	if labels == nil {
		labels = make(map[string]string)
	}

	labels[velerov1api.BackupNameLabel] = ""
	labels[velerov1api.RestoreNameLabel] = ""

	obj.SetLabels(labels)
}

// updates the backup/restore labels
func (ctx *restoreContext) updateBackupRestoreLabels(fromCluster, fromClusterWithLabels *unstructured.Unstructured, namespace string, resourceClient client.Dynamic) (warnings, errs Result) {
	patchBytes, err := generatePatch(fromCluster, fromClusterWithLabels)
	if err != nil {
		ctx.log.Errorf("error generating patch for %s %s: %v", fromCluster.GroupVersionKind().Kind, kube.NamespaceAndName(fromCluster), err)
		errs.Add(namespace, err)
		return warnings, errs
	}

	if patchBytes == nil {
		// In-cluster and desired state are the same, so move on to
		// the next items
		ctx.log.Errorf("skipped updating backup/restore labels for %s %s: in-cluster and desired state are the same along-with the labels", fromCluster.GroupVersionKind().Kind, kube.NamespaceAndName(fromCluster))
		return warnings, errs
	}

	// try patching the in-cluster resource (with only latest backup/restore labels)
	_, err = resourceClient.Patch(fromCluster.GetName(), patchBytes)
	if err != nil {
		ctx.log.Errorf("backup/restore label patch attempt failed for %s %s: %v", fromCluster.GroupVersionKind(), kube.NamespaceAndName(fromCluster), err)
		errs.Add(namespace, err)
	} else {
		ctx.log.Infof("backup/restore labels successfully updated for %s %s", fromCluster.GroupVersionKind().Kind, kube.NamespaceAndName(fromCluster))
	}
	return warnings, errs
}

// function to process existingResourcePolicy as update, tries to patch the diff between in-cluster and restore obj first
// if the patch fails then tries to update the backup/restore labels for the in-cluster version
func (ctx *restoreContext) processUpdateResourcePolicy(fromCluster, fromClusterWithLabels, obj *unstructured.Unstructured, namespace string, resourceClient client.Dynamic) (warnings, errs Result) {
	ctx.log.Infof("restore API has existingResourcePolicy defined as update , executing restore workflow accordingly for changed resource %s %s ", obj.GroupVersionKind().Kind, kube.NamespaceAndName(fromCluster))
	ctx.log.Infof("attempting patch on %s %q", fromCluster.GetKind(), fromCluster.GetName())
	// remove restore labels so that we apply the latest backup/restore names on the object via patch
	removeRestoreLabels(fromCluster)
	patchBytes, err := generatePatch(fromCluster, obj)
	if err != nil {
		ctx.log.Errorf("error generating patch for %s %s: %v", obj.GroupVersionKind().Kind, kube.NamespaceAndName(obj), err)
		errs.Add(namespace, err)
		return warnings, errs
	}

	if patchBytes == nil {
		// In-cluster and desired state are the same, so move on to
		// the next items
		ctx.log.Errorf("skipped updating %s %s: in-cluster and desired state are the same", fromCluster.GroupVersionKind().Kind, kube.NamespaceAndName(fromCluster))
		return warnings, errs
	}

	// try patching the in-cluster resource (resource diff plus latest backup/restore labels)
	_, err = resourceClient.Patch(obj.GetName(), patchBytes)
	if err != nil {
		ctx.log.Warnf("patch attempt failed for %s %s: %v", fromCluster.GroupVersionKind(), kube.NamespaceAndName(fromCluster), err)
		warnings.Add(namespace, err)
		// try just patching the labels
		warningsFromUpdate, errsFromUpdate := ctx.updateBackupRestoreLabels(fromCluster, fromClusterWithLabels, namespace, resourceClient)
		warnings.Merge(&warningsFromUpdate)
		errs.Merge(&errsFromUpdate)
	} else {
		ctx.log.Infof("%s %s successfully updated", obj.GroupVersionKind().Kind, kube.NamespaceAndName(obj))
	}
	return warnings, errs
}
