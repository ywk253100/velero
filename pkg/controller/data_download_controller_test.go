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

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgofake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/datapath"
	datapathmockes "github.com/vmware-tanzu/velero/pkg/datapath/mocks"
	"github.com/vmware-tanzu/velero/pkg/exposer"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/uploader"

	exposermockes "github.com/vmware-tanzu/velero/pkg/exposer/mocks"
)

const dataDownloadName string = "datadownload-1"

func dataDownloadBuilder() *builder.DataDownloadBuilder {
	return builder.ForDataDownload(velerov1api.DefaultNamespace, dataDownloadName).
		BackupStorageLocation("bsl-loc").
		DataMover("velero").
		SnapshotID("test-snapshot-id").TargetVolume(velerov2alpha1api.TargetVolumeSpec{
		PV:        "test-pv",
		PVC:       "test-pvc",
		Namespace: "test-ns",
	})
}

func initDataDownloadReconciler(objects []runtime.Object, needError ...bool) (*DataDownloadReconciler, error) {
	var errs []error = make([]error, 6)
	for k, isError := range needError {
		if k == 0 && isError {
			errs[0] = fmt.Errorf("Get error")
		} else if k == 1 && isError {
			errs[1] = fmt.Errorf("Create error")
		} else if k == 2 && isError {
			errs[2] = fmt.Errorf("Update error")
		} else if k == 3 && isError {
			errs[3] = fmt.Errorf("Patch error")
		} else if k == 4 && isError {
			errs[4] = apierrors.NewConflict(velerov2alpha1api.Resource("datadownload"), dataDownloadName, errors.New("conflict"))
		} else if k == 5 && isError {
			errs[5] = fmt.Errorf("List error")
		}
	}
	return initDataDownloadReconcilerWithError(objects, errs...)
}

func initDataDownloadReconcilerWithError(objects []runtime.Object, needError ...error) (*DataDownloadReconciler, error) {
	scheme := runtime.NewScheme()
	err := velerov1api.AddToScheme(scheme)
	if err != nil {
		return nil, err
	}
	err = velerov2alpha1api.AddToScheme(scheme)
	if err != nil {
		return nil, err
	}
	err = corev1.AddToScheme(scheme)
	if err != nil {
		return nil, err
	}

	fakeClient := &FakeClient{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
	}

	for k := range needError {
		if k == 0 {
			fakeClient.getError = needError[0]
		} else if k == 1 {
			fakeClient.createError = needError[1]
		} else if k == 2 {
			fakeClient.updateError = needError[2]
		} else if k == 3 {
			fakeClient.patchError = needError[3]
		} else if k == 4 {
			fakeClient.updateConflict = needError[4]
		} else if k == 5 {
			fakeClient.listError = needError[5]
		}
	}

	var fakeKubeClient *clientgofake.Clientset
	if len(objects) != 0 {
		fakeKubeClient = clientgofake.NewSimpleClientset(objects...)
	} else {
		fakeKubeClient = clientgofake.NewSimpleClientset()
	}

	fakeFS := velerotest.NewFakeFileSystem()
	pathGlob := fmt.Sprintf("/host_pods/%s/volumes/*/%s", "test-uid", "test-pvc")
	_, err = fakeFS.Create(pathGlob)
	if err != nil {
		return nil, err
	}

	credentialFileStore, err := credentials.NewNamespacedFileStore(
		fakeClient,
		velerov1api.DefaultNamespace,
		"/tmp/credentials",
		fakeFS,
	)
	if err != nil {
		return nil, err
	}

	dataPathMgr := datapath.NewManager(1)

	return NewDataDownloadReconciler(fakeClient, fakeKubeClient, dataPathMgr, nil, &credentials.CredentialGetter{FromFile: credentialFileStore}, "test_node", time.Minute*5, velerotest.NewLogger(), metrics.NewServerMetrics()), nil
}

func TestDataDownloadReconcile(t *testing.T) {
	daemonSet := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "velero",
			Name:      "node-agent",
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "DaemonSet",
			APIVersion: appsv1.SchemeGroupVersion.String(),
		},
		Spec: appsv1.DaemonSetSpec{},
	}

	tests := []struct {
		name              string
		dd                *velerov2alpha1api.DataDownload
		targetPVC         *corev1.PersistentVolumeClaim
		dataMgr           *datapath.Manager
		needErrs          []bool
		needCreateFSBR    bool
		isExposeErr       bool
		isGetExposeErr    bool
		isNilExposer      bool
		isFSBRInitErr     bool
		isFSBRRestoreErr  bool
		notNilExpose      bool
		notMockCleanUp    bool
		mockCancel        bool
		mockClose         bool
		expected          *velerov2alpha1api.DataDownload
		expectedStatusMsg string
		checkFunc         func(du velerov2alpha1api.DataDownload) bool
		expectedResult    *ctrl.Result
	}{
		{
			name:      "Unknown data download status",
			dd:        dataDownloadBuilder().Phase("Unknown").Cancel(true).Result(),
			targetPVC: builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
		},
		{
			name:              "Cancel data downloand in progress and patch data download error",
			dd:                dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseInProgress).Cancel(true).Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			needErrs:          []bool{false, false, false, true},
			needCreateFSBR:    true,
			expectedStatusMsg: "Patch error",
		},
		{
			name:       "Cancel data downloand in progress with empty FSBR",
			dd:         dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseInProgress).Cancel(true).Result(),
			targetPVC:  builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			mockCancel: true,
		},
		{
			name:           "Cancel data downloand in progress",
			dd:             dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseInProgress).Cancel(true).Result(),
			targetPVC:      builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			needCreateFSBR: true,
			mockCancel:     true,
		},
		{
			name:           "Error in data path is concurrent limited",
			dd:             dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			targetPVC:      builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			dataMgr:        datapath.NewManager(0),
			notNilExpose:   true,
			notMockCleanUp: true,
			expectedResult: &ctrl.Result{Requeue: true, RequeueAfter: time.Second * 5},
		},
		{
			name:              "Error getting volume directory name for pvc in pod",
			dd:                dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			notNilExpose:      true,
			mockClose:         true,
			expectedStatusMsg: "error identifying unique volume path on host",
		},
		{
			name:              "Unable to update status to in progress for data download",
			dd:                dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			needErrs:          []bool{false, false, false, true},
			notNilExpose:      true,
			notMockCleanUp:    true,
			expectedStatusMsg: "Patch error",
		},
		{
			name:              "accept DataDownload error",
			dd:                dataDownloadBuilder().Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			needErrs:          []bool{false, false, true, false},
			expectedStatusMsg: "Update error",
		},
		{
			name: "Not create target pvc",
			dd:   dataDownloadBuilder().Result(),
		},
		{
			name:              "Uninitialized dataDownload",
			dd:                dataDownloadBuilder().Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			isNilExposer:      true,
			expectedStatusMsg: "uninitialized generic exposer",
		},
		{
			name:      "DataDownload not created in velero default namespace",
			dd:        builder.ForDataDownload("test-ns", dataDownloadName).Result(),
			targetPVC: builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
		},
		{
			name:              "Failed to get dataDownload",
			dd:                builder.ForDataDownload("test-ns", dataDownloadName).Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			needErrs:          []bool{true, false, false, false},
			expectedStatusMsg: "Get error",
		},
		{
			name:      "Unsupported dataDownload type",
			dd:        dataDownloadBuilder().DataMover("Unsuppoorted type").Result(),
			targetPVC: builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
		},
		{
			name:      "Restore is exposed",
			dd:        dataDownloadBuilder().Result(),
			targetPVC: builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
		},
		{
			name:      "Get empty restore exposer",
			dd:        dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			targetPVC: builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
		},
		{
			name:              "Failed to get restore exposer",
			dd:                dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			expectedStatusMsg: "Error to get restore exposer",
			isGetExposeErr:    true,
		},
		{
			name:              "Error to start restore expose",
			dd:                dataDownloadBuilder().Result(),
			targetPVC:         builder.ForPersistentVolumeClaim("test-ns", "test-pvc").Result(),
			expectedStatusMsg: "Error to expose restore exposer",
			isExposeErr:       true,
		},
		{
			name:     "prepare timeout",
			dd:       dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).StartTimestamp(&metav1.Time{Time: time.Now().Add(-time.Minute * 5)}).Result(),
			expected: dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseFailed).Result(),
		},
		{
			name: "dataDownload with enabled cancel",
			dd: func() *velerov2alpha1api.DataDownload {
				dd := dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).Result()
				controllerutil.AddFinalizer(dd, DataUploadDownloadFinalizer)
				dd.DeletionTimestamp = &metav1.Time{Time: time.Now()}
				return dd
			}(),
			checkFunc: func(du velerov2alpha1api.DataDownload) bool {
				return du.Spec.Cancel
			},
			expected: dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).Result(),
		},
		{
			name: "dataDownload with remove finalizer and should not be retrieved",
			dd: func() *velerov2alpha1api.DataDownload {
				dd := dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseFailed).Cancel(true).Result()
				controllerutil.AddFinalizer(dd, DataUploadDownloadFinalizer)
				dd.DeletionTimestamp = &metav1.Time{Time: time.Now()}
				return dd
			}(),
			checkFunc: func(dd velerov2alpha1api.DataDownload) bool {
				return !controllerutil.ContainsFinalizer(&dd, DataUploadDownloadFinalizer)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var objs []runtime.Object
			if test.targetPVC != nil {
				objs = []runtime.Object{test.targetPVC, daemonSet}
			}
			r, err := initDataDownloadReconciler(objs, test.needErrs...)
			require.NoError(t, err)
			defer func() {
				r.client.Delete(ctx, test.dd, &kbclient.DeleteOptions{})
				if test.targetPVC != nil {
					r.client.Delete(ctx, test.targetPVC, &kbclient.DeleteOptions{})
				}
			}()

			ctx := context.Background()
			if test.dd.Namespace == velerov1api.DefaultNamespace {
				err = r.client.Create(ctx, test.dd)
				require.NoError(t, err)
			}

			if test.dataMgr != nil {
				r.dataPathMgr = test.dataMgr
			} else {
				r.dataPathMgr = datapath.NewManager(1)
			}

			datapath.FSBRCreator = func(string, string, kbclient.Client, string, datapath.Callbacks, logrus.FieldLogger) datapath.AsyncBR {
				fsBR := datapathmockes.NewAsyncBR(t)
				if test.mockCancel {
					fsBR.On("Cancel").Return()
				}

				if test.mockClose {
					fsBR.On("Close", mock.Anything).Return()
				}

				return fsBR
			}

			if test.isExposeErr || test.isGetExposeErr || test.isNilExposer || test.notNilExpose {
				if test.isNilExposer {
					r.restoreExposer = nil
				} else {
					r.restoreExposer = func() exposer.GenericRestoreExposer {
						ep := exposermockes.NewGenericRestoreExposer(t)
						if test.isExposeErr {
							ep.On("Expose", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("Error to expose restore exposer"))
						} else if test.notNilExpose {
							hostingPod := builder.ForPod("test-ns", "test-name").Volumes(&corev1.Volume{Name: "test-pvc"}).Result()
							hostingPod.ObjectMeta.SetUID("test-uid")
							ep.On("GetExposed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&exposer.ExposeResult{ByPod: exposer.ExposeByPod{HostingPod: hostingPod, VolumeName: "test-pvc"}}, nil)
						} else if test.isGetExposeErr {
							ep.On("GetExposed", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("Error to get restore exposer"))
						}

						if !test.notMockCleanUp {
							ep.On("CleanUp", mock.Anything, mock.Anything).Return()
						}
						return ep
					}()
				}
			}

			if test.needCreateFSBR {
				if fsBR := r.dataPathMgr.GetAsyncBR(test.dd.Name); fsBR == nil {
					_, err := r.dataPathMgr.CreateFileSystemBR(test.dd.Name, pVBRRequestor, ctx, r.client, velerov1api.DefaultNamespace, datapath.Callbacks{OnCancelled: r.OnDataDownloadCancelled}, velerotest.NewLogger())
					require.NoError(t, err)
				}
			}

			actualResult, err := r.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: velerov1api.DefaultNamespace,
					Name:      test.dd.Name,
				},
			})

			if test.expectedStatusMsg != "" {
				assert.Contains(t, err.Error(), test.expectedStatusMsg)
			} else {
				require.Nil(t, err)
			}

			require.NotNil(t, actualResult)

			if test.expectedResult != nil {
				assert.Equal(t, test.expectedResult.Requeue, actualResult.Requeue)
				assert.Equal(t, test.expectedResult.RequeueAfter, actualResult.RequeueAfter)
			}

			dd := velerov2alpha1api.DataDownload{}
			err = r.client.Get(ctx, kbclient.ObjectKey{
				Name:      test.dd.Name,
				Namespace: test.dd.Namespace,
			}, &dd)

			if test.expected != nil {
				require.NoError(t, err)
				assert.Equal(t, dd.Status.Phase, test.expected.Status.Phase)
			}

			if test.isGetExposeErr {
				assert.Contains(t, dd.Status.Message, test.expectedStatusMsg)
			}
			if test.dd.Namespace == velerov1api.DefaultNamespace {
				if controllerutil.ContainsFinalizer(test.dd, DataUploadDownloadFinalizer) {
					assert.True(t, true, apierrors.IsNotFound(err))
				} else {
					require.Nil(t, err)
				}
			} else {
				assert.True(t, true, apierrors.IsNotFound(err))
			}

			t.Logf("%s: \n %v \n", test.name, dd)
		})
	}
}

func TestOnDataDownloadFailed(t *testing.T) {
	for _, getErr := range []bool{true, false} {
		ctx := context.TODO()
		needErrs := []bool{getErr, false, false, false}
		r, err := initDataDownloadReconciler(nil, needErrs...)
		require.NoError(t, err)

		dd := dataDownloadBuilder().Result()
		namespace := dd.Namespace
		ddName := dd.Name
		// Add the DataDownload object to the fake client
		assert.NoError(t, r.client.Create(ctx, dd))
		r.OnDataDownloadFailed(ctx, namespace, ddName, fmt.Errorf("Failed to handle %v", ddName))
		updatedDD := &velerov2alpha1api.DataDownload{}
		if getErr {
			assert.Error(t, r.client.Get(ctx, types.NamespacedName{Name: ddName, Namespace: namespace}, updatedDD))
			assert.NotEqual(t, velerov2alpha1api.DataDownloadPhaseFailed, updatedDD.Status.Phase)
			assert.Equal(t, updatedDD.Status.StartTimestamp.IsZero(), true)
		} else {
			assert.NoError(t, r.client.Get(ctx, types.NamespacedName{Name: ddName, Namespace: namespace}, updatedDD))
			assert.Equal(t, velerov2alpha1api.DataDownloadPhaseFailed, updatedDD.Status.Phase)
			assert.Equal(t, updatedDD.Status.StartTimestamp.IsZero(), true)
		}
	}
}

func TestOnDataDownloadCancelled(t *testing.T) {
	for _, getErr := range []bool{true, false} {
		ctx := context.TODO()
		needErrs := []bool{getErr, false, false, false}
		r, err := initDataDownloadReconciler(nil, needErrs...)
		require.NoError(t, err)

		dd := dataDownloadBuilder().Result()
		namespace := dd.Namespace
		ddName := dd.Name
		// Add the DataDownload object to the fake client
		assert.NoError(t, r.client.Create(ctx, dd))
		r.OnDataDownloadCancelled(ctx, namespace, ddName)
		updatedDD := &velerov2alpha1api.DataDownload{}
		if getErr {
			assert.Error(t, r.client.Get(ctx, types.NamespacedName{Name: ddName, Namespace: namespace}, updatedDD))
			assert.NotEqual(t, velerov2alpha1api.DataDownloadPhaseFailed, updatedDD.Status.Phase)
			assert.Equal(t, updatedDD.Status.StartTimestamp.IsZero(), true)
		} else {
			assert.NoError(t, r.client.Get(ctx, types.NamespacedName{Name: ddName, Namespace: namespace}, updatedDD))
			assert.Equal(t, velerov2alpha1api.DataDownloadPhaseCanceled, updatedDD.Status.Phase)
			assert.Equal(t, updatedDD.Status.StartTimestamp.IsZero(), false)
			assert.Equal(t, updatedDD.Status.CompletionTimestamp.IsZero(), false)
		}
	}
}

func TestOnDataDownloadCompleted(t *testing.T) {
	tests := []struct {
		name            string
		emptyFSBR       bool
		isGetErr        bool
		rebindVolumeErr bool
	}{
		{
			name:            "Data download complete",
			emptyFSBR:       false,
			isGetErr:        false,
			rebindVolumeErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.TODO()
			needErrs := []bool{test.isGetErr, false, false, false}
			r, err := initDataDownloadReconciler(nil, needErrs...)
			r.restoreExposer = func() exposer.GenericRestoreExposer {
				ep := exposermockes.NewGenericRestoreExposer(t)
				if test.rebindVolumeErr {
					ep.On("RebindVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("Error to rebind volume"))

				} else {
					ep.On("RebindVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				}
				ep.On("CleanUp", mock.Anything, mock.Anything).Return()
				return ep
			}()

			require.NoError(t, err)
			dd := dataDownloadBuilder().Result()
			namespace := dd.Namespace
			ddName := dd.Name
			// Add the DataDownload object to the fake client
			assert.NoError(t, r.client.Create(ctx, dd))
			r.OnDataDownloadCompleted(ctx, namespace, ddName, datapath.Result{})
			updatedDD := &velerov2alpha1api.DataDownload{}
			if test.isGetErr {
				assert.Error(t, r.client.Get(ctx, types.NamespacedName{Name: ddName, Namespace: namespace}, updatedDD))
				assert.Equal(t, velerov2alpha1api.DataDownloadPhase(""), updatedDD.Status.Phase)
				assert.Equal(t, updatedDD.Status.CompletionTimestamp.IsZero(), true)
			} else {
				assert.NoError(t, r.client.Get(ctx, types.NamespacedName{Name: ddName, Namespace: namespace}, updatedDD))
				assert.Equal(t, velerov2alpha1api.DataDownloadPhaseCompleted, updatedDD.Status.Phase)
				assert.Equal(t, updatedDD.Status.CompletionTimestamp.IsZero(), false)
			}
		})
	}
}

func TestOnDataDownloadProgress(t *testing.T) {
	totalBytes := int64(1024)
	bytesDone := int64(512)
	tests := []struct {
		name     string
		dd       *velerov2alpha1api.DataDownload
		progress uploader.Progress
		needErrs []bool
	}{
		{
			name: "patch in progress phase success",
			dd:   dataDownloadBuilder().Result(),
			progress: uploader.Progress{
				TotalBytes: totalBytes,
				BytesDone:  bytesDone,
			},
		},
		{
			name:     "failed to get datadownload",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []bool{true, false, false, false},
		},
		{
			name:     "failed to patch datadownload",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []bool{false, false, false, true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.TODO()

			r, err := initDataDownloadReconciler(nil, test.needErrs...)
			require.NoError(t, err)
			defer func() {
				r.client.Delete(ctx, test.dd, &kbclient.DeleteOptions{})
			}()
			// Create a DataDownload object
			dd := dataDownloadBuilder().Result()
			namespace := dd.Namespace
			duName := dd.Name
			// Add the DataDownload object to the fake client
			assert.NoError(t, r.client.Create(context.Background(), dd))

			// Create a Progress object
			progress := &uploader.Progress{
				TotalBytes: totalBytes,
				BytesDone:  bytesDone,
			}

			// Call the OnDataDownloadProgress function
			r.OnDataDownloadProgress(ctx, namespace, duName, progress)
			if len(test.needErrs) != 0 && !test.needErrs[0] {
				// Get the updated DataDownload object from the fake client
				updatedDu := &velerov2alpha1api.DataDownload{}
				assert.NoError(t, r.client.Get(ctx, types.NamespacedName{Name: duName, Namespace: namespace}, updatedDu))
				// Assert that the DataDownload object has been updated with the progress
				assert.Equal(t, test.progress.TotalBytes, updatedDu.Status.Progress.TotalBytes)
				assert.Equal(t, test.progress.BytesDone, updatedDu.Status.Progress.BytesDone)
			}
		})
	}
}

func TestFindDataDownloadForPod(t *testing.T) {
	needErrs := []bool{false, false, false, false}
	r, err := initDataDownloadReconciler(nil, needErrs...)
	require.NoError(t, err)
	tests := []struct {
		name      string
		du        *velerov2alpha1api.DataDownload
		pod       *corev1.Pod
		checkFunc func(*velerov2alpha1api.DataDownload, []reconcile.Request)
	}{
		{
			name: "find dataDownload for pod",
			du:   dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).Result(),
			pod:  builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Labels(map[string]string{velerov1api.DataDownloadLabel: dataDownloadName}).Status(corev1.PodStatus{Phase: corev1.PodRunning}).Result(),
			checkFunc: func(du *velerov2alpha1api.DataDownload, requests []reconcile.Request) {
				// Assert that the function returns a single request
				assert.Len(t, requests, 1)
				// Assert that the request contains the correct namespaced name
				assert.Equal(t, du.Namespace, requests[0].Namespace)
				assert.Equal(t, du.Name, requests[0].Name)
			},
		}, {
			name: "no selected label found for pod",
			du:   dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).Result(),
			pod:  builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Result(),
			checkFunc: func(du *velerov2alpha1api.DataDownload, requests []reconcile.Request) {
				// Assert that the function returns a single request
				assert.Empty(t, requests)
			},
		}, {
			name: "no matched pod",
			du:   dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).Result(),
			pod:  builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Labels(map[string]string{velerov1api.DataDownloadLabel: "non-existing-datadownload"}).Result(),
			checkFunc: func(du *velerov2alpha1api.DataDownload, requests []reconcile.Request) {
				assert.Empty(t, requests)
			},
		},
		{
			name: "dataDownload not accept",
			du:   dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseInProgress).Result(),
			pod:  builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Labels(map[string]string{velerov1api.DataDownloadLabel: dataDownloadName}).Result(),
			checkFunc: func(du *velerov2alpha1api.DataDownload, requests []reconcile.Request) {
				assert.Empty(t, requests)
			},
		},
	}
	for _, test := range tests {
		ctx := context.Background()
		assert.NoError(t, r.client.Create(ctx, test.pod))
		assert.NoError(t, r.client.Create(ctx, test.du))
		// Call the findSnapshotRestoreForPod function
		requests := r.findSnapshotRestoreForPod(context.Background(), test.pod)
		test.checkFunc(test.du, requests)
		r.client.Delete(ctx, test.du, &kbclient.DeleteOptions{})
		if test.pod != nil {
			r.client.Delete(ctx, test.pod, &kbclient.DeleteOptions{})
		}
	}
}

func TestAcceptDataDownload(t *testing.T) {
	tests := []struct {
		name        string
		dd          *velerov2alpha1api.DataDownload
		needErrs    []error
		succeeded   bool
		expectedErr string
	}{
		{
			name:        "update fail",
			dd:          dataDownloadBuilder().Result(),
			needErrs:    []error{nil, nil, fmt.Errorf("fake-update-error"), nil},
			expectedErr: "fake-update-error",
		},
		{
			name:     "accepted by others",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []error{nil, nil, &fakeAPIStatus{metav1.StatusReasonConflict}, nil},
		},
		{
			name:      "succeed",
			dd:        dataDownloadBuilder().Result(),
			needErrs:  []error{nil, nil, nil, nil},
			succeeded: true,
		},
	}
	for _, test := range tests {
		ctx := context.Background()
		r, err := initDataDownloadReconcilerWithError(nil, test.needErrs...)
		require.NoError(t, err)

		err = r.client.Create(ctx, test.dd)
		require.NoError(t, err)

		succeeded, err := r.acceptDataDownload(ctx, test.dd)
		assert.Equal(t, test.succeeded, succeeded)
		if test.expectedErr == "" {
			assert.NoError(t, err)
		} else {
			assert.EqualError(t, err, test.expectedErr)
		}
	}
}

func TestOnDdPrepareTimeout(t *testing.T) {
	tests := []struct {
		name     string
		dd       *velerov2alpha1api.DataDownload
		needErrs []error
		expected *velerov2alpha1api.DataDownload
	}{
		{
			name:     "update fail",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []error{nil, nil, fmt.Errorf("fake-update-error"), nil},
			expected: dataDownloadBuilder().Result(),
		},
		{
			name:     "update interrupted",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []error{nil, nil, &fakeAPIStatus{metav1.StatusReasonConflict}, nil},
			expected: dataDownloadBuilder().Result(),
		},
		{
			name:     "succeed",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []error{nil, nil, nil, nil},
			expected: dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseFailed).Result(),
		},
	}
	for _, test := range tests {
		ctx := context.Background()
		r, err := initDataDownloadReconcilerWithError(nil, test.needErrs...)
		require.NoError(t, err)

		err = r.client.Create(ctx, test.dd)
		require.NoError(t, err)

		r.onPrepareTimeout(ctx, test.dd)

		dd := velerov2alpha1api.DataDownload{}
		_ = r.client.Get(ctx, kbclient.ObjectKey{
			Name:      test.dd.Name,
			Namespace: test.dd.Namespace,
		}, &dd)

		assert.Equal(t, test.expected.Status.Phase, dd.Status.Phase)
	}
}

func TestTryCancelDataDownload(t *testing.T) {
	tests := []struct {
		name        string
		dd          *velerov2alpha1api.DataDownload
		needErrs    []error
		succeeded   bool
		expectedErr string
	}{
		{
			name:     "update fail",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []error{nil, nil, fmt.Errorf("fake-update-error"), nil},
		},
		{
			name:     "cancel by others",
			dd:       dataDownloadBuilder().Result(),
			needErrs: []error{nil, nil, &fakeAPIStatus{metav1.StatusReasonConflict}, nil},
		},
		{
			name:      "succeed",
			dd:        dataDownloadBuilder().Result(),
			needErrs:  []error{nil, nil, nil, nil},
			succeeded: true,
		},
	}
	for _, test := range tests {
		ctx := context.Background()
		r, err := initDataDownloadReconcilerWithError(nil, test.needErrs...)
		require.NoError(t, err)

		err = r.client.Create(ctx, test.dd)
		require.NoError(t, err)

		r.TryCancelDataDownload(ctx, test.dd)

		if test.expectedErr == "" {
			assert.NoError(t, err)
		} else {
			assert.EqualError(t, err, test.expectedErr)
		}
	}
}

func TestUpdateDataDownloadWithRetry(t *testing.T) {

	namespacedName := types.NamespacedName{
		Name:      dataDownloadName,
		Namespace: "velero",
	}

	// Define test cases
	testCases := []struct {
		Name      string
		needErrs  []bool
		ExpectErr bool
	}{
		{
			Name:      "SuccessOnFirstAttempt",
			needErrs:  []bool{false, false, false, false},
			ExpectErr: false,
		},
		{
			Name:      "Error get",
			needErrs:  []bool{true, false, false, false, false},
			ExpectErr: true,
		},
		{
			Name:      "Error update",
			needErrs:  []bool{false, false, true, false, false},
			ExpectErr: true,
		},
		{
			Name:      "Conflict with error timeout",
			needErrs:  []bool{false, false, false, false, true},
			ExpectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx, cancelFunc := context.WithTimeout(context.TODO(), time.Second*5)
			defer cancelFunc()
			r, err := initDataDownloadReconciler(nil, tc.needErrs...)
			require.NoError(t, err)
			err = r.client.Create(ctx, dataDownloadBuilder().Result())
			require.NoError(t, err)
			updateFunc := func(dataDownload *velerov2alpha1api.DataDownload) {
				dataDownload.Spec.Cancel = true
			}
			err = UpdateDataDownloadWithRetry(ctx, r.client, namespacedName, velerotest.NewLogger().WithField("name", tc.Name), updateFunc)
			if tc.ExpectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFindDataDownloads(t *testing.T) {
	tests := []struct {
		name            string
		pod             corev1.Pod
		du              *velerov2alpha1api.DataDownload
		expectedUploads []velerov2alpha1api.DataDownload
		expectedError   bool
	}{
		// Test case 1: Pod with matching nodeName and DataDownload label
		{
			name: "MatchingPod",
			pod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "velero",
					Name:      "pod-1",
					Labels: map[string]string{
						velerov1api.DataDownloadLabel: dataDownloadName,
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "node-1",
				},
			},
			du: dataDownloadBuilder().Result(),
			expectedUploads: []velerov2alpha1api.DataDownload{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "velero",
						Name:      dataDownloadName,
					},
				},
			},
			expectedError: false,
		},
		// Test case 2: Pod with non-matching nodeName
		{
			name: "NonMatchingNodePod",
			pod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "velero",
					Name:      "pod-2",
					Labels: map[string]string{
						velerov1api.DataDownloadLabel: dataDownloadName,
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "node-2",
				},
			},
			du:              dataDownloadBuilder().Result(),
			expectedUploads: []velerov2alpha1api.DataDownload{},
			expectedError:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, err := initDataDownloadReconcilerWithError(nil)
			require.NoError(t, err)
			r.nodeName = "node-1"
			err = r.client.Create(ctx, test.du)
			require.NoError(t, err)
			err = r.client.Create(ctx, &test.pod)
			require.NoError(t, err)
			uploads, err := r.FindDataDownloads(context.Background(), r.client, "velero")

			if test.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(test.expectedUploads), len(uploads))
			}
		})
	}
}

func TestAttemptDataDownloadResume(t *testing.T) {
	tests := []struct {
		name                   string
		dataUploads            []velerov2alpha1api.DataDownload
		du                     *velerov2alpha1api.DataDownload
		pod                    *corev1.Pod
		needErrs               []bool
		acceptedDataDownloads  []string
		prepareddDataDownloads []string
		cancelledDataDownloads []string
		expectedError          bool
	}{
		// Test case 1: Process Accepted DataDownload
		{
			name: "AcceptedDataDownload",
			pod: builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Volumes(&corev1.Volume{Name: dataDownloadName}).NodeName("node-1").Labels(map[string]string{
				velerov1api.DataDownloadLabel: dataDownloadName,
			}).Result(),
			du:                    dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).Result(),
			acceptedDataDownloads: []string{dataDownloadName},
			expectedError:         false,
		},
		// Test case 2: Cancel an Accepted DataDownload
		{
			name: "CancelAcceptedDataDownload",
			du:   dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhaseAccepted).Result(),
		},
		// Test case 3: Process Accepted Prepared DataDownload
		{
			name: "PreparedDataDownload",
			pod: builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Volumes(&corev1.Volume{Name: dataDownloadName}).NodeName("node-1").Labels(map[string]string{
				velerov1api.DataDownloadLabel: dataDownloadName,
			}).Result(),
			du:                     dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			prepareddDataDownloads: []string{dataDownloadName},
		},
		// Test case 4: Process Accepted InProgress DataDownload
		{
			name: "InProgressDataDownload",
			pod: builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Volumes(&corev1.Volume{Name: dataDownloadName}).NodeName("node-1").Labels(map[string]string{
				velerov1api.DataDownloadLabel: dataDownloadName,
			}).Result(),
			du:                     dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			prepareddDataDownloads: []string{dataDownloadName},
		},
		// Test case 5: get resume error
		{
			name: "ResumeError",
			pod: builder.ForPod(velerov1api.DefaultNamespace, dataDownloadName).Volumes(&corev1.Volume{Name: dataDownloadName}).NodeName("node-1").Labels(map[string]string{
				velerov1api.DataDownloadLabel: dataDownloadName,
			}).Result(),
			needErrs:      []bool{false, false, false, false, false, true},
			du:            dataDownloadBuilder().Phase(velerov2alpha1api.DataDownloadPhasePrepared).Result(),
			expectedError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.TODO()
			r, err := initDataDownloadReconciler(nil, test.needErrs...)
			r.nodeName = "node-1"
			require.NoError(t, err)
			defer func() {
				r.client.Delete(ctx, test.du, &kbclient.DeleteOptions{})
				if test.pod != nil {
					r.client.Delete(ctx, test.pod, &kbclient.DeleteOptions{})
				}
			}()

			assert.NoError(t, r.client.Create(ctx, test.du))
			if test.pod != nil {
				assert.NoError(t, r.client.Create(ctx, test.pod))
			}
			// Run the test
			err = r.AttemptDataDownloadResume(ctx, r.client, r.logger.WithField("name", test.name), test.du.Namespace)

			if test.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify DataDownload marked as Cancelled
				for _, duName := range test.cancelledDataDownloads {
					dataUpload := &velerov2alpha1api.DataDownload{}
					err := r.client.Get(context.Background(), types.NamespacedName{Namespace: "velero", Name: duName}, dataUpload)
					require.NoError(t, err)
					assert.Equal(t, velerov2alpha1api.DataDownloadPhaseCanceled, dataUpload.Status.Phase)
				}
				// Verify DataDownload marked as Accepted
				for _, duName := range test.acceptedDataDownloads {
					dataUpload := &velerov2alpha1api.DataDownload{}
					err := r.client.Get(context.Background(), types.NamespacedName{Namespace: "velero", Name: duName}, dataUpload)
					require.NoError(t, err)
					assert.Equal(t, velerov2alpha1api.DataDownloadPhaseAccepted, dataUpload.Status.Phase)
				}
				// Verify DataDownload marked as Prepared
				for _, duName := range test.prepareddDataDownloads {
					dataUpload := &velerov2alpha1api.DataDownload{}
					err := r.client.Get(context.Background(), types.NamespacedName{Namespace: "velero", Name: duName}, dataUpload)
					require.NoError(t, err)
					assert.Equal(t, velerov2alpha1api.DataDownloadPhasePrepared, dataUpload.Status.Phase)
				}
			}
		})
	}
}
