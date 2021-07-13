package e2e

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/pkg/errors"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func waitUntilServiceAccountCreated(ctx context.Context, client testClient, namespace, serviceAccount string, timeout time.Duration) error {
	return wait.PollImmediate(5*time.Second, timeout,
		func() (bool, error) {
			if _, err := client.clientGo.CoreV1().ServiceAccounts(namespace).Get(ctx, serviceAccount, metav1.GetOptions{}); err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
				return false, nil
			}
			return true, nil
		})
}

func patchServiceAccountWithImagePullSecret(ctx context.Context, client testClient, namespace, serviceAccount, dockerCredentialFile string) error {
	credential, err := ioutil.ReadFile(dockerCredentialFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read the docker credential file %q", dockerCredentialFile)
	}
	secretName := "image-pull-secret"
	secret := builder.ForSecret(namespace, secretName).Data(map[string][]byte{".dockerconfigjson": credential}).Result()
	secret.Type = corev1.SecretTypeDockerConfigJson
	if _, err = client.clientGo.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return errors.Wrapf(err, "failed to create secret %q under namespace %q", secretName, namespace)
	}

	if _, err = client.clientGo.CoreV1().ServiceAccounts(namespace).Patch(ctx, serviceAccount, types.StrategicMergePatchType,
		[]byte(fmt.Sprintf(`{"imagePullSecrets": [{"name": "%s"}]}`, secretName)), metav1.PatchOptions{}); err != nil {
		return errors.Wrapf(err, "failed to patch the service account %q under the namespace %q", serviceAccount, namespace)
	}
	return nil
}
