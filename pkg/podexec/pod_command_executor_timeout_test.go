package podexec

import (
	"context"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	v1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

const timeoutTestPodJSON = `{
	"metadata": {"namespace": "ns", "name": "pod-1"},
	"spec": {"containers": [{"name": "container-1"}]}
}`

// contextAwareExecutor returns once its context is cancelled, like the SPDY executor does.
type contextAwareExecutor struct {
	cancelled     chan struct{}
	cancelledOnce bool
}

func (e *contextAwareExecutor) Stream(options remotecommand.StreamOptions) error { return nil }

func (e *contextAwareExecutor) StreamWithContext(ctx context.Context, options remotecommand.StreamOptions) error {
	<-ctx.Done()
	if !e.cancelledOnce {
		e.cancelledOnce = true
		close(e.cancelled)
	}
	return ctx.Err()
}

func newTimeoutTestExecutor(t *testing.T, exec remotecommand.Executor) (*defaultPodCommandExecutor, map[string]any) {
	t.Helper()

	clientConfig := &rest.Config{}
	poster := &mockPoster{}
	podCommandExecutor := NewPodCommandExecutor(clientConfig, poster).(*defaultPodCommandExecutor)

	factory := &mockStreamExecutorFactory{}
	podCommandExecutor.streamExecutorFactory = factory

	baseURL, _ := url.Parse("https://some.server")
	contentConfig := rest.ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "", Version: "v1"}}
	poster.On("Post").Return(rest.NewRequestWithClient(baseURL, "/api/v1", contentConfig, nil))
	factory.On("NewSPDYExecutor", clientConfig, "POST", mock.Anything).Return(exec, nil)

	pod, err := velerotest.GetAsMap(timeoutTestPodJSON)
	if err != nil {
		t.Fatal(err)
	}

	return podCommandExecutor, pod
}

func timeoutTestHook(timeout time.Duration) *v1.ExecHook {
	return &v1.ExecHook{
		Container: "container-1",
		Command:   []string{"sh", "-c", "sleep 60"},
		Timeout:   metav1.Duration{Duration: timeout},
	}
}

// A hook that times out must have its exec stream cancelled, otherwise the command keeps
// running on the API server after ExecutePodCommand has returned.
func TestExecutePodCommandCancelsStreamOnTimeout(t *testing.T) {
	exec := &contextAwareExecutor{cancelled: make(chan struct{})}
	podCommandExecutor, pod := newTimeoutTestExecutor(t, exec)

	err := podCommandExecutor.ExecutePodCommand(velerotest.NewLogger(), pod, "ns", "pod-1", "hookName", timeoutTestHook(100*time.Millisecond))
	if err == nil {
		t.Fatal("expected a timeout error")
	}

	select {
	case <-exec.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("stream was not cancelled after the hook timed out")
	}
}

// When the stream returns because the context expired, both select cases are ready and one
// is picked at random, so the reported error must not depend on which one wins.
func TestExecutePodCommandTimeoutErrorIsDeterministic(t *testing.T) {
	const rounds = 50

	messages := map[string]int{}
	for range rounds {
		exec := &contextAwareExecutor{cancelled: make(chan struct{})}
		podCommandExecutor, pod := newTimeoutTestExecutor(t, exec)

		err := podCommandExecutor.ExecutePodCommand(velerotest.NewLogger(), pod, "ns", "pod-1", "hookName", timeoutTestHook(time.Millisecond))
		if err == nil {
			t.Fatal("expected a timeout error")
		}
		messages[err.Error()]++
	}

	if len(messages) != 1 {
		t.Fatalf("expected one error message, got %d: %v", len(messages), messages)
	}
}

func TestExecutePodCommandDoesNotLeakOnTimeout(t *testing.T) {
	const rounds = 10

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	before := runtime.NumGoroutine()

	for range rounds {
		exec := &contextAwareExecutor{cancelled: make(chan struct{})}
		podCommandExecutor, pod := newTimeoutTestExecutor(t, exec)

		if err := podCommandExecutor.ExecutePodCommand(velerotest.NewLogger(), pod, "ns", "pod-1", "hookName", timeoutTestHook(50*time.Millisecond)); err == nil {
			t.Fatal("expected a timeout error")
		}
	}

	time.Sleep(time.Second)
	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	if leaked := runtime.NumGoroutine() - before; leaked >= rounds {
		t.Fatalf("%d goroutines leaked over %d timed out hooks", leaked, rounds)
	}
}
