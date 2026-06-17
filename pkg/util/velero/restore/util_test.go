package restore

import (
	"testing"

	"github.com/stretchr/testify/require"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

func TestIsResourcePolicyValid(t *testing.T) {
	require.True(t, IsResourcePolicyValid(string(velerov1api.ResourcePolicyTypeNone)))
	require.True(t, IsResourcePolicyValid(string(velerov1api.ResourcePolicyTypeUpdate)))
	require.False(t, IsResourcePolicyValid(""))
}
