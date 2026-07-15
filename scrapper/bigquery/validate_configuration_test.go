package bigquery

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

// serviceDisabledBody is the shape Google returns when a required API is
// disabled in the project — a 403 carrying a google.rpc.ErrorInfo detail whose
// reason is SERVICE_DISABLED and whose metadata names the service and an
// activation URL. Captured from a real Cloud Resource Manager API failure
// (project id anonymized).
const serviceDisabledBody = `{
  "error": {
    "code": 403,
    "message": "Cloud Resource Manager API has not been used in project 000000000000 before or it is disabled. Enable it by visiting https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview?project=000000000000 then retry. If you enabled this API recently, wait a few minutes for the action to propagate to our systems and retry.",
    "errors": [
      {
        "message": "Cloud Resource Manager API has not been used in project 000000000000 before or it is disabled.",
        "domain": "usageLimits",
        "reason": "accessNotConfigured"
      }
    ],
    "status": "PERMISSION_DENIED",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "SERVICE_DISABLED",
        "domain": "googleapis.com",
        "metadata": {
          "service": "cloudresourcemanager.googleapis.com",
          "consumer": "projects/000000000000",
          "activationUrl": "https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview?project=000000000000"
        }
      }
    ]
  }
}`

func googleErrFromBody(t *testing.T, status int, body string) error {
	t.Helper()
	res := &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
	err := googleapi.CheckResponse(res)
	require.Error(t, err)
	return err
}

// TestServiceDisabledFromErr_DetectsDisabledApi reproduces QUA-113: a disabled
// Cloud Resource Manager API surfaces as a raw 403 during ValidateConfiguration.
// The detector must recognize it and expose the service + activation URL so the
// caller can turn it into an actionable warning instead of a raw Google error.
func TestServiceDisabledFromErr_DetectsDisabledApi(t *testing.T) {
	err := googleErrFromBody(t, http.StatusForbidden, serviceDisabledBody)

	info, ok := serviceDisabledFromErr(err)
	require.True(t, ok, "SERVICE_DISABLED 403 must be detected")
	assert.Equal(t, "cloudresourcemanager.googleapis.com", info.service)
	assert.Equal(
		t,
		"https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview?project=000000000000",
		info.activationURL,
	)
}

// TestServiceDisabledFromErr_DetectsThroughWrap ensures pkg/errors wrapping does
// not hide the SERVICE_DISABLED detail (the scrapper wraps errors liberally).
func TestServiceDisabledFromErr_DetectsThroughWrap(t *testing.T) {
	err := errors.Wrap(googleErrFromBody(t, http.StatusForbidden, serviceDisabledBody), "validate")

	info, ok := serviceDisabledFromErr(err)
	require.True(t, ok)
	assert.Equal(t, "cloudresourcemanager.googleapis.com", info.service)
}

// TestServiceDisabledFromErr_IgnoresOtherErrors ensures ordinary permission
// denials (a plain 403 with no SERVICE_DISABLED detail) and nil are not
// misclassified — those must keep the existing "failed to test permissions" path.
func TestServiceDisabledFromErr_IgnoresOtherErrors(t *testing.T) {
	plain403 := `{"error":{"code":403,"message":"caller does not have permission","status":"PERMISSION_DENIED"}}`
	_, ok := serviceDisabledFromErr(googleErrFromBody(t, http.StatusForbidden, plain403))
	assert.False(t, ok, "plain 403 must not be treated as SERVICE_DISABLED")

	_, ok = serviceDisabledFromErr(nil)
	assert.False(t, ok, "nil error must not be treated as SERVICE_DISABLED")

	_, ok = serviceDisabledFromErr(errors.New("boom"))
	assert.False(t, ok, "non-Google error must not be treated as SERVICE_DISABLED")
}
