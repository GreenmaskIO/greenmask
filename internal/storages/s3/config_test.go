package s3

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate_SSEModes(t *testing.T) {
	for _, mode := range []string{"", "AES256", "aws:kms", "aws:kms:dsse"} {
		t.Run("accepts "+mode, func(t *testing.T) {
			require.NoError(t, (&Config{SSE: mode}).Validate())
		})
	}

	for _, mode := range []string{"aes256", "AWS:KMS", "kms", "none", "sse-c"} {
		t.Run("rejects "+mode, func(t *testing.T) {
			err := (&Config{SSE: mode}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unknown sse")
		})
	}
}

func TestConfigValidate_KMSKeyARN(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:123456789012:key/test-key"

	for _, mode := range []string{"aws:kms", "aws:kms:dsse"} {
		t.Run("allowed with "+mode, func(t *testing.T) {
			require.NoError(t, (&Config{SSE: mode, KMSKeyARN: arn}).Validate())
		})
	}

	// Silently dropping the key would leave the dump encrypted by something
	// other than the key the user asked for, so it must fail loudly instead.
	for _, mode := range []string{"", "AES256"} {
		t.Run("rejected with sse "+mode, func(t *testing.T) {
			err := (&Config{SSE: mode, KMSKeyARN: arn}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "kms_key_arn requires sse")
		})
	}
}

func TestConfigValidate_BucketKeyEnabled(t *testing.T) {
	for _, mode := range []string{"aws:kms", "aws:kms:dsse"} {
		t.Run("allowed with "+mode, func(t *testing.T) {
			require.NoError(t, (&Config{SSE: mode, BucketKeyEnabled: true}).Validate())
		})
	}

	for _, mode := range []string{"", "AES256"} {
		t.Run("rejected with sse "+mode, func(t *testing.T) {
			err := (&Config{SSE: mode, BucketKeyEnabled: true}).Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "bucket_key_enabled requires sse")
		})
	}
}

func TestConfigValidate_DefaultConfigIsValid(t *testing.T) {
	require.NoError(t, NewConfig().Validate())
}

// A KMS key with no sse must never reach the wire on its own: the key header
// alone is meaningless to S3. Validate rejects that config, and the early
// return in applyEncryption keeps it structurally impossible regardless.
func TestApplyEncryption_NoSSEEmitsNothing(t *testing.T) {
	cfg := &Config{
		KMSKeyARN:        "arn:aws:kms:us-east-1:123456789012:key/test-key",
		BucketKeyEnabled: true,
	}
	ui := &s3manager.UploadInput{}

	cfg.applyEncryption(ui)

	assert.Nil(t, ui.ServerSideEncryption)
	assert.Nil(t, ui.SSEKMSKeyId)
	assert.Nil(t, ui.BucketKeyEnabled)
}
