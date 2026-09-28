package s3

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockS3Client implements s3iface.S3API for testing Delete batching.
type mockS3Client struct {
	s3iface.S3API
	calls []deleteCall
	err   error // if set, return this error on every call
	// failOnCall makes the Nth call (0-indexed) return an error
	failOnCall int
}

type deleteCall struct {
	keys []string
}

func (m *mockS3Client) DeleteObjectsWithContext(
	_ aws.Context, input *s3.DeleteObjectsInput, _ ...request.Option,
) (*s3.DeleteObjectsOutput, error) {
	idx := len(m.calls)
	var keys []string
	for _, obj := range input.Delete.Objects {
		keys = append(keys, *obj.Key)
	}
	m.calls = append(m.calls, deleteCall{keys: keys})

	if m.err != nil && idx == m.failOnCall {
		return nil, m.err
	}
	return &s3.DeleteObjectsOutput{}, nil
}

// mockUploader implements s3manageriface.UploaderAPI for testing PutObject.
type mockUploader struct {
	s3manageriface.UploaderAPI
	input *s3manager.UploadInput
	err   error
}

func (m *mockUploader) UploadWithContext(
	_ aws.Context, input *s3manager.UploadInput, _ ...func(*s3manager.Uploader),
) (*s3manager.UploadOutput, error) {
	m.input = input
	if m.err != nil {
		return nil, m.err
	}
	return &s3manager.UploadOutput{}, nil
}

func makeFilePaths(n int) []string {
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("file_%d.dat", i)
	}
	return paths
}

func newTestStorage(mock *mockS3Client) *Storage {
	return &Storage{
		config:  &Config{Bucket: "test-bucket"},
		service: mock,
		prefix:  "dumps/",
	}
}

func TestDelete_EmptyList(t *testing.T) {
	mock := &mockS3Client{}
	st := newTestStorage(mock)

	err := st.Delete(context.Background())
	require.NoError(t, err)
	assert.Empty(t, mock.calls)
}

func TestDelete_SingleBatch(t *testing.T) {
	mock := &mockS3Client{}
	st := newTestStorage(mock)

	err := st.Delete(context.Background(), makeFilePaths(500)...)
	require.NoError(t, err)
	assert.Len(t, mock.calls, 1)
	assert.Len(t, mock.calls[0].keys, 500)
}

func TestDelete_ExactlyOneBatch(t *testing.T) {
	mock := &mockS3Client{}
	st := newTestStorage(mock)

	err := st.Delete(context.Background(), makeFilePaths(1000)...)
	require.NoError(t, err)
	assert.Len(t, mock.calls, 1)
	assert.Len(t, mock.calls[0].keys, 1000)
}

func TestDelete_MultipleBatches(t *testing.T) {
	mock := &mockS3Client{}
	st := newTestStorage(mock)

	err := st.Delete(context.Background(), makeFilePaths(2500)...)
	require.NoError(t, err)
	assert.Len(t, mock.calls, 3)
	assert.Len(t, mock.calls[0].keys, 1000)
	assert.Len(t, mock.calls[1].keys, 1000)
	assert.Len(t, mock.calls[2].keys, 500)
}

func TestDelete_ErrorStopsBatching(t *testing.T) {
	mock := &mockS3Client{
		err:        fmt.Errorf("s3 failure"),
		failOnCall: 1,
	}
	st := newTestStorage(mock)

	err := st.Delete(context.Background(), makeFilePaths(2500)...)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error deleting objects")
	assert.Contains(t, err.Error(), "s3 failure")
	// Should have stopped after the second batch failed
	assert.Len(t, mock.calls, 2)
}

func TestDelete_PrefixApplied(t *testing.T) {
	mock := &mockS3Client{}
	st := newTestStorage(mock)

	err := st.Delete(context.Background(), "a.txt", "b.txt")
	require.NoError(t, err)
	require.Len(t, mock.calls, 1)
	assert.Equal(t, []string{"dumps/a.txt", "dumps/b.txt"}, mock.calls[0].keys)
}

func TestPutObject_NoSSEByDefault(t *testing.T) {
	mock := &mockUploader{}
	st := &Storage{
		config:   &Config{Bucket: "test-bucket"},
		uploader: mock,
		prefix:   "dumps/",
	}

	err := st.PutObject(context.Background(), "file.dat", strings.NewReader("data"))
	require.NoError(t, err)
	require.NotNil(t, mock.input)
	assert.Nil(t, mock.input.ServerSideEncryption)
}

func TestPutObject_SSEApplied(t *testing.T) {
	mock := &mockUploader{}
	st := &Storage{
		config:   &Config{Bucket: "test-bucket", SSE: "AES256"},
		uploader: mock,
		prefix:   "dumps/",
	}

	err := st.PutObject(context.Background(), "file.dat", strings.NewReader("data"))
	require.NoError(t, err)
	require.NotNil(t, mock.input)
	require.NotNil(t, mock.input.ServerSideEncryption)
	assert.Equal(t, "AES256", *mock.input.ServerSideEncryption)
}

func TestPutObject_KMSKeyAppliedForAwsKms(t *testing.T) {
	mock := &mockUploader{}
	st := &Storage{
		config:   &Config{Bucket: "test-bucket", SSE: "aws:kms", KMSKeyARN: "arn:aws:kms:us-east-1:123456789012:key/test-key"},
		uploader: mock,
		prefix:   "dumps/",
	}

	err := st.PutObject(context.Background(), "file.dat", strings.NewReader("data"))
	require.NoError(t, err)
	require.NotNil(t, mock.input)
	require.NotNil(t, mock.input.ServerSideEncryption)
	assert.Equal(t, "aws:kms", *mock.input.ServerSideEncryption)
	require.NotNil(t, mock.input.SSEKMSKeyId)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/test-key", *mock.input.SSEKMSKeyId)
}

func TestPutObject_KMSKeyAppliedForAwsKmsDsse(t *testing.T) {
	mock := &mockUploader{}
	st := &Storage{
		config:   &Config{Bucket: "test-bucket", SSE: "aws:kms:dsse", KMSKeyARN: "arn:aws:kms:us-east-1:123456789012:key/test-key"},
		uploader: mock,
		prefix:   "dumps/",
	}

	err := st.PutObject(context.Background(), "file.dat", strings.NewReader("data"))
	require.NoError(t, err)
	require.NotNil(t, mock.input)
	require.NotNil(t, mock.input.ServerSideEncryption)
	assert.Equal(t, "aws:kms:dsse", *mock.input.ServerSideEncryption)
	require.NotNil(t, mock.input.SSEKMSKeyId)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/test-key", *mock.input.SSEKMSKeyId)
}

func TestPutObject_KMSKeyOmittedWhenArnEmpty(t *testing.T) {
	mock := &mockUploader{}
	st := &Storage{
		config:   &Config{Bucket: "test-bucket", SSE: "aws:kms"},
		uploader: mock,
		prefix:   "dumps/",
	}

	err := st.PutObject(context.Background(), "file.dat", strings.NewReader("data"))
	require.NoError(t, err)
	require.NotNil(t, mock.input)
	require.NotNil(t, mock.input.ServerSideEncryption)
	assert.Equal(t, "aws:kms", *mock.input.ServerSideEncryption)
	assert.Nil(t, mock.input.SSEKMSKeyId)
}

func TestPutObject_KMSKeyIgnoredForNonKmsSSE(t *testing.T) {
	mock := &mockUploader{}
	st := &Storage{
		config:   &Config{Bucket: "test-bucket", SSE: "AES256", KMSKeyARN: "arn:aws:kms:us-east-1:123456789012:key/test-key"},
		uploader: mock,
		prefix:   "dumps/",
	}

	err := st.PutObject(context.Background(), "file.dat", strings.NewReader("data"))
	require.NoError(t, err)
	require.NotNil(t, mock.input)
	require.NotNil(t, mock.input.ServerSideEncryption)
	assert.Equal(t, "AES256", *mock.input.ServerSideEncryption)
	assert.Nil(t, mock.input.SSEKMSKeyId)
}

func TestPutObject_UploadError(t *testing.T) {
	mock := &mockUploader{err: fmt.Errorf("upload failure")}
	st := &Storage{
		config:   &Config{Bucket: "test-bucket"},
		uploader: mock,
		prefix:   "dumps/",
	}

	err := st.PutObject(context.Background(), "file.dat", strings.NewReader("data"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3 object uploading error")
	assert.Contains(t, err.Error(), "upload failure")
}
