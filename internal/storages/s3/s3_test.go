package s3

import (
	"bytes"
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func getCfg() *Config {
	cfg := NewConfig()
	cfg.Endpoint = "http://localhost:9000"
	cfg.Bucket = "testbucket"
	cfg.Region = "us-east-1"
	cfg.AccessKeyId = "Q3AM3UQ867SPQQA43P2F"
	cfg.SecretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	return cfg
}

func TestNewStorage(t *testing.T) {
	cfg := getCfg()
	st, err := NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	require.NoError(t, err)
	dirs, files, err := st.ListDir(context.Background())
	require.NoError(t, err)
	log.Debug().Any("dirs", dirs).Any("files", files).Msg("")
}

func TestStorage_GetWriter(t *testing.T) {
	cfg := getCfg()
	st, err := NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	require.NoError(t, err)
	buf := bytes.NewBuffer([]byte("asdadsoiajdkaoisdmaokaos"))
	err = st.PutObject(context.Background(), "/test.txt", buf)
	require.NoError(t, err)
}
