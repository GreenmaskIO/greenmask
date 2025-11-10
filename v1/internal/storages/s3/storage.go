// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

const s3StorageDefaultDelimiter = "/"

const (
	s3StorageAwsErrorCodeNotFount  = "NotFound"
	s3StorageAwsErrorCodeNoSuchKey = "NoSuchKey"
)

type Storage struct {
	config    *S3Config
	session   *session.Session
	service   s3iface.S3API
	uploader  s3manageriface.UploaderAPI
	prefix    string
	delimiter string
}

func New(ctx context.Context, cfg S3Config, logLevel string) (*Storage, error) {

	ses, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("cannot establish session: %w", err)
	}

	awsCfg := aws.NewConfig()
	awsCfg.WithS3ForcePathStyle(cfg.ForcePathStyle)
	awsCfg.WithS3UseAccelerate(cfg.UseAccelerate)
	request.WithRetryer(awsCfg, client.DefaultRetryer{NumMaxRetries: cfg.MaxRetries})

	accessKeyID := cfg.AccessKeyId
	secretAccessKey := cfg.SecretAccessKey
	sessionToken := cfg.SessionToken

	if cfg.RoleArn != "" {
		ss := sts.New(ses)
		role, err := ss.AssumeRoleWithContext(
			ctx,
			&sts.AssumeRoleInput{
				RoleArn:         aws.String(cfg.RoleArn),
				RoleSessionName: aws.String(cfg.SessionName),
			},
		)
		if err != nil {
			return nil, fmt.Errorf("unable to perform role assuming: %w", err)
		}
		accessKeyID = *role.Credentials.AccessKeyId
		secretAccessKey = *role.Credentials.SecretAccessKey
		sessionToken = *role.Credentials.SessionToken
	}

	if cfg.SecretAccessKey != "" && cfg.AccessKeyId != "" {
		sp := &credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
				SessionToken:    sessionToken,
			},
		}
		cps := defaults.CredProviders(awsCfg, defaults.Handlers())

		var providers []credentials.Provider
		providers = append(providers, sp)
		providers = append(providers, cps...)

		creds := credentials.NewCredentials(&credentials.ChainProvider{
			VerboseErrors: aws.BoolValue(awsCfg.CredentialsChainVerboseErrors),
			Providers:     providers,
		})
		awsCfg.WithCredentials(creds)
	}

	var lv aws.LogLevelType
	switch logLevel {
	case zerolog.LevelDebugValue:
		lv = aws.LogDebug | aws.LogDebugWithRequestErrors | aws.LogDebugWithRequestRetries
	default:
		lv = aws.LogOff
	}
	awsCfg.WithLogger(LogWrapper{logger: &log.Logger})
	awsCfg.WithLogLevel(lv)

	if cfg.NoVerifySsl {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		awsCfg.WithHTTPClient(&http.Client{Transport: tr})
	}

	if cfg.Endpoint != "" {
		awsCfg.WithEndpoint(cfg.Endpoint)
	}

	if cfg.Region != "" {
		awsCfg.WithRegion(cfg.Region)
	}

	if cfg.CertFile != "" {
		file, err := os.Open(cfg.CertFile)
		if err != nil {
			return nil, fmt.Errorf("")
		}
		defer file.Close()
		ses, err = session.NewSessionWithOptions(session.Options{Config: *ses.Config, CustomCABundle: file})
		if err != nil {
			return nil, fmt.Errorf("cannot establish session using provided certFile: %w", err)
		}
	}

	service := s3.New(ses, awsCfg)
	uploader := s3manager.NewUploaderWithClient(
		service, func(uploader *s3manager.Uploader) {
			uploader.PartSize = cfg.MaxPartSize
			uploader.Concurrency = cfg.Concurrency
		},
	)

	log.Debug().
		Str("region", *service.Config.Region).
		Str("bucket", cfg.Bucket).
		Msg("s3 storage bucket")

	return &Storage{
		prefix:   fixPrefix(cfg.Prefix),
		session:  ses,
		config:   &cfg,
		service:  service,
		uploader: uploader,
	}, nil
}

func (s *Storage) GetCwd() string {
	return s.prefix
}

func (s *Storage) Dirname() string {
	return filepath.Base(s.prefix)
}

func (s *Storage) ListDir(ctx context.Context) (files []string, dirs []interfaces.Storager, err error) {

	listFunc := func(commonPrefixes []*s3.CommonPrefix, contents []*s3.Object) {
		for _, prefix := range commonPrefixes {

			dirs = append(
				dirs, &Storage{
					config:   s.config,
					session:  s.session,
					service:  s.service,
					uploader: s.uploader,
					prefix:   fixPrefix(*prefix.Prefix),
				},
			)
		}
		for _, object := range contents {
			files = append(files, strings.TrimPrefix(*object.Key, s.prefix))
		}
	}

	prefix := aws.String(s.prefix)
	delimiter := aws.String(s3StorageDefaultDelimiter)
	if s.config.UseListObjectsV1 {
		page := &s3.ListObjectsInput{
			Prefix:    prefix,
			Bucket:    aws.String(s.config.Bucket),
			Delimiter: delimiter,
		}
		err = s.service.ListObjectsPagesWithContext(
			ctx, page, func(page *s3.ListObjectsOutput, lastPage bool) bool {
				listFunc(page.CommonPrefixes, page.Contents)
				return true
			},
		)
		if err != nil {
			return nil, nil, fmt.Errorf("error listing s3 objects v1: %w", err)
		}
	} else {
		page := &s3.ListObjectsV2Input{
			Prefix:    prefix,
			Bucket:    aws.String(s.config.Bucket),
			Delimiter: delimiter,
		}
		err = s.service.ListObjectsV2PagesWithContext(
			ctx, page, func(
				page *s3.ListObjectsV2Output, lastPage bool,
			) bool {
				listFunc(page.CommonPrefixes, page.Contents)
				return true
			},
		)
		if err != nil {
			return nil, nil, fmt.Errorf("error listing s3 objects v2: %w", err)
		}
	}

	return
}

func (s *Storage) GetObject(ctx context.Context, filePath string) (writer io.ReadCloser, err error) {
	obj, err := s.service.GetObjectWithContext(
		ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.config.Bucket),
			Key:    aws.String(path.Join(s.prefix, filePath)),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error getting object: %w", err)
	}
	return obj.Body, nil
}

func (s *Storage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	ui := &s3manager.UploadInput{
		Bucket:       aws.String(s.config.Bucket),
		Key:          aws.String(path.Join(s.prefix, filePath)),
		Body:         body,
		StorageClass: aws.String(s.config.StorageClass),
	}

	// TODO: Implement server side encryption
	if _, err := s.uploader.UploadWithContext(ctx, ui); err != nil {
		return fmt.Errorf("s3 object uploading error: %w", err)
	}
	return nil
}

func (s *Storage) Delete(ctx context.Context, filePaths ...string) error {
	objs := make([]*s3.ObjectIdentifier, len(filePaths))
	for idx, fp := range filePaths {
		objs[idx] = &s3.ObjectIdentifier{
			Key: aws.String(path.Join(s.prefix, fp)),
		}
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(s.config.Bucket),
		Delete: &s3.Delete{
			Objects: objs,
		},
	}
	_, err := s.service.DeleteObjectsWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("error deleting objects: %w", err)
	}
	return nil
}

func (s *Storage) DeleteAll(ctx context.Context, pathPrefix string) error {
	pathPrefix = fixPrefix(pathPrefix)
	ss := s.SubStorage(pathPrefix, true)
	filesList, err := walk(ctx, ss, "")
	if err != nil {
		return fmt.Errorf("error walking through storage: %w", err)
	}

	if err = ss.Delete(ctx, filesList...); err != nil {
		return fmt.Errorf("error deleting files: %w", err)
	}
	return nil
}

func (s *Storage) SubStorage(subPath string, relative bool) interfaces.Storager {
	prefix := subPath
	if relative {
		prefix = fixPrefix(path.Join(s.prefix, prefix))
	}
	return &Storage{
		config:    s.config,
		session:   s.session,
		service:   s.service,
		uploader:  s.uploader,
		prefix:    prefix,
		delimiter: s.delimiter,
	}
}

func (s *Storage) Exists(ctx context.Context, fileName string) (bool, error) {
	hoi := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(path.Join(s.prefix, fileName)),
	}

	_, err := s.service.HeadObjectWithContext(ctx, hoi)
	if err != nil {
		var awsErr awserr.Error
		if errors.As(err, &awsErr) && (awsErr.Code() == s3StorageAwsErrorCodeNotFount || awsErr.Code() == s3StorageAwsErrorCodeNoSuchKey) {
			return false, nil
		}
		return false, fmt.Errorf("error getting object info: %w", err)
	}
	return true, nil
}

func (s *Storage) Stat(fileName string) (*models.StorageObjectStat, error) {
	fullPath := path.Join(s.prefix, fileName)
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fullPath),
	}

	headObjectOutput, err := s.service.HeadObject(headObjectInput)
	if err != nil {
		return nil, fmt.Errorf("error getting object info: %w", err)
	}

	return &models.StorageObjectStat{
		Name:         fullPath,
		LastModified: *(headObjectOutput.LastModified),
		Exist:        true,
	}, nil
}

func fixPrefix(prefix string) string {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix = prefix + "/"
	}
	return prefix
}
