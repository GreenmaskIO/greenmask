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

package filestore

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
)

const (
	defaultFilestoreSubdir   = "filestore"
	defaultArchiveName       = "filestore.tar.gz"
	defaultMetadataFileName  = "filestore.json"
	tarGzDoubleExtension     = ".tar.gz"
	defaultArchiveSplitWidth = 4
)

type dumpSettings struct {
	cfg            *domains.FilestoreDump
	usePgzip       bool
	splitEnabled   bool
	subdir         string
	archiveName    string
	metadataName   string
	maxSizeBytes   int64
	maxFiles       int
	failOnMissing  bool
	fileListPath   string
	rootPath       string
}

type fileEntry struct {
	AbsolutePath string
	RelativePath string
}

type archiveState struct {
	index          int
	name           string
	tw             *tar.Writer
	gzWriter       ioutils.CountWriteCloser
	reader         ioutils.CountReadCloser
	done           chan error
	files          int
	originalBytes  int64
}

// Dump packs the configured filestore subset (or whole directory) and uploads it to storage.
func Dump(ctx context.Context, cfg *domains.FilestoreDump, st storages.Storager, defaultPgzip bool) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	settings, err := buildDumpSettings(cfg, defaultPgzip)
	if err != nil {
		return err
	}

	log.Info().Msg("starting filestore dump")
	entries, missing, err := collectEntries(settings)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		log.Warn().Msg("filestore dump skipped because no files matched criteria")
		return writeMetadata(ctx, st, settings, missing, nil, 0, 0)
	}

	manager := newArchiveManager(st, settings)
	for _, entry := range entries {
		if err := manager.add(ctx, entry); err != nil {
			return err
		}
	}

	totalCompressed, err := manager.close(ctx)
	if err != nil {
		return err
	}

	if err := writeMetadata(ctx, st, settings, missing, manager.archives, manager.totalFiles, totalCompressed); err != nil {
		return err
	}
	log.Info().
		Int("archives", len(manager.archives)).
		Int("files", manager.totalFiles).
		Int64("original_bytes", manager.totalOriginalBytes).
		Int64("compressed_bytes", totalCompressed).
		Msg("filestore dump completed")
	return nil
}

func buildDumpSettings(cfg *domains.FilestoreDump, defaultPgzip bool) (*dumpSettings, error) {
	if cfg.RootPath == "" {
		return nil, errors.New("dump.filestore.root_path cannot be empty")
	}
	rootPath := filepath.Clean(cfg.RootPath)
	info, err := os.Stat(rootPath)
	if err != nil {
		return nil, fmt.Errorf("stat filestore root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("filestore root %s is not a directory", rootPath)
	}
	subdir := cfg.Subdir
	if subdir == "" {
		subdir = defaultFilestoreSubdir
	}
	archiveName := cfg.ArchiveName
	if archiveName == "" {
		archiveName = defaultArchiveName
	}
	metadataName := cfg.MetadataName
	if metadataName == "" {
		metadataName = defaultMetadataFileName
	}
	usePgzip := defaultPgzip
	if cfg.UsePgzip != nil {
		usePgzip = *cfg.UsePgzip
	}
	splitEnabled := cfg.Split.MaxFiles > 0 || cfg.Split.MaxSizeBytes > 0
	return &dumpSettings{
		cfg:           cfg,
		usePgzip:      usePgzip,
		splitEnabled:  splitEnabled,
		subdir:        subdir,
		archiveName:   archiveName,
		metadataName:  metadataName,
		maxSizeBytes:  cfg.Split.MaxSizeBytes,
		maxFiles:      cfg.Split.MaxFiles,
		failOnMissing: cfg.FailOnMissing,
		fileListPath:  cfg.FileList,
		rootPath:      rootPath,
	}, nil
}

func collectEntries(settings *dumpSettings) ([]fileEntry, []string, error) {
	if settings.fileListPath != "" {
		return collectFromList(settings)
	}
	return collectWholeDirectory(settings)
}

func collectFromList(settings *dumpSettings) ([]fileEntry, []string, error) {
	file, err := os.Open(settings.fileListPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open file list: %w", err)
	}
	defer file.Close()

	var entries []fileEntry
	var missing []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		relPath := filepath.Clean(line)
		if filepath.IsAbs(relPath) {
			return nil, nil, fmt.Errorf("file list entries must be relative: %s", relPath)
		}
		fullPath := filepath.Join(settings.rootPath, relPath)
		if !strings.HasPrefix(fullPath, settings.rootPath) {
			return nil, nil, fmt.Errorf("file list entry exits filestore root: %s", relPath)
		}
		info, err := os.Lstat(fullPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if settings.failOnMissing {
					return nil, nil, fmt.Errorf("listed file %s not found", relPath)
				}
				missing = append(missing, relPath)
				continue
			}
			return nil, nil, fmt.Errorf("stat %s: %w", relPath, err)
		}
		if info.IsDir() {
			log.Warn().Str("path", relPath).Msg("skipping directory entry in file list")
			continue
		}
		entries = append(entries, fileEntry{
			AbsolutePath: fullPath,
			RelativePath: filepath.ToSlash(relPath),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("read file list: %w", err)
	}
	return entries, missing, nil
}

func collectWholeDirectory(settings *dumpSettings) ([]fileEntry, []string, error) {
	var entries []fileEntry
	err := filepath.WalkDir(settings.rootPath, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			log.Warn().Str("path", path).Msg("skipping unsupported file type")
			return nil
		}
		rel, err := filepath.Rel(settings.rootPath, path)
		if err != nil {
			return err
		}
		entries = append(entries, fileEntry{
			AbsolutePath: path,
			RelativePath: filepath.ToSlash(rel),
		})
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("walk filestore root: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].RelativePath < entries[j].RelativePath
	})
	return entries, nil, nil
}

type archiveManager struct {
	settings           *dumpSettings
	st                 storages.Storager
	filestoreStorage   storages.Storager
	current            *archiveState
	archives           []archiveMeta
	totalFiles         int
	totalOriginalBytes int64
}

func newArchiveManager(st storages.Storager, settings *dumpSettings) *archiveManager {
	return &archiveManager{
		settings:         settings,
		st:               st,
		filestoreStorage: st.SubStorage(settings.subdir, true),
	}
}

func (m *archiveManager) add(ctx context.Context, entry fileEntry) error {
	info, err := os.Lstat(entry.AbsolutePath)
	if err != nil {
		return fmt.Errorf("stat %s: %w", entry.RelativePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return m.addSymlink(ctx, entry, info)
	}
	if !info.Mode().IsRegular() {
		log.Warn().Str("path", entry.RelativePath).Msg("skipping unsupported file type")
		return nil
	}
	if err := m.ensureArchive(ctx); err != nil {
		return err
	}
	if err := m.writeFile(entry, info); err != nil {
		return err
	}
	if m.shouldRotate() {
		if err := m.rotate(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *archiveManager) addSymlink(ctx context.Context, entry fileEntry, info os.FileInfo) error {
	if err := m.ensureArchive(ctx); err != nil {
		return err
	}
	linkTarget, err := os.Readlink(entry.AbsolutePath)
	if err != nil {
		return fmt.Errorf("readlink %s: %w", entry.RelativePath, err)
	}
	hdr, err := tar.FileInfoHeader(info, linkTarget)
	if err != nil {
		return fmt.Errorf("tar header for %s: %w", entry.RelativePath, err)
	}
	hdr.Name = entry.RelativePath
	if err := m.current.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write header for %s: %w", entry.RelativePath, err)
	}
	m.recordFile(info.Size())
	return nil
}

func (m *archiveManager) writeFile(entry fileEntry, info os.FileInfo) error {
	file, err := os.Open(entry.AbsolutePath)
	if err != nil {
		return fmt.Errorf("open %s: %w", entry.RelativePath, err)
	}
	defer file.Close()

	hdr, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("tar header for %s: %w", entry.RelativePath, err)
	}
	hdr.Name = entry.RelativePath
	if err := m.current.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write header for %s: %w", entry.RelativePath, err)
	}
	if _, err := io.Copy(m.current.tw, file); err != nil {
		return fmt.Errorf("copy %s: %w", entry.RelativePath, err)
	}
	m.recordFile(info.Size())
	return nil
}

func (m *archiveManager) recordFile(size int64) {
	m.current.files++
	m.current.originalBytes += size
	m.totalFiles++
	m.totalOriginalBytes += size
}

func (m *archiveManager) close(ctx context.Context) (int64, error) {
	if err := m.rotate(ctx); err != nil {
		return 0, err
	}
	var compressedTotal int64
	for _, archive := range m.archives {
		compressedTotal += archive.CompressedBytes
	}
	return compressedTotal, nil
}

func (m *archiveManager) ensureArchive(ctx context.Context) error {
	if m.current != nil {
		return nil
	}
	nextIndex := len(m.archives) + 1
	name := m.buildArchiveName(nextIndex)
	w, r := ioutils.NewGzipPipe(m.settings.usePgzip)
	state := &archiveState{
		index:    nextIndex,
		name:     name,
		tw:       tar.NewWriter(w),
		gzWriter: w,
		reader:   r,
		done:     make(chan error, 1),
	}
	go func() {
		defer func() {
			if err := r.Close(); err != nil {
				log.Warn().Err(err).Str("archive", name).Msg("error closing filestore reader")
			}
		}()
		err := m.filestoreStorage.PutObject(ctx, name, r)
		state.done <- err
	}()
	m.current = state
	return nil
}

func (m *archiveManager) rotate(ctx context.Context) error {
	if m.current == nil {
		return nil
	}
	if err := m.current.tw.Close(); err != nil {
		return fmt.Errorf("close tar archive %s: %w", m.current.name, err)
	}
	if err := m.current.gzWriter.Close(); err != nil {
		return fmt.Errorf("close gzip archive %s: %w", m.current.name, err)
	}
	if err := <-m.current.done; err != nil {
		return fmt.Errorf("upload archive %s: %w", m.current.name, err)
	}

	m.archives = append(m.archives, archiveMeta{
		Name:            m.current.name,
		Files:           m.current.files,
		OriginalBytes:   m.current.originalBytes,
		CompressedBytes: m.current.reader.GetCount(),
	})
	m.current = nil
	return nil
}

func (m *archiveManager) shouldRotate() bool {
	if !m.settings.splitEnabled || m.current == nil {
		return false
	}
	if m.settings.maxFiles > 0 && m.current.files >= m.settings.maxFiles {
		return true
	}
	if m.settings.maxSizeBytes > 0 && m.current.originalBytes >= m.settings.maxSizeBytes {
		return true
	}
	return false
}

func (m *archiveManager) buildArchiveName(idx int) string {
	if !m.settings.splitEnabled && idx == 1 {
		return m.settings.archiveName
	}
	name := m.settings.archiveName
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	suffix := ext
	if strings.HasSuffix(strings.ToLower(name), tarGzDoubleExtension) {
		base = strings.TrimSuffix(name, tarGzDoubleExtension)
		suffix = tarGzDoubleExtension
	}
	return fmt.Sprintf("%s-%0*d%s", base, defaultArchiveSplitWidth, idx, suffix)
}

func writeMetadata(ctx context.Context, st storages.Storager, settings *dumpSettings, missing []string, archives []archiveMeta, totalFiles int, totalCompressed int64) error {
	meta := metadata{
		GeneratedAt:          time.Now().UTC(),
		RootPath:             settings.rootPath,
		FileList:             settings.fileListPath,
		Subdir:               settings.subdir,
		ArchiveName:          settings.archiveName,
		UsePgzip:             settings.usePgzip,
		TotalFiles:           totalFiles,
		TotalOriginalBytes:   0,
		TotalCompressedBytes: totalCompressed,
		Archives:             archives,
		Missing:              missing,
	}
	var totalOriginal int64
	for _, archive := range archives {
		totalOriginal += archive.OriginalBytes
	}
	meta.TotalOriginalBytes = totalOriginal
	meta.Split.MaxFiles = settings.maxFiles
	meta.Split.MaxSizeBytes = settings.maxSizeBytes

	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal filestore metadata: %w", err)
	}

	filestoreStorage := st.SubStorage(settings.subdir, true)
	if err := filestoreStorage.PutObject(ctx, settings.metadataName, bytes.NewReader(metaBytes)); err != nil {
		return fmt.Errorf("store filestore metadata: %w", err)
	}
	return nil
}

