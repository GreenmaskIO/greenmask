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

import "time"

type archiveMeta struct {
	Name            string `json:"name"`
	Files           int    `json:"files"`
	OriginalBytes   int64  `json:"original_bytes"`
	CompressedBytes int64  `json:"compressed_bytes"`
}

type metadata struct {
	GeneratedAt          time.Time     `json:"generated_at"`
	RootPath             string        `json:"root_path,omitempty"`
	FileList             string        `json:"file_list,omitempty"`
	IncludeListQuery     string        `json:"include_list_query,omitempty"`
	IncludeListQueryFile string        `json:"include_list_query_file,omitempty"`
	IncludeListSource    string        `json:"include_list_source,omitempty"`
	Subdir               string        `json:"subdir"`
	ArchiveName          string        `json:"archive_name"`
	UsePgzip             bool          `json:"use_pgzip"`
	TotalFiles           int           `json:"total_files"`
	TotalOriginalBytes   int64         `json:"total_original_bytes"`
	TotalCompressedBytes int64         `json:"total_compressed_bytes"`
	Archives             []archiveMeta `json:"archives"`
	Split                struct {
		MaxSizeBytes int64 `json:"max_size_bytes,omitempty"`
		MaxFiles     int   `json:"max_files,omitempty"`
	} `json:"split"`
	Missing []string `json:"missing,omitempty"`
}

