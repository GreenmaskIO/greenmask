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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path"
	"regexp"

	"gopkg.in/yaml.v3"

	storageDto "github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/storages"
)

const templateName = "metadataList"

const (
	FormatText = "text"
	FormatYaml = "yaml"
	FormatJson = "json"
)

var templateString = `;
; Archive created at {{ .Header.CreationDate.Format "2006-01-02 15:04:05 UTC" }}
;     dbname: {{ .Header.DbName }}
;     TOC Entries: {{ .Header.TocEntriesCount }}
;     Compression: {{ .Header.Compression }}
;     Dump Version: {{ .Header.DumpVersion }}
;     TableFormat: DIRECTORY
;     Integer: {{ .Header.Integer }} bytes
;     Offset: {{ .Header.Offset }} bytes
;     Dumped from database version: {{ .Header.DumpedFrom }}
;     Dumped by pg_dump version: {{ .Header.DumpedBy }}
;
;
; Selected TOC Entries:
;
{{- range .Entries }}
{{ .DumpId }}; {{ .ObjectOid }} {{ .DatabaseOid }} {{ .ObjectType }} {{ if ne .Schema "" }}{{ .Schema }}{{ else }}-{{ end }} {{ .Name }} {{ .Owner }}
{{- end }}
`

func ShowDump(ctx context.Context, st storages.Storager, dumpId string, format string) error {
	meta := &storageDto.Metadata{}
	r, err := st.GetObject(ctx, path.Join(dumpId, MetadataJsonFileName))
	if err != nil {
		return fmt.Errorf("cannot get metadata: %w", err)
	}
	if err := json.NewDecoder(r).Decode(meta); err != nil {
		return fmt.Errorf("matadata parsing error: %w", err)
	}

	re := regexp.MustCompile(`^"(.*)"$`)

	// deleting escaping quotes
	for _, e := range meta.Entries {
		e.Schema = re.ReplaceAllString(e.Schema, "$1")
		e.Name = re.ReplaceAllString(e.Name, "$1")
		e.Owner = re.ReplaceAllString(e.Owner, "$1")
	}

	switch format {
	case FormatText:
		if err = printText(meta); err != nil {
			return err
		}
	case FormatYaml:
		if err = printYaml(meta); err != nil {
			return err
		}
	case FormatJson:
		if err = printJson(meta); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown output format %s", format)
	}
	return nil
}

func printJson(meta *storageDto.Metadata) error {
	if err := json.NewEncoder(os.Stdout).Encode(meta); err != nil {
		return fmt.Errorf("json render error: %w", err)
	}
	return nil
}

func printYaml(meta *storageDto.Metadata) error {
	if err := yaml.NewEncoder(os.Stdout).Encode(meta); err != nil {
		return fmt.Errorf("yaml render error: %w", err)
	}
	return nil
}

func printText(meta *storageDto.Metadata) error {
	t, err := template.New(templateName).Parse(templateString)
	if err != nil {
		return fmt.Errorf("cannot parser TOC report template: %w", err)
	}

	if err := t.Execute(os.Stdout, &meta); err != nil {
		return fmt.Errorf("template reder error: %s", err)
	}
	return nil
}
