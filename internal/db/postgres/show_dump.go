package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path"

	storage2 "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/storage"
	"github.com/GreenmaskIO/greenmask/internal/storage"
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
;     Format: DIRECTORY
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

func ShowDump(ctx context.Context, st storage.Storager, dumpId string, format string) error {
	meta := &storage2.Metadata{}
	r, err := st.GetReader(ctx, path.Join(dumpId, "metadata.json"))
	if err != nil {
		return fmt.Errorf("cannot get metadata: %w", err)
	}
	if err := json.NewDecoder(r).Decode(meta); err != nil {
		return fmt.Errorf("matadata parsing error: %w", err)
	}

	switch format {
	case FormatText:
		err = printText(meta)
	case FormatYaml:
		err = printYaml(meta)
	case FormatJson:
		err = printJson(meta)
	default:
		return fmt.Errorf("unknown output format %s", format)
	}
	return nil
}

func printJson(meta *storage2.Metadata) error {
	if err := json.NewEncoder(os.Stdout).Encode(meta); err != nil {
		return fmt.Errorf("json render error: %w", err)
	}
	return nil
}

func printYaml(meta *storage2.Metadata) error {
	if err := yaml.NewEncoder(os.Stdout).Encode(meta); err != nil {
		return fmt.Errorf("yaml render error: %w", err)
	}
	return nil
}

func printText(meta *storage2.Metadata) error {
	t, err := template.New(templateName).Parse(templateString)
	if err != nil {
		return fmt.Errorf("cannot parser TOC report template: %w", err)
	}

	if err := t.Execute(os.Stdout, &meta); err != nil {
		return fmt.Errorf("template reder error: %s", err)
	}
	return nil
}
