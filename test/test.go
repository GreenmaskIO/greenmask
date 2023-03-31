package test

import (
	"github.com/wwoytenko/greenfuscator/internal/domains"
	"github.com/wwoytenko/greenfuscator/internal/masker/simple"
)

var metricsTable = domains.Table{
	Schema: "public",
	Name:   "metrics",
	Columns: map[string]domains.Column{
		"id": {
			Name:   "id",
			Type:   "TEXT",
			Masker: &simple.DummyMasker{},
		},
		"type": {
			Name:   "type",
			Type:   "TEXT",
			Masker: &simple.DummyMasker{},
		},
		"value": {
			Name:   "value",
			Type:   "double precision",
			Masker: &simple.DummyMasker{},
		},
		"delta": {
			Name:   "delta",
			Type:   "double precision",
			Masker: &simple.DummyMasker{},
		},
		"created_at": {
			Name:   "created_at",
			Type:   "timestamp without time zone",
			Masker: &simple.DummyMasker{},
		},
	},
}
