package context

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func Test_isTransformerAllowedToApplyForReferences(t *testing.T) {
	r := utils.DefaultTransformerRegistry

	t.Run("RandomInt and hash engine", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               transformers.RandomIntTransformerName,
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
				"engine": toolkit.ParamsValue("hash"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.Empty(t, w)
		require.True(t, ok)
	})

	t.Run("RandomInt and without hash engine", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               transformers.RandomIntTransformerName,
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
				"engine": toolkit.ParamsValue("random"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.NotEmpty(t, w)
		require.False(t, ok)
	})

	t.Run("Template", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               transformers.TemplateTransformerName,
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.NotEmpty(t, w)
		require.False(t, ok)
	})

	t.Run("Unknown name", func(t *testing.T) {
		cfg := &domains.TransformerConfig{
			Name:               "unknown",
			ApplyForReferences: true,
			Params: toolkit.StaticParameters{
				"column": toolkit.ParamsValue("id"),
			},
		}
		ok, w := isTransformerAllowedToApplyForReferences(cfg, r)
		require.NotEmpty(t, w)
		require.False(t, ok)
	})
}
