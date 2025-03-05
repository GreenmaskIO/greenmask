package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"gopkg.in/yaml.v3"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// dummyConfig - This is a dummy config to the viper workaround
// It is used to parse the transformation parameters manually only avoiding parsing other pars of the config
// The reason why is here https://github.com/GreenmaskIO/greenmask/discussions/85 .
type dummyConfig struct {
	Dump struct {
		Transformation []struct {
			Transformers []struct {
				Params map[string]interface{} `yaml:"params" json:"params"`
			} `yaml:"transformers" json:"transformers"`
		} `yaml:"transformation" json:"transformation"`
	} `yaml:"dump" json:"dump"`
}

// setTransformerParams - get the value from domains.TransformerConfig.MetadataParams, marshall
// this value and store into domains.TransformerConfig.Params.
func setTransformerParams(tmpCfg *dummyConfig, cfg *domains.Config) (err error) {
	for tableIdx, tableObj := range tmpCfg.Dump.Transformation {
		for transformationIdx, transformationObj := range tableObj.Transformers {
			transformer := cfg.Dump.Transformation[tableIdx].Transformers[transformationIdx]
			tmpTransformer := tmpCfg.Dump.Transformation[tableIdx].Transformers[transformationIdx]
			paramsMap := make(map[string]toolkit.ParamsValue, len(transformationObj.Params))
			for paramName, decodedValue := range tmpTransformer.Params {
				var encodedVal toolkit.ParamsValue
				switch v := decodedValue.(type) {
				case string:
					encodedVal = toolkit.ParamsValue(v)
				default:
					encodedVal, err = json.Marshal(v)
					if err != nil {
						return fmt.Errorf("cannot convert object to json bytes: %w", err)
					}
				}
				paramsMap[paramName] = encodedVal
			}
			transformer.Params = paramsMap
			transformer.MetadataParams = tmpTransformer.Params
		}
	}
	return nil
}

// ParseTransformerParamsManually - manually parse dump.transformation[a].transformers[b].params
// The problem described https://github.com/GreenmaskIO/greenmask/issues/76
// We need to keep the original keys in the map without lowercasing
// To overcome this problem we need use default yaml and json parsers avoiding vaiper or mapstructure usage.
func ParseTransformerParamsManually(cfgFilePath string, cfg *domains.Config) error {
	ext := path.Ext(cfgFilePath)
	tmpCfg := &dummyConfig{}
	f, err := os.Open(cfgFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	switch ext {
	case ".json":
		if err = json.NewDecoder(f).Decode(&tmpCfg); err != nil {
			return err
		}
	case ".yaml", ".yml":
		if err = yaml.NewDecoder(f).Decode(&tmpCfg); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported file extension \"%s\"", err)
	}
	return setTransformerParams(tmpCfg, cfg)
}
