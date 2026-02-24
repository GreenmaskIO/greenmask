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

package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// ParseTransformerParamsManually - manually parse dump.transformation[a].transformers[b].params
// The problem described https://github.com/GreenmaskIO/greenmask/issues/76
// We need to keep the original keys in the map without lowercasing
// To overcome this problem we need use default yaml and json parsers avoiding vaiper or mapstructure usage.
func ParseTransformerParamsManually(cfgFilePath string, cfg *domains.Config) error {
	ext := path.Ext(cfgFilePath)
	tmpCfg := &domains.DummyConfig{}
	f, err := os.Open(cfgFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing config file")
		}
	}()

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
		return fmt.Errorf("unsupported file extension \"%s\"", ext)
	}
	return setTransformerParams(tmpCfg, cfg)
}

// setTransformerParams - get the value from domains.TransformerConfig.MetadataParams, marshall this value and store into
// domains.TransformerConfig.Params
func setTransformerParams(tmpCfg *domains.DummyConfig, cfg *domains.Config) (err error) {
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
