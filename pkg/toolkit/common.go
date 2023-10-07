package toolkit

import "gopkg.in/yaml.v3"

type Oid int
type AttNum uint32

type ParamsValue []byte

func (pv ParamsValue) MarshalYAML() (interface{}, error) {
	var res = map[string]interface{}{}
	err := yaml.Unmarshal(pv, res)
	if err != nil {
		// fallback unmarshalling to string
		return string(pv), nil
	}

	return res, nil
}
