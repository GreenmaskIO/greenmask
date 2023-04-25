package faker

import (
	"errors"
	"fmt"
	"github.com/jaswdr/faker"
	"github.com/wwoytenko/greenfuscator/internal/domains"
	"golang.org/x/exp/slices"
	"strings"
)

type UserAgentTransformer struct {
	Column domains.ColumnMeta
	F      func() string
}

func (uat *UserAgentTransformer) Transform(originalValue string) (string, error) {
	return uat.F(), nil
}

func NewUserAgentTransformer(column domains.ColumnMeta, params map[string]string) (domains.Transformer, error) {
	agents := []string{"Any", "Chrome", "Firefox", "Opera", "InternetExplorer", "Safari"}
	agent, ok := params["agent"]
	if !ok {
		return nil, errors.New("expected \"agent\" argument")
	}
	if !slices.Contains(agents, agent) {
		return nil, fmt.Errorf("unexpected agent value %s expected one of %s", agent, strings.Join(agents, ", "))
	}

	var f func() string
	userAgent := faker.UserAgent{Faker: &faker.Faker{Generator: &RandomFakerGenerator{}}}
	switch agent {
	case "Any":
		f = userAgent.UserAgent
	case "Chrome":
		f = userAgent.Chrome
	case "Firefox":
		f = userAgent.Firefox
	case "InternetExplorer":
		f = userAgent.InternetExplorer
	case "Safari":
		f = userAgent.Safari
	}
	return &UuidTransformer{
		Column: column,
		F:      f,
	}, nil
}
