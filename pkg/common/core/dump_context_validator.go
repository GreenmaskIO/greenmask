package core

type DumpContextValidatorInput struct {
	DumpContext DumpContext     `json:"context"`
	Diff        DumpContextDiff `json:"diff"`
}
