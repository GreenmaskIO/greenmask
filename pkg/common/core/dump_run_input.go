package core

import "fmt"

// DumpRunInput carries all execution-time parameters for DumpProcessor.Run.
// Call Validate before use to catch missing required resources early.
type DumpRunInput struct {
	Session     DatabaseSession
	Conn        ConnectionConfigurer
	St          Storager
	DumpID      DumpID
	Plan        DumpPlan
	Instruction DumpInstruction
}

func (i DumpRunInput) Validate() error {
	if i.Session == nil {
		return fmt.Errorf("session is required")
	}
	if i.Conn == nil {
		return fmt.Errorf("conn is required")
	}
	if i.St == nil {
		return fmt.Errorf("storage is required")
	}
	if err := i.DumpID.Validate(); err != nil {
		return fmt.Errorf("dump id: %w", err)
	}
	return nil
}
