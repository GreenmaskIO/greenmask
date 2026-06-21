package core

import "fmt"

// RestoreRunInput carries all execution-time parameters for RestoreProcessor.Run.
// Call Validate before use to catch missing required resources early.
type RestoreRunInput struct {
	Session     DatabaseSession
	Conn        ConnectionConfigurer
	St          Storager
	Meta        Metadata
	Instruction RestoreInstruction
}

func (i RestoreRunInput) Validate() error {
	if i.Session == nil {
		return fmt.Errorf("session is required")
	}
	if i.Conn == nil {
		return fmt.Errorf("conn is required")
	}
	if i.St == nil {
		return fmt.Errorf("storage is required")
	}
	return nil
}
