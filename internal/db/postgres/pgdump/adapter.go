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

package pgdump

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

// TODO: Simplify that adapter

const (
	NoneActionStage = iota
	StartedActionStage
	EndedActionStage
)

var (
	StateLowerCase     StateName = "StateLowerCase"
	StateKeepCase      StateName = "StateKeepCase"
	StateWildcard      StateName = "StateWildcard"
	StateDoubledQuotes StateName = "StateDoubledQuotes"
	StateQuestionMark  StateName = "StateQuestionMark"
	DefaultState                 = &State{
		Name:   StateLowerCase,
		Action: LowerCaseState,
	}
)

type ActionContext struct {
	strings.Builder
	Stage int
}

func (ac *ActionContext) IsDone() bool {
	return ac.Stage == EndedActionStage
}

func (ac *ActionContext) SetDone() {
	ac.Stage = EndedActionStage
}

type StateName string

type Action func(actx *ActionContext, s string, dest *strings.Builder) error

type State struct {
	ActionContext
	Name   StateName
	Action Action
}

func LowerCaseState(actx *ActionContext, s string, dest *strings.Builder) error {
	if s == "" {
		return errors.New("unexpected char length")
	}
	actx.SetDone() // It is always done because it's default state
	if _, err := dest.WriteString(strings.ToLower(s)); err != nil {
		return err
	}
	return nil
}

func KeepCaseState(actx *ActionContext, s string, dest *strings.Builder) error {
	if s == `"` {
		if actx.Stage == NoneActionStage {
			actx.Stage = StartedActionStage
		} else {
			actx.Stage = EndedActionStage
		}
		return nil
	} else if actx.Stage == NoneActionStage {
		return errors.New("syntax error")
	}
	if _, err := dest.WriteString(s); err != nil {
		return err
	}
	return nil

}

func WildCardState(actx *ActionContext, s string, dest *strings.Builder) error {
	if string(s) != "*" {
		return errors.New("unknown character")
	}
	if _, err := dest.WriteString(".*"); err != nil {
		return err
	}
	actx.SetDone()
	return nil
}

func QuestionMarkState(actx *ActionContext, s string, dest *strings.Builder) error {
	if s != "?" {
		return errors.New("unknown character")
	}
	if _, err := dest.WriteRune('.'); err != nil {
		return err
	}
	actx.SetDone()
	return nil
}

func DoubleQuoteState(actx *ActionContext, s string, dest *strings.Builder) error {
	if s != `""` {
		return errors.New("unknown character")
	}
	if _, err := dest.WriteRune('"'); err != nil {
		return err
	}
	actx.SetDone()
	return nil
}

type ParserContext struct {
	currentState *State
	// stateStack - nested states that we would be able to handle
	stateStack []*State
}

func NewParser() *ParserContext {
	return &ParserContext{
		currentState: DefaultState,
		stateStack:   []*State{DefaultState},
	}
}

func (p *ParserContext) PushState(state *State) {
	p.currentState = state
	p.stateStack = append(p.stateStack, state)
}

func (p *ParserContext) PopState() {
	p.currentState = p.stateStack[len(p.stateStack)-2]
	p.stateStack = p.stateStack[:len(p.stateStack)-1]
}

func (p *ParserContext) Depth() int {
	return len(p.stateStack)
}

func AdaptRegexp(data string) (string, error) {
	pctx := NewParser()
	src := strings.NewReader(data)
	dest := &strings.Builder{}
	var isEOF bool

	buf := make([]byte, 0, 128)
	literals := bytes.NewBuffer(buf)

	for {
		ch, _, err := src.ReadRune()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if pctx.Depth() > 1 {
					return "", errors.New("unexpected end of string")
				}
				return fmt.Sprintf("^(%s)$", dest.String()), nil
			}
			return "", err
		}
		if _, err = literals.WriteRune(ch); err != nil {
			return "", err
		}
		switch ch {
		// TODO: Parsing (R+|) = R*, or (R|) = R?
		case '"':
			// Parsing "KeepCase" or "" = "
			ch, _, err = src.ReadRune()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return "", err
				}
				isEOF = true
			}
			// Handling "" = "
			switch ch {
			case '"':
				// Doubled double quote parse
				pctx.PushState(&State{
					Name:   StateDoubledQuotes,
					Action: DoubleQuoteState,
				})
				if _, err = literals.WriteRune(ch); err != nil {
					return "", err
				}
			default:
				// Keep case parsing "KeepCase"
				if !isEOF {
					if err = src.UnreadRune(); err != nil {
						return "", err
					}
				}

				if pctx.currentState.Name != StateKeepCase {
					pctx.PushState(&State{
						Name:   StateKeepCase,
						Action: KeepCaseState,
					})
				}
			}

		case '*':
			if pctx.currentState.Name != StateWildcard {
				pctx.PushState(&State{
					Name:   StateWildcard,
					Action: WildCardState,
				})
			}
		case '?':
			if pctx.currentState.Name != StateQuestionMark {
				pctx.PushState(&State{
					Name:   StateQuestionMark,
					Action: QuestionMarkState,
				})
			}
		}

		if err = pctx.currentState.Action(&pctx.currentState.ActionContext, literals.String(), dest); err != nil {
			return "", err
		}

		if pctx.Depth() > 1 && pctx.currentState.IsDone() {
			pctx.PopState()
		}
		literals.Reset()
	}
}
