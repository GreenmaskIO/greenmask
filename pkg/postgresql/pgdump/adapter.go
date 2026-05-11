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
	noneActionStage = iota
	startedActionStage
	endedActionStage
)

var (
	stateLowerCase     stateName = "stateLowerCase"
	stateKeepCase      stateName = "stateKeepCase"
	stateWildcard      stateName = "stateWildcard"
	stateDoubledQuotes stateName = "stateDoubledQuotes"
	stateQuestionMark  stateName = "stateQuestionMark"
	defaultState                 = &state{
		name:   stateLowerCase,
		action: lowerCaseState,
	}
)

type actionContext struct {
	strings.Builder
	stage int
}

func (ac *actionContext) isDone() bool {
	return ac.stage == endedActionStage
}

func (ac *actionContext) setDone() {
	ac.stage = endedActionStage
}

type stateName string

type action func(actx *actionContext, s string, dest *strings.Builder) error

type state struct {
	actionContext
	name   stateName
	action action
}

func lowerCaseState(actx *actionContext, s string, dest *strings.Builder) error {
	if s == "" {
		return errors.New("unexpected char length")
	}
	actx.setDone() // It is always done because it's default state
	if _, err := dest.WriteString(strings.ToLower(s)); err != nil {
		return err
	}
	return nil
}

func keepCaseState(actx *actionContext, s string, dest *strings.Builder) error {
	if s == `"` {
		if actx.stage == noneActionStage {
			actx.stage = startedActionStage
		} else {
			actx.stage = endedActionStage
		}
		return nil
	} else if actx.stage == noneActionStage {
		return errors.New("syntax error")
	}
	if _, err := dest.WriteString(s); err != nil {
		return err
	}
	return nil

}

func wildCardState(actx *actionContext, s string, dest *strings.Builder) error {
	if string(s) != "*" {
		return errors.New("unknown character")
	}
	if _, err := dest.WriteString(".*"); err != nil {
		return err
	}
	actx.setDone()
	return nil
}

func questionMarkState(actx *actionContext, s string, dest *strings.Builder) error {
	if s != "?" {
		return errors.New("unknown character")
	}
	if _, err := dest.WriteRune('.'); err != nil {
		return err
	}
	actx.setDone()
	return nil
}

func doubleQuoteState(actx *actionContext, s string, dest *strings.Builder) error {
	if s != `""` {
		return errors.New("unknown character")
	}
	if _, err := dest.WriteRune('"'); err != nil {
		return err
	}
	actx.setDone()
	return nil
}

type ParserContext struct {
	currentState *state
	// stateStack - nested states that we would be able to handle
	stateStack []*state
}

func newParser() *ParserContext {
	return &ParserContext{
		currentState: defaultState,
		stateStack:   []*state{defaultState},
	}
}

func (p *ParserContext) pushState(state *state) {
	p.currentState = state
	p.stateStack = append(p.stateStack, state)
}

func (p *ParserContext) popState() {
	p.currentState = p.stateStack[len(p.stateStack)-2]
	p.stateStack = p.stateStack[:len(p.stateStack)-1]
}

func (p *ParserContext) Depth() int {
	return len(p.stateStack)
}

func AdaptRegexp(data string) (string, error) {
	pctx := newParser()
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
				pctx.pushState(&state{
					name:   stateDoubledQuotes,
					action: doubleQuoteState,
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

				if pctx.currentState.name != stateKeepCase {
					pctx.pushState(&state{
						name:   stateKeepCase,
						action: keepCaseState,
					})
				}
			}

		case '*':
			if pctx.currentState.name != stateWildcard {
				pctx.pushState(&state{
					name:   stateWildcard,
					action: wildCardState,
				})
			}
		case '?':
			if pctx.currentState.name != stateQuestionMark {
				pctx.pushState(&state{
					name:   stateQuestionMark,
					action: questionMarkState,
				})
			}
		}

		if err = pctx.currentState.action(&pctx.currentState.actionContext, literals.String(), dest); err != nil {
			return "", err
		}

		if pctx.Depth() > 1 && pctx.currentState.isDone() {
			pctx.popState()
		}
		literals.Reset()
	}
}
