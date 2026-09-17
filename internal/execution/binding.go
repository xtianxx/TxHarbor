package execution

import (
	"context"
	"fmt"
)

// BindingResult is the 011-side 008 observation classification (gates.md §3).
// Only BindingMatches permits issuing a send-class step; every other class
// refuses with its mapped refusal class.
type BindingResult int

const (
	BindingMatches BindingResult = iota
	BindingAbsent
	BindingConflict
	BindingPaused
	BindingTerminal
	BindingReadFailed
)

func (r BindingResult) String() string {
	switch r {
	case BindingMatches:
		return "matches"
	case BindingAbsent:
		return "absent"
	case BindingConflict:
		return "conflict"
	case BindingPaused:
		return "paused"
	case BindingTerminal:
		return "terminal"
	case BindingReadFailed:
		return "read_failed"
	default:
		return fmt.Sprintf("unknown(%d)", int(r))
	}
}

// RefusalClass maps the observation to its refusal class; "" means the class
// permits a send-class step. An unknown value fails closed.
func (r BindingResult) RefusalClass() RefusalClass {
	switch r {
	case BindingMatches:
		return ""
	case BindingAbsent:
		return ClassBindingAbsent
	case BindingConflict:
		return ClassBindingConflict
	case BindingPaused:
		return ClassBindingPaused
	case BindingTerminal:
		return ClassBindingTerminal
	default:
		return ClassBindingReadFailed
	}
}

// PermitsSend is true only for BindingMatches.
func (r BindingResult) PermitsSend() bool { return r == BindingMatches }

// BindingReader reads the 008 observation for one intent. The concrete live
// adapter (binding_live.go, build tag integration) reuses 008's exported
// ReadProvider exactly as 009's binding_live.go does, which keeps this
// package's business import graph free of 008's RPC-bearing read package.
type BindingReader interface {
	ReadBinding(ctx context.Context, intentID string) (BindingResult, error)
}
