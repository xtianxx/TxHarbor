// Package health owns dependency probing, the readiness aggregate and the
// /livez + /readyz HTTP handlers.
package health

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/xtianxx/txharbor/internal/logx"
)

// ErrNotChecked is the initial state of every check before the first probe.
var ErrNotChecked = errors.New("not checked yet")

// Aggregate holds the latest result of every readiness check. Ready means all
// checks passed (data-model §3).
type Aggregate struct {
	mu     sync.RWMutex
	order  []string
	checks map[string]error
}

// New creates an aggregate that starts fully not-ready.
func New(names ...string) *Aggregate {
	a := &Aggregate{checks: make(map[string]error, len(names))}
	a.order = append(a.order, names...)
	for _, name := range names {
		a.checks[name] = ErrNotChecked
	}
	return a
}

// Set records the latest result for one check (nil error = ok).
func (a *Aggregate) Set(name string, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.checks[name]; !ok {
		a.order = append(a.order, name)
	}
	a.checks[name] = err
}

// Names returns check names in registration order.
func (a *Aggregate) Names() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return append([]string(nil), a.order...)
}

// Ready reports whether every check passed.
func (a *Aggregate) Ready() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.order) == 0 {
		return false
	}
	for _, name := range a.order {
		if a.checks[name] != nil {
			return false
		}
	}
	return true
}

// Report is a readyz snapshot.
type Report struct {
	Ready  bool
	Failed []string
	Detail string
}

// Report returns the current aggregate state with failed checks and a
// credential-redacted detail string.
func (a *Aggregate) Report() Report {
	a.mu.RLock()
	defer a.mu.RUnlock()
	rep := Report{Ready: true}
	var details []string
	for _, name := range a.order {
		if err := a.checks[name]; err != nil {
			rep.Ready = false
			rep.Failed = append(rep.Failed, name)
			details = append(details, name+": "+err.Error())
		}
	}
	sort.Strings(rep.Failed)
	rep.Detail = logx.Redact(strings.Join(details, "; "))
	return rep
}
