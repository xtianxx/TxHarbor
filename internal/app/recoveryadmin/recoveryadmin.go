// Package recoveryadmin implements the thin 015 operator command surface
// (`txharbor recovery-admin`): migrate/control/backup/verify-backup/restore/
// instance-open/instance-close/checklist-set/checklist-verify/verify/approve/
// release/status/drill all converge here.
//
// B0 setup skeleton (T004): this batch registers the fixed action surface and
// every action only performs help and argument parsing. No behavior is
// implemented yet — a well-formed invocation reports NOT IMPLEMENTED and exits
// non-zero; it never returns a false success, never substitutes a default for a
// required argument and never starts a backup, restore, verification, approval
// or release flow. Missing arguments and unknown actions are usage errors
// (exit 2); help goes to stdout, refusals and usage errors go to stderr.
//
// T008/T069 implement exactly one action on top of that skeleton: `migrate
// up|status` in migrate.go, scoped to the independent control store
// (TXHARBOR_RECOVERY_CONTROL_DSN) with the same trust-boundary and schema
// version guard the store uses. Every other action is still the stub.
//
// It lives in its own package (rather than internal/app) for the same reason as
// internal/app/reconcileadmin: the delivered command imports internal/recovery
// and its source adapters, which keeps that build/test graph independent of
// internal/app. `internal/app` stays the assembly point for the capability gate
// wiring (T030-T034/T070); this package owns the command surface only.
//
// Boundary notes for the owning batches that follow: T004 is the single
// registration owner (later CLI tasks add their own files on top of the fixed
// action set); the authenticated principal comes from the deployment-controlled
// TXHARBOR_RECOVERY_PRINCIPAL binding and never from a flag; --operator/--reason
// style free text is audit carriage only and can never authorize; and every
// required configuration value is refused by its exact key name ("not
// configured") rather than defaulted.
package recoveryadmin

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

// Deps carries the process dependencies so the command is testable in-process
// (the same shape as internal/app.Deps, reduced to what this package needs).
type Deps struct {
	Getenv func(string) (string, bool)
	Stdout io.Writer
	Stderr io.Writer
}

func (d Deps) stdout() io.Writer {
	if d.Stdout == nil {
		return io.Discard
	}
	return d.Stdout
}

func (d Deps) stderr() io.Writer {
	if d.Stderr == nil {
		return io.Discard
	}
	return d.Stderr
}

// recoveryAdminFlag is one parsed flag of an action. Every value is carried as
// text in B0; typed parsing belongs to the owning batch.
type recoveryAdminFlag struct {
	name  string
	usage string
}

// recoveryAdminAction is one action of the fixed surface. In B0 it carries the
// help/parse metadata only; behavior lands with the owning batch and is added
// in its own file.
type recoveryAdminAction struct {
	// name is the action token (`txharbor recovery-admin <name>`).
	name string
	// summary is the one-line help text.
	summary string
	// usage is the accepted argument form without the command prefix.
	usage string
	// flags are the accepted flags.
	flags []recoveryAdminFlag
	// required lists flag names that must be present and non-blank. A missing
	// required flag is refused by name; no default is invented (production
	// thresholds and authorization material never come from this stub).
	required []string
	// positional, when non-empty, is the closed set of required positional
	// tokens (e.g. migrate's up|status).
	positional []string
}

// recoveryAdminActions is the fixed action surface, in help order.
var recoveryAdminActions = []recoveryAdminAction{
	{
		name:       "migrate",
		summary:    "apply/show the control-store schema versions (control DSN only)",
		usage:      "migrate up|status",
		positional: []string{"up", "status"},
	},
	{
		name:       "control",
		summary:    "manage participants and identity mappings",
		usage:      "control participant-register|identity-map-set|identity-map-show",
		positional: []string{"participant-register", "identity-map-set", "identity-map-show"},
	},
	{
		name:    "backup",
		summary: "produce a backup artifact and its manifest (unverified until verify-backup)",
		usage:   "backup --chain-id CHAIN [--out DIR]",
		flags: []recoveryAdminFlag{
			{name: "chain-id", usage: "scope chain identity (required)"},
			{name: "out", usage: "artifact output directory override (defaults to the configured artifact dir)"},
		},
		required: []string{"chain-id"},
	},
	{
		name:    "verify-backup",
		summary: "restore a backup into an isolated target and verify it",
		usage:   "verify-backup --manifest M --target-dsn TARGET",
		flags: []recoveryAdminFlag{
			{name: "manifest", usage: "manifest path (required)"},
			{name: "target-dsn", usage: "isolated target DSN (required)"},
		},
		required: []string{"manifest", "target-dsn"},
	},
	{
		name:    "restore",
		summary: "restore a verified manifest into the bound recovery instance",
		usage:   "restore --manifest M --target-dsn TARGET --instance ID",
		flags: []recoveryAdminFlag{
			{name: "manifest", usage: "manifest path (required)"},
			{name: "target-dsn", usage: "target DSN (required; isolated unless explicitly declared)"},
			{name: "instance", usage: "recovery instance id (required)"},
		},
		required: []string{"manifest", "target-dsn", "instance"},
	},
	{
		name:    "instance-open",
		summary: "open a recovery instance (executor recorded; at most one open)",
		usage:   "instance-open --kind recovery|baseline [--reason R]",
		flags: []recoveryAdminFlag{
			{name: "kind", usage: "recovery|baseline (required)"},
			{name: "reason", usage: "audit annotation (optional)"},
		},
		required: []string{"kind"},
	},
	{
		name:    "instance-close",
		summary: "close the open recovery instance (only when every capability is validly released)",
		usage:   "instance-close --instance ID",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
		},
		required: []string{"instance"},
	},
	{
		name:    "checklist-set",
		summary: "record isolation-checklist evidence (executor)",
		usage:   "checklist-set --instance ID --item ITEM",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "item", usage: "checklist item key (required)"},
		},
		required: []string{"instance", "item"},
	},
	{
		name:    "checklist-verify",
		summary: "confirm a checklist item (non-executor verifier)",
		usage:   "checklist-verify --instance ID --item ITEM",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "item", usage: "checklist item key (required)"},
		},
		required: []string{"instance", "item"},
	},
	{
		name:    "verify",
		summary: "run bounded V1-V9 fact verification for the instance",
		usage:   "verify --instance ID --scope SCOPE",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "scope", usage: "verification scope (required)"},
		},
		required: []string{"instance", "scope"},
	},
	{
		name:    "approve",
		summary: "record an approval (single/dual; executor excluded)",
		usage:   "approve --instance ID --capability CAP",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "capability", usage: "capability name (required)"},
		},
		required: []string{"instance", "capability"},
	},
	{
		name:    "release",
		summary: "release one capability (derived evaluation; nothing is writable)",
		usage:   "release --instance ID --capability CAP",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (required)"},
			{name: "capability", usage: "capability name (required)"},
		},
		required: []string{"instance", "capability"},
	},
	{
		name:    "status",
		summary: "show the read-only per-capability restored/verified/released view",
		usage:   "status [--instance ID]",
		flags: []recoveryAdminFlag{
			{name: "instance", usage: "recovery instance id (optional)"},
		},
	},
	{
		name:    "drill",
		summary: "record a full disaster-recovery drill (independent drill channel)",
		usage:   "drill",
	},
}

// recoveryAdminActionByName returns the action with the given token.
func recoveryAdminActionByName(name string) (recoveryAdminAction, bool) {
	for _, action := range recoveryAdminActions {
		if action.name == name {
			return action, true
		}
	}
	return recoveryAdminAction{}, false
}

// Run runs the 015 operator command surface. `migrate up|status` (T008) is
// implemented in migrate.go; every other action is still the B0 stub (help
// and argument parsing only, no behavior).
func Run(ctx context.Context, args []string, d Deps) int {
	if len(args) == 0 {
		recoveryAdminUsage(d.stderr())
		return 2
	}
	switch args[0] {
	case "help", "-h", "--help":
		return recoveryAdminHelp(args[1:], d)
	case "migrate":
		// T008/T069: the control-store provisioning path is implemented in
		// migrate.go; every other action remains the B0 help/parse stub.
		return recoveryAdminMigrate(ctx, args[1:], d)
	}
	action, ok := recoveryAdminActionByName(args[0])
	if !ok {
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor recovery-admin: unknown action %q\n", args[0])
		recoveryAdminUsage(stderr)
		return 2
	}
	return recoveryAdminStub(ctx, action, args[1:], d)
}

// recoveryAdminHelp prints the full action surface, or one action's usage when
// a known action is named.
func recoveryAdminHelp(args []string, d Deps) int {
	if len(args) == 0 {
		recoveryAdminUsage(d.stdout())
		return 0
	}
	action, ok := recoveryAdminActionByName(args[0])
	if !ok || len(args) > 1 {
		stderr := d.stderr()
		fmt.Fprintf(stderr, "txharbor recovery-admin: unknown action %q\n", args[0])
		recoveryAdminUsage(stderr)
		return 2
	}
	recoveryAdminActionUsage(d.stdout(), action)
	return 0
}

// recoveryAdminStub parses one action's arguments and refuses to act. Parse
// failures, unexpected arguments and missing required arguments are usage
// errors (2); a complete invocation reports NOT IMPLEMENTED and exits 1 — a
// stub must never return a false success and never starts a recovery flow.
func recoveryAdminStub(ctx context.Context, action recoveryAdminAction, args []string, d Deps) int {
	_ = ctx // B0: no behavior, no control-store or artifact access.
	stdout, stderr := d.stdout(), d.stderr()

	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			recoveryAdminActionUsage(stdout, action)
			return 0
		}
	}

	fs := flag.NewFlagSet("txharbor recovery-admin "+action.name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { recoveryAdminActionUsage(stderr, action) }
	values := make(map[string]*string, len(action.flags))
	for _, f := range action.flags {
		values[f.name] = fs.String(f.name, "", f.usage)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			recoveryAdminActionUsage(stdout, action)
			return 0
		}
		return 2
	}

	if len(action.positional) > 0 {
		if fs.NArg() != 1 {
			recoveryAdminActionUsage(stderr, action)
			return 2
		}
		token := strings.TrimSpace(fs.Arg(0))
		known := false
		for _, want := range action.positional {
			if token == want {
				known = true
				break
			}
		}
		if !known {
			fmt.Fprintf(stderr, "txharbor recovery-admin %s: unknown argument %q (want %s)\n",
				action.name, fs.Arg(0), strings.Join(action.positional, "|"))
			recoveryAdminActionUsage(stderr, action)
			return 2
		}
	} else if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: unexpected argument %q\n", action.name, fs.Arg(0))
		recoveryAdminActionUsage(stderr, action)
		return 2
	}

	var missing []string
	for _, name := range action.required {
		value, ok := values[name]
		if !ok || strings.TrimSpace(*value) == "" {
			missing = append(missing, "--"+name)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(stderr, "txharbor recovery-admin %s: missing required %s\n", action.name, strings.Join(missing, ", "))
		recoveryAdminActionUsage(stderr, action)
		return 2
	}

	fmt.Fprintf(stderr,
		"txharbor recovery-admin %s: NOT IMPLEMENTED in the B0 setup skeleton; no action taken (no backup/restore/verification/release flow is started and no success is claimed)\n",
		action.name)
	return 1
}

// recoveryAdminUsage prints the full fixed action surface.
func recoveryAdminUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: txharbor recovery-admin <action> [args]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "actions:")
	for _, action := range recoveryAdminActions {
		fmt.Fprintf(w, "  %-16s %s\n", action.name, action.summary)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "B0 setup skeleton: every action performs help/argument parsing only except `migrate up|status` (T008), which acts on the independent control store only; no backup/recovery flow is implemented and no invocation claims success. A stub invocation exits non-zero with NOT IMPLEMENTED; missing arguments and unknown actions exit 2. Required configuration values are refused by name and never defaulted.")
}

// recoveryAdminActionUsage prints one action's accepted argument form.
func recoveryAdminActionUsage(w io.Writer, action recoveryAdminAction) {
	fmt.Fprintf(w, "usage: txharbor recovery-admin %s\n", action.usage)
	for _, f := range action.flags {
		fmt.Fprintf(w, "  --%s\t%s\n", f.name, f.usage)
	}
}
