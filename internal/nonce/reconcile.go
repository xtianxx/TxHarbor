// reconcile.go owns startup rebuild verification and floor reconciliation:
// re-deriving the durable frontier from nonce_bindings, advancing
// reconciled_floor only on evidence, and gating allocation until the rebuild
// passes (FR-12/FR-13, R7). Skeleton only (T001): doc comments, no behavior.
package nonce
