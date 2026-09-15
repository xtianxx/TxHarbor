// Package withdrawal implements receive-only withdrawal intake: API-key
// authenticated, grant-authorized, idempotent request receipt plus
// owner-restricted query. It never allocates nonces, signs, or broadcasts
// (008+ boundary); persistence truth lives in PostgreSQL (Tables 1-6,
// migrations/000007_withdrawal_creation.sql).
//
// auth.go will own API-key credential verification (T007).
package withdrawal
