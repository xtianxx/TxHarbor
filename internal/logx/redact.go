// Package logx provides credential redaction for anything that might be
// logged or echoed: connection strings, DSNs, URLs, tokens and secrets.
//
// Redaction keeps non-sensitive structural context (scheme, user, host,
// port, database name, keys) so failures stay diagnosable.
package logx

import "regexp"

// Redacted is the placeholder that replaces every credential value.
const Redacted = "[REDACTED]"

var (
	// scheme://user:password@host -> scheme://user:[REDACTED]@host
	uriCreds = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^/@\s:]+:)([^@\s/]+)(@)`)

	// Keys that carry credentials in keyword/value DSNs and logfmt output.
	secretKey = `(?:password|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key|private[_-]?key|authorization)`

	// password=secret / password='s e c r e t' / password="..."
	// The bare value stops at whitespace, comma, semicolon and URL separators
	// so neighbouring keys survive redaction.
	kvSecret = regexp.MustCompile(`(?i)\b(` + secretKey + `)\s*=\s*("[^"]*"|'[^']*'|[^\s,;&?]+)`)

	// ?password=secret&token=... URL query parameters.
	urlParam = regexp.MustCompile(`(?i)([?&](?:` + secretKey + `)=)([^&\s]+)`)

	// Authorization: Bearer <token>
	bearer = regexp.MustCompile(`(?i)(Bearer\s+)\S+`)
)

// Redact replaces credential values in s with [REDACTED]. It is safe to call
// on strings that contain no credentials.
func Redact(s string) string {
	// Bearer runs first: the kv/url bare-value classes stop at whitespace, so
	// they would otherwise eat the "Bearer" keyword and leave the token behind
	// (authorization=Bearer <token>). Order matters.
	s = bearer.ReplaceAllString(s, "${1}"+Redacted)
	s = uriCreds.ReplaceAllString(s, "${1}"+Redacted+"${3}")
	s = urlParam.ReplaceAllString(s, "${1}"+Redacted)
	s = kvSecret.ReplaceAllString(s, "${1}="+Redacted)
	return s
}
