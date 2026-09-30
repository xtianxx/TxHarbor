package controlstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/xtianxx/txharbor/internal/logx"
)

// credentialFieldNames is an exact-name set, deliberately not a substring
// matcher: evidence_token, target_guard_key and key_fingerprint are ordinary
// recovery metadata, not credential fields.
var credentialFieldNames = map[string]bool{
	"authorization": true,
	"password":      true, "passwd": true, "pwd": true, "pgpassword": true,
	"privatekey": true, "token": true,
	"apikey": true, "accesskey": true,
	"secret": true, "clientsecret": true,
	"credential": true, "credentials": true,
	"dsn": true, "databaseurl": true, "url": true, "uri": true,
}

var privateKeyPEM = regexp.MustCompile(`(?i)-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----`)

// client_secret is part of the recognized structured-field vocabulary even
// though the shared log redactor's legacy pattern does not yet include it.
var clientSecretValue = regexp.MustCompile(`(?i)(?:\bclient[_-]?secret\s*=\s*(?:"[^"]*"|'[^']*'|[^\s,;&?]+)|[?&]client[_-]?secret=([^&\s]+))`)

func normalizedCredentialField(name string) bool {
	name = strings.TrimSpace(name)
	name = strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "-", ""), "_", ""))
	return credentialFieldNames[name]
}

func redactedMarker(value string) bool {
	value = strings.TrimSpace(value)
	return value == logx.Redacted || value == "<redacted>" || value == "[redacted]"
}

// validateCredentialText checks only recognizable credential encodings.
// Comparing with the shared log redactor keeps URL, keyword/value, query
// parameter, and bearer detection aligned with the diagnostics boundary.
func validateCredentialText(field, value string) error {
	if redactedMarker(value) {
		return nil
	}
	if logx.Redact(value) != value || clientSecretValue.MatchString(value) || privateKeyPEM.MatchString(value) {
		return fmt.Errorf("%s contains credential material", field)
	}
	return nil
}

// ValidateCredentialText applies the control-store credential-text policy to
// caller-supplied free text. Errors identify only the supplied field label.
func ValidateCredentialText(field, value string) error {
	return validateCredentialText(field, value)
}

func validateCredentialJSON(field string, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%s must be valid JSON: %w", field, err)
	}
	if err := validateCredentialJSONValue(value); err != nil {
		return fmt.Errorf("%s contains credential material", field)
	}
	return nil
}

// ValidateCredentialJSON applies the control-store credential policy to JSON
// keys and values recursively. Errors never include the offending key, value,
// or path.
func ValidateCredentialJSON(field string, raw []byte) error {
	return validateCredentialJSON(field, raw)
}

func validateCredentialJSONValue(value any) error {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if err := validateCredentialText("key", key); err != nil {
				return errors.New("sensitive key text")
			}
			if normalizedCredentialField(key) {
				if text, ok := child.(string); !ok || !redactedMarker(text) {
					return errors.New("sensitive field")
				}
			}
			if err := validateCredentialJSONValue(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := validateCredentialJSONValue(child); err != nil {
				return err
			}
		}
	case string:
		if err := validateCredentialText("value", v); err != nil {
			return err
		}
	}
	return nil
}
