package recovery

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestChecklistSecrecyValidators(t *testing.T) {
	const password = "canary-checklist-password-81d2"
	const token = "canary-checklist-token-3c91"
	const hexCredential = "private_key=" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cases := []struct {
		name  string
		value string
	}{
		{"URI password", "postgres://operator:" + password + "@db.example.test/app"},
		{"keyword token", "token=" + token},
		{"private PEM", "-----BEGIN PRIVATE KEY-----\ncanary\n-----END PRIVATE KEY-----"},
		{"64-hex credential field", hexCredential},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateChecklistText("evidence_ref", tc.value); err == nil || strings.Contains(err.Error(), tc.value) {
				t.Fatalf("ValidateChecklistText accepted or echoed unsafe text: %v", err)
			}
			raw := []byte(`{"nested":{"context":` + quoteJSON(tc.value) + `}}`)
			if err := ValidateChecklistSummary(raw); err == nil || strings.Contains(err.Error(), tc.value) {
				t.Fatalf("ValidateChecklistSummary accepted or echoed unsafe JSON: %v", err)
			}
		})
	}
	for _, safe := range []string{"evidence://015/checkpoint-7", "routine operator checkpoint"} {
		if err := ValidateChecklistText("evidence_ref", safe); err != nil {
			t.Fatalf("safe text %q refused: %v", safe, err)
		}
	}
	if err := ValidateChecklistSummary([]byte(`{"source":"operator checkpoint","count":2}`)); err != nil {
		t.Fatalf("safe summary refused: %v", err)
	}
}

func TestChecklistSummaryRejectsCredentialFieldNamesRecursively(t *testing.T) {
	const hexKey = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	cases := []struct {
		name string
		raw  string
	}{
		{"authorization in nested array", `{"checks":[{"authorization":"Basic encoded"}]}`},
		{"privatekey in nested array", `{"checks":[{"privatekey":"` + hexKey + `"}]}`},
		{"authorization separator alias", `{"details":{"Authorization":"Basic encoded"}}`},
		{"private key separator alias", `{"details":{"private-key":"` + hexKey + `"}}`},
		{"api key alias", `{"details":{"api-key":"present"}}`},
		{"access key alias", `{"details":{"access_key":"present"}}`},
		{"client secret alias", `{"details":{"client_secret":"present"}}`},
		{"client secret hyphen alias", `{"checks":[{"client-secret":"present"}]}`},
		{"whitespace padded private key in array", `{"checks":[{" private_key ":"present"}]}`},
		{"whitespace padded authorization in array", `{"checks":[{" authorization ":"present"}]}`},
		{"whitespace padded client secret in array", `{"checks":[{" client_secret ":"present"}]}`},
		{"credential-shaped key text", `{"details":{"password=canary-checklist":"ordinary"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateChecklistSummary([]byte(tc.raw)); err == nil {
				t.Fatal("credential field name accepted")
			}
		})
	}

	for _, raw := range []string{
		`{"status":"healthy","count":2}`,
		`{"status_token_count":2,"authorization_status":"unknown"}`,
		`{"status":"authorization_recheck","target_guard_key":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","key_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`,
		`{" status ":"healthy"," key_fingerprint ":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}`,
	} {
		if err := ValidateChecklistSummary([]byte(raw)); err != nil {
			t.Fatalf("ordinary status fields refused: %s: %v", raw, err)
		}
	}
}

func quoteJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
