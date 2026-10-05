package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestApprovalAndReleaseValidateOriginalReasonBeforeTruncation(t *testing.T) {
	canary := "original-reason-pem-canary-198"
	reasons := []string{
		strings.Repeat("ordinary-reason-", 50) + "-----BEGIN PRIVATE KEY-----" + canary + strings.Repeat("private-material", 50) + "-----END PRIVATE KEY-----",
		strings.Repeat("ordinary-reason-", 50) + "-----BEGIN RSA PRIVATE KEY-----" + canary + strings.Repeat("private-material", 50),
	}
	for _, reason := range reasons {
		_, approvalErr := Approve(context.Background(), nil, ApprovalRequest{Reason: reason})
		if !errors.Is(approvalErr, ErrApprovalRequest) || !strings.Contains(approvalErr.Error(), "reason") || strings.Contains(approvalErr.Error(), canary) {
			t.Errorf("approval reason should fail pre-truncation with a generic field error, got %v", approvalErr)
		}
		_, releaseErr := Release(context.Background(), nil, nil, ReleaseRequest{Reason: reason})
		if !errors.Is(releaseErr, ErrReleaseRequest) || !strings.Contains(releaseErr.Error(), "reason") || strings.Contains(releaseErr.Error(), canary) {
			t.Errorf("release reason should fail pre-truncation with a generic field error, got %v", releaseErr)
		}
	}
}
