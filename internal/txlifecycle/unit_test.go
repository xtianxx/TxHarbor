package txlifecycle

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/xtianxx/txharbor/internal/eth"
)

func validPrepareRequest() *PrepareRequest {
	return &PrepareRequest{
		AttemptID: "att-1", SigningRequestID: "sr-1", IntentID: "int-1", BindingRef: "bind-1",
		AuthorizationID: "wa-1", AuthorizationVersion: 1, RecoveryVersion: 0, ChainID: 31337,
		Sender: "0x1111111111111111111111111111111111111111",
		Nonce:  "7", TxType: TxTypeDynamicFee,
		GasLimit: "21000", MaxFeePerGas: "1000000000", MaxPriorityFeePerGas: "100000000",
		Asset:     "0x2222222222222222222222222222222222222222",
		Recipient: "0x3333333333333333333333333333333333333333",
		Amount:    "1000",
	}
}

func TestPrepareRequestShapeValidation(t *testing.T) {
	if err := validPrepareRequest().Validate(); err != nil {
		t.Fatalf("valid request refused: %v", err)
	}
	cases := map[string]func(*PrepareRequest){
		"empty attempt":       func(r *PrepareRequest) { r.AttemptID = "" },
		"zero chain":          func(r *PrepareRequest) { r.ChainID = 0 },
		"zero amount":         func(r *PrepareRequest) { r.Amount = "0" },
		"bad nonce":           func(r *PrepareRequest) { r.Nonce = "-1" },
		"legacy with max fee": func(r *PrepareRequest) { r.TxType = TxTypeLegacy; r.GasPrice = "1"; r.MaxFeePerGas = "2" },
		"priority over max":   func(r *PrepareRequest) { r.MaxPriorityFeePerGas = "2000000000" },
		"bad sender":          func(r *PrepareRequest) { r.Sender = "nope" },
		"self replacement":    func(r *PrepareRequest) { r.ReplacementOf = r.AttemptID },
	}
	for name, mutate := range cases {
		req := validPrepareRequest()
		mutate(req)
		if err := req.Validate(); err == nil {
			t.Errorf("%s: expected refusal", name)
		}
	}
}

func TestCanonicalEnvelopeAndContentHashDeterminism(t *testing.T) {
	req := validPrepareRequest()
	env1, err := req.CanonicalEnvelope()
	if err != nil {
		t.Fatal(err)
	}
	env2, err := req.CanonicalEnvelope()
	if err != nil {
		t.Fatal(err)
	}
	if string(env1) != string(env2) {
		t.Fatal("envelope is not deterministic")
	}
	h1, err := req.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := req.ContentHash()
	if h1 != h2 {
		t.Fatal("content hash is not stable")
	}
	if !strings.HasPrefix(h1, "0x") || len(h1) != 66 {
		t.Fatalf("content hash shape: %s", h1)
	}
	// Economic change moves the content hash.
	other := validPrepareRequest()
	other.Amount = "1001"
	h3, _ := other.ContentHash()
	if h3 == h1 {
		t.Fatal("amount change did not move content_hash")
	}
}

func TestTransferCalldataAndExpectedTransfer(t *testing.T) {
	recipient := common.HexToAddress("0x3333333333333333333333333333333333333333")
	amount := big.NewInt(1000)
	data := TransferCalldata(recipient, amount)
	if len(data) != 68 {
		t.Fatalf("calldata length = %d, want 68", len(data))
	}
	asset := common.HexToAddress("0x2222222222222222222222222222222222222222")
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	expected := ExpectedTransfer{Asset: asset, Sender: sender, Recipient: recipient, Amount: amount}
	log := types.Log{
		Address: asset,
		Topics:  []common.Hash{eth.TransferSig, common.BytesToHash(sender.Bytes()), common.BytesToHash(recipient.Bytes())},
		Data:    amount.FillBytes(make([]byte, 32)),
	}
	if ok, reason := expected.Match(log); !ok {
		t.Fatalf("expected transfer did not match: %s", reason)
	}
	// Wrong recipient must not match.
	expected.Recipient = common.HexToAddress("0x4444444444444444444444444444444444444444")
	if ok, reason := expected.Match(log); ok || reason != "recipient" {
		t.Fatalf("wrong recipient matched: ok=%v reason=%s", ok, reason)
	}
}

func TestFindExpectedTransferReasons(t *testing.T) {
	asset := common.HexToAddress("0x2222222222222222222222222222222222222222")
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	recipient := common.HexToAddress("0x3333333333333333333333333333333333333333")
	expected := ExpectedTransfer{Asset: asset, Sender: sender, Recipient: recipient, Amount: big.NewInt(1000)}
	if ok, reason := FindExpectedTransfer(nil, expected); ok || reason != "transfer_missing" {
		t.Fatalf("empty logs: ok=%v reason=%s", ok, reason)
	}
	wrongAmount := types.Log{
		Address: asset,
		Topics:  []common.Hash{eth.TransferSig, common.BytesToHash(sender.Bytes()), common.BytesToHash(recipient.Bytes())},
		Data:    big.NewInt(999).FillBytes(make([]byte, 32)),
	}
	if ok, reason := FindExpectedTransfer([]types.Log{wrongAmount}, expected); ok || reason != "amount" {
		t.Fatalf("wrong amount: ok=%v reason=%s", ok, reason)
	}
}

func TestReceiptEffectVerdicts(t *testing.T) {
	asset := common.HexToAddress("0x2222222222222222222222222222222222222222")
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	recipient := common.HexToAddress("0x3333333333333333333333333333333333333333")
	expected := ExpectedTransfer{Asset: asset, Sender: sender, Recipient: recipient, Amount: big.NewInt(1000)}
	good := &types.Log{
		Address: asset,
		Topics:  []common.Hash{eth.TransferSig, common.BytesToHash(sender.Bytes()), common.BytesToHash(recipient.Bytes())},
		Data:    big.NewInt(1000).FillBytes(make([]byte, 32)),
	}
	if effect, _ := receiptEffect(1, []*types.Log{good}, expected); effect != "effective" {
		t.Fatalf("effect = %s, want effective", effect)
	}
	if effect, _ := receiptEffect(0, []*types.Log{good}, expected); effect != "ineffective_status" {
		t.Fatalf("status=0 effect = %s", effect)
	}
	if effect, _ := receiptEffect(1, nil, expected); effect != "ineffective_transfer_missing" {
		t.Fatalf("missing transfer effect = %s", effect)
	}
	bad := &types.Log{
		Address: asset,
		Topics:  []common.Hash{eth.TransferSig, common.BytesToHash(sender.Bytes()), common.BytesToHash(recipient.Bytes())},
		Data:    big.NewInt(999).FillBytes(make([]byte, 32)),
	}
	if effect, _ := receiptEffect(1, []*types.Log{bad}, expected); effect != "ineffective_transfer_mismatch" {
		t.Fatalf("mismatch effect = %s", effect)
	}
	// Extra logs alongside the expected one stay effective.
	if effect, _ := receiptEffect(1, []*types.Log{bad, good}, expected); effect != "effective" {
		t.Fatalf("extra logs effect = %s", effect)
	}
}

func TestClassifyDispatchTable(t *testing.T) {
	cases := []struct {
		err     error
		outcome string
		class   string
	}{
		{nil, "accepted", ""},
		{&eth.Error{Kind: eth.KindAlreadyKnown}, "accepted", "already_known"},
		{&eth.Error{Kind: eth.KindNonceTooLow}, "rejected", "nonce_too_low"},
		{&eth.Error{Kind: eth.KindReplacementUnderpriced}, "rejected", "replacement_underpriced"},
		{&eth.Error{Kind: eth.KindInsufficientFunds}, "rejected", "insufficient_funds"},
		{&eth.Error{Kind: eth.KindIntrinsicGasTooLow}, "rejected", "intrinsic_gas_too_low"},
		{&eth.Error{Kind: eth.KindTimeout}, "unknown", "timeout"},
		{&eth.Error{Kind: eth.KindRateLimited}, "unknown", "rate_limited"},
		{&eth.Error{Kind: eth.KindHashMismatch}, "unknown", "hash_mismatch"},
		{&eth.Error{Kind: eth.KindInvalidResponse}, "unknown", "invalid_response"},
		{&eth.Error{Kind: eth.KindTransport}, "unknown", "transport"},
	}
	for _, c := range cases {
		outcome, class := classifyDispatch(c.err)
		if outcome != c.outcome || class != c.class {
			t.Errorf("classifyDispatch(%v) = (%s,%s), want (%s,%s)", c.err, outcome, class, c.outcome, c.class)
		}
	}
}

func TestSendabilityMatrix(t *testing.T) {
	cases := []struct {
		kind     SendKind
		state    string
		accepted bool
		class    RefusalClass
	}{
		{SendInitial, "signed", false, ""},
		{SendInitial, "prepared", false, ClassAttemptNotSendable},
		{SendInitial, "sent", true, ClassAlreadyAccepted},
		{SendInitial, "unknown", false, ClassAlreadyAccepted},
		{SendInitial, "effective", false, ClassAttemptNotSendable},
		{SendReplay, "sent", true, ""},
		{SendReplay, "unknown", false, ""},
		{SendReplay, "signed", false, ""},
		{SendReplay, "prepared", false, ClassSendModeMismatch},
		{SendReplay, "replaced", false, ClassAttemptNotSendable},
	}
	for _, c := range cases {
		ref := sendabilityRefusal(c.kind, c.state, c.accepted)
		got := RefusalClass("")
		if ref != nil {
			got = ref.Class
		}
		if got != c.class {
			t.Errorf("sendability(%s,%s,%v) = %s, want %s", c.kind, c.state, c.accepted, got, c.class)
		}
	}
}

func TestCheckFeeScopeExact(t *testing.T) {
	// gas_limit × max_fee_per_gas must use exact big.Int arithmetic.
	a := &Attempt{TxType: int(TxTypeDynamicFee), GasLimit: "21000", MaxFeePerGas: "1000000000", MaxPriorityFeePerGas: "100000000"}
	if ref := checkFeeScope(a, 21000000000000, 1000000000, 100000000); ref != nil {
		t.Fatalf("in-range scope refused: %v", ref)
	}
	if ref := checkFeeScope(a, 20999999999999, 1000000000, 100000000); ref == nil || ref.Class != ClassFeeScopeExceeded || ref.Field != "fee_max_total" {
		t.Fatalf("total-cap breach = %v", ref)
	}
	if ref := checkFeeScope(a, 21000000000000, 999999999, 100000000); ref == nil || ref.Field != "fee_max_per_gas" {
		t.Fatalf("per-gas breach = %v", ref)
	}
	if ref := checkFeeScope(a, 21000000000000, 1000000000, 99999999); ref == nil || ref.Field != "fee_max_priority" {
		t.Fatalf("priority breach = %v", ref)
	}
	// A product that overflows int64 still compares exactly.
	huge := &Attempt{TxType: int(TxTypeDynamicFee), GasLimit: "115792089237316195423570985008687907853269984665640564039457584007913129639935", MaxFeePerGas: "2"}
	if ref := checkFeeScope(huge, 1, 2, 0); ref == nil || ref.Field != "fee_max_total" {
		t.Fatalf("overflow product = %v", ref)
	}
}

func TestFeeDimensionsDiffer(t *testing.T) {
	anchor := &Attempt{TxType: int(TxTypeDynamicFee), GasLimit: "21000", MaxFeePerGas: "1000000000", MaxPriorityFeePerGas: "100000000"}
	same := validPrepareRequest()
	same.MaxFeePerGas = "1000000000"
	same.MaxPriorityFeePerGas = "100000000"
	same.GasLimit = "21000"
	if feeDimensionsDiffer(same, anchor) {
		t.Fatal("identical fee dimensions reported as changed")
	}
	raised := validPrepareRequest()
	raised.MaxFeePerGas = "2000000000"
	if !feeDimensionsDiffer(raised, anchor) {
		t.Fatal("raised max_fee not detected")
	}
}

func TestReconcileTransition(t *testing.T) {
	if to, from := reconcileTransition("found_pending", "unknown"); to != "sent" || len(from) != 1 {
		t.Fatalf("unknown + found_pending = %s", to)
	}
	if to, _ := reconcileTransition("not_found_yet", "sent"); to != "unknown" {
		t.Fatalf("sent + not_found_yet = %s", to)
	}
	if to, _ := reconcileTransition("not_found_yet", "signed"); to != "" {
		t.Fatalf("signed + not_found_yet must not change state, got %s", to)
	}
	if to, _ := reconcileTransition("unavailable", "unknown"); to != "" {
		t.Fatalf("unavailable must not change state, got %s", to)
	}
}

func TestReceiptTransition(t *testing.T) {
	if to, extra := receiptTransition("sent", "effective", false, false); to != "" {
		t.Fatalf("unverified receipt must not transition, got %s %s", to, extra)
	}
	if to, _ := receiptTransition("sent", "effective", true, false); to != "effective" {
		t.Fatalf("sent + effective canonical = %s", to)
	}
	if to, extra := receiptTransition("effective", "effective", true, true); to != "confirmed" || !strings.Contains(extra, "confirmed_at") {
		t.Fatalf("confirmed transition = %s %s", to, extra)
	}
	if to, _ := receiptTransition("sent", "ineffective_status", true, false); to != "ineffective" {
		t.Fatalf("ineffective transition = %s", to)
	}
}

func TestRefusalTaxonomyNewClasses(t *testing.T) {
	if RetryabilityOf(ClassIntentFrozen) != RetryAfterStateChange {
		t.Fatal("intent_frozen must be retryable after manual release")
	}
	if RetryabilityOf(ClassReleaseNotPermitted) != RetryNever {
		t.Fatal("release_not_permitted must never retry")
	}
}

func TestEventVocabulary(t *testing.T) {
	for _, e := range []string{EventPostFinalCheckExpiry, EventRegionAbortedNoDispatch, EventFrozen, EventReleased} {
		if !ValidEvent(e) {
			t.Errorf("event %s missing from vocabulary", e)
		}
	}
	if ValidEvent("not_an_event") {
		t.Fatal("unknown event accepted")
	}
}

func TestSignedBytesReconstructionFixedVector(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	req := validPrepareRequest()
	req.Sender = strings.ToLower(sender.Hex())
	unsigned, err := req.Transaction()
	if err != nil {
		t.Fatal(err)
	}
	signer := types.LatestSignerForChainID(new(big.Int).SetUint64(req.ChainID))
	signed, err := types.SignTx(unsigned, signer, key)
	if err != nil {
		t.Fatal(err)
	}
	expectedBytes, err := signed.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	v, r, s := signed.RawSignatureValues()
	sig := make([]byte, 65)
	r.FillBytes(sig[0:32])
	s.FillBytes(sig[32:64])
	sig[64] = byte(v.Uint64())

	gotBytes, gotHash, err := reconstructSignedBytes(req, "0x"+hex.EncodeToString(sig))
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(gotBytes) != hex.EncodeToString(expectedBytes) {
		t.Fatal("reconstructed bytes differ from the signed transaction")
	}
	if gotHash != crypto.Keccak256Hash(expectedBytes) {
		t.Fatal("reconstructed hash mismatch")
	}
	// A tampered signature must fail the sender cross-check.
	sig[64] ^= 0x01
	if _, _, err := reconstructSignedBytes(req, "0x"+hex.EncodeToString(sig)); err == nil {
		t.Fatal("tampered signature accepted")
	}
}
