package restore

import (
	"encoding/json"
	"strings"
	"testing"
)

// buildVsockState constructs a synthetic state.json with a vsock device node,
// mirroring the double-encoded layout CH writes.
func buildVsockState(deviceID string, pending bool, localPortLast uint32) []byte {
	inner, _ := json.Marshal(map[string]any{
		"avail_features":          1,
		"acked_features":          1,
		"local_port_last":         localPortLast,
		"transport_reset_pending": pending,
	})
	tree := map[string]any{
		"snapshots": map[string]any{
			"device-manager": map[string]any{
				"snapshots": map[string]any{
					deviceID: map[string]any{
						"snapshot_data": map[string]any{
							"state": string(inner),
						},
					},
				},
			},
		},
	}
	out, _ := json.Marshal(tree)
	return out
}

func TestParseVsockFromState(t *testing.T) {
	const localPortLast = uint32((1 << 30) + 1234)
	state := buildVsockState("_vsock0", true, localPortLast)

	pending, gotPort, ok, err := parseVsockFromState(state)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !pending {
		t.Error("transport_reset_pending: want true")
	}
	if gotPort != localPortLast {
		t.Errorf("local_port_last: want=%d got=%d", localPortLast, gotPort)
	}
}

func TestParseVsockFromState_Acked(t *testing.T) {
	state := buildVsockState("__vsock", false, 42)
	pending, port, ok, err := parseVsockFromState(state)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !ok || pending {
		t.Fatalf("ok=%t pending=%t, want ok=true pending=false", ok, pending)
	}
	if port != 42 {
		t.Fatalf("local_port_last = %d, want 42", port)
	}
}

func TestParseVsockFromState_MissingDevice(t *testing.T) {
	// Older snapshots may predate patch 0005/0006; no vsock node is not an error.
	state := []byte(`{"snapshots":{"device-manager":{"snapshots":{"__balloon":{"snapshot_data":{"state":"{}"}}}}}}`)
	_, _, ok, err := parseVsockFromState(state)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false")
	}
}

func TestParseVsockFromState_Malformed(t *testing.T) {
	_, _, _, err := parseVsockFromState([]byte("not json"))
	if err == nil {
		t.Fatal("expected error on malformed json")
	}
	if !strings.Contains(err.Error(), "state.json") {
		t.Errorf("error should mention state.json: %v", err)
	}
}
