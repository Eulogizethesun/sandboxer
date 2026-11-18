package restore

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/kuasar-sandbox/sandboxer/pkg/perfmark"
)

// vsockDeviceState mirrors the fields of CH's VsockState that the restore
// perf diagnostics need. The state is double-encoded (JSON string inside
// snapshot_data.state), exactly like the balloon device state.
type vsockDeviceState struct {
	// LocalPortLast is the last host-side local port CH allocated (patch 0005).
	LocalPortLast uint32 `json:"local_port_last"`
	// TransportResetPending is true while CH holds the RX gate waiting for the
	// guest to acknowledge VIRTIO_VSOCK_EVENT_TRANSPORT_RESET (patch 0006).
	TransportResetPending bool `json:"transport_reset_pending"`
}

// parseVsockFromState walks state.json, finds the vsock device state under
// snapshots["device-manager"].snapshots["<vsock-id>"], and returns its
// transport_reset_pending flag and local_port_last cursor. The vsock device id
// comes from CH's --vsock id (the platform does not set one, so upstream
// generates "_vsock0"); any device node with a vsock name is accepted.
//
// Returns ok=false (with no error) when the snapshot has no vsock device.
// Returns an error only on malformed JSON.
func parseVsockFromState(stateJSON []byte) (pending bool, localPortLast uint32, ok bool, err error) {
	var tree chSnapshotTree
	if err := json.Unmarshal(stateJSON, &tree); err != nil {
		return false, 0, false, fmt.Errorf("state.json: %w", err)
	}
	dm, found := tree.Snapshots["device-manager"]
	if !found || dm == nil {
		return false, 0, false, nil
	}
	for id, node := range dm.Snapshots {
		if !isVsockSnapshotID(id) || node == nil || node.SnapshotData == nil {
			continue
		}
		var state vsockDeviceState
		if err := json.Unmarshal([]byte(node.SnapshotData.State), &state); err != nil {
			return false, 0, false, fmt.Errorf("vsock state %q inner: %w", id, err)
		}
		return state.TransportResetPending, state.LocalPortLast, true, nil
	}
	return false, 0, false, nil
}

// isVsockSnapshotID accepts the ids CH can assign: user-provided ids from
// --vsock, and the generated "_vsock<N>" form (plus a leading underscore
// variant should CH ever use it).
func isVsockSnapshotID(id string) bool {
	return strings.HasPrefix(strings.TrimLeft(id, "_"), "vsock")
}

// emitRestoreSnapshotState records the captured vsock transport state for
// restore perf analysis. KUASAR_PERF=1 is checked by the caller; this function
// is a no-op if the mark set is disabled (Marks is nil-safe). Diagnostics must
// never fail a restore, so read/parse errors are logged and skipped.
func emitRestoreSnapshotState(p *perfmark.Marks, statePath string, logf func(string, ...any)) {
	stateJSON, err := os.ReadFile(statePath)
	if err != nil {
		logf("restore: read state.json for perf: %v", err)
		return
	}
	pending, localPortLast, ok, err := parseVsockFromState(stateJSON)
	if err != nil {
		logf("restore: parse vsock state for perf: %v", err)
		return
	}
	if !ok {
		logf("restore: state.json has no vsock device; skipping restore.snapshot_state")
		return
	}
	p.MarkKV("restore.snapshot_state", map[string]any{
		"transport_reset_pending": pending,
		"local_port_last":         localPortLast,
	})
}
