package sandbox

import (
	"testing"
	"time"
)

func TestVsockResetSettings(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		enabled bool
		timeout time.Duration
	}{
		{
			name:    "unset keeps legacy behavior",
			enabled: false,
			timeout: defaultVsockResetTimeout,
		},
		{
			name:    "exactly one enables",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "1"},
			enabled: true,
			timeout: defaultVsockResetTimeout,
		},
		{
			name:    "zero disables",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "0"},
			enabled: false,
			timeout: defaultVsockResetTimeout,
		},
		{
			name:    "true is not one",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "true"},
			enabled: false,
			timeout: defaultVsockResetTimeout,
		},
		{
			name:    "positive timeout override",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "1", vsockResetTimeoutEnv: "250"},
			enabled: true,
			timeout: 250 * time.Millisecond,
		},
		{
			name:    "whitespace around timeout",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "1", vsockResetTimeoutEnv: " 125 "},
			enabled: true,
			timeout: 125 * time.Millisecond,
		},
		{
			name:    "invalid timeout keeps default",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "1", vsockResetTimeoutEnv: "soon"},
			enabled: true,
			timeout: defaultVsockResetTimeout,
		},
		{
			name:    "zero timeout keeps default",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "1", vsockResetTimeoutEnv: "0"},
			enabled: true,
			timeout: defaultVsockResetTimeout,
		},
		{
			name:    "negative timeout keeps default",
			env:     map[string]string{vsockResetBeforeFreezeEnv: "1", vsockResetTimeoutEnv: "-5"},
			enabled: true,
			timeout: defaultVsockResetTimeout,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(key string) string { return tt.env[key] }
			enabled, timeout := vsockResetSettings(getenv)
			if enabled != tt.enabled {
				t.Fatalf("enabled = %t, want %t", enabled, tt.enabled)
			}
			if timeout != tt.timeout {
				t.Fatalf("timeout = %s, want %s", timeout, tt.timeout)
			}
		})
	}
}
