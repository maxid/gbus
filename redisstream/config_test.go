package redisstream

import (
	"testing"
)

func TestNamespaceStreamKey(t *testing.T) {
	cfg := Config{Namespace: "app"}
	if err := cfg.Normalize(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.streamKey("raw"); got != "app:bus:raw" {
		t.Fatalf("stream key = %q", got)
	}
	if got := cfg.streamKeyToTopic("app:bus:raw"); got != "raw" {
		t.Fatalf("topic = %q", got)
	}
}

func TestNamespaceField(t *testing.T) {
	cfg := Config{Namespace: "app"}
	if err := cfg.Normalize(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.streamKey("raw"); got != "app:bus:raw" {
		t.Fatalf("stream key = %q", got)
	}
}

func TestExplicitPrefixWinsOverNamespace(t *testing.T) {
	cfg := Config{Namespace: "app", StreamKeyPrefix: "bus:"}
	if err := cfg.Normalize(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.streamKey("raw"); got != "bus:raw" {
		t.Fatalf("stream key = %q", got)
	}
}
