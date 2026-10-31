package gbus

import (
	"errors"
	"testing"
)

func TestNamerAppliesTheNamespace(t *testing.T) {
	namer, err := NewNamer("xdmp-dev", ".") //nolint:misspell // namespace comes from config
	if err != nil {
		t.Fatalf("namer: %v", err)
	}

	if got := namer.Topic("iothub_raw"); got != "xdmp-dev.iothub_raw" {
		t.Fatalf("topic = %q", got)
	}
	if got := namer.Group("iot-app"); got != "xdmp-dev.iot-app" {
		t.Fatalf("group = %q", got)
	}
	if got := namer.Strip("xdmp-dev.iothub_raw"); got != "iothub_raw" {
		t.Fatalf("strip = %q", got)
	}
}

// Without a prefix the names pass through untouched: a dedicated cluster needs no
// namespacing.
func TestNamerWithoutPrefix(t *testing.T) {
	namer, err := NewNamer("", "")
	if err != nil {
		t.Fatalf("namer: %v", err)
	}
	if got := namer.Topic("iothub_raw"); got != "iothub_raw" {
		t.Fatalf("topic = %q", got)
	}
	if got := namer.Strip("iothub_raw"); got != "iothub_raw" {
		t.Fatalf("strip = %q", got)
	}
}

func TestNamerCustomSeparator(t *testing.T) {
	namer, _ := NewNamer("xdmp_dev", "_")
	if got := namer.Topic("iothub_raw"); got != "xdmp_dev_iothub_raw" {
		t.Fatalf("topic = %q", got)
	}
	if got := namer.Strip("xdmp_dev_iothub_raw"); got != "iothub_raw" {
		t.Fatalf("strip = %q", got)
	}
}

// A prefix that Kafka would reject must fail at construction, not on the first publish.
func TestNamerRejectsInvalidPrefixes(t *testing.T) {
	cases := []string{
		"xdmp dev", // space
		"xdmp/dev", // slash
		"xdmp:dev", // colon
		".",        // reserved
		"..",       // reserved
	}
	for _, prefix := range cases {
		if _, err := NewNamer(prefix, "."); !errors.Is(err, ErrPermanent) {
			t.Errorf("prefix %q: err = %v, want ErrPermanent", prefix, err)
		}
	}

	if _, err := NewNamer("xdmp-dev.", "."); !errors.Is(err, ErrPermanent) {
		t.Error("a prefix ending with the separator must be rejected")
	}
}
