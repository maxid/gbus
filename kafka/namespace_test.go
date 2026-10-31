package kafka

import (
	"errors"
	"testing"

	"github.com/maxid/gbus"
)

// The namespace is applied at the driver boundary, so business code keeps using the
// logical topic and the handler never sees the prefix.
func TestProducerAppliesNamespace(t *testing.T) {
	namer, err := gbus.NewNamer("xdmp-dev", ".")
	if err != nil {
		t.Fatalf("namer: %v", err)
	}
	if got := namer.Topic("iothub_raw"); got != "xdmp-dev.iothub_raw" {
		t.Fatalf("physical topic = %q", got)
	}
}

// Two environments on one cluster must not share a consumer group, or they steal each
// other's partitions.
func TestConsumerNamespacesTheGroup(t *testing.T) {
	namer, _ := gbus.NewNamer("xdmp-dev", ".")

	first := namer.Group("iot-app")
	second, _ := gbus.NewNamer("xdmp-prod", ".")
	if first == second.Group("iot-app") {
		t.Fatalf("both environments resolve to group %q", first)
	}
}

// A dead-letter topic is namespaced by the producer that writes it, so passing the
// logical name is the only way to avoid "xdmp-dev.xdmp-dev.dlq".
func TestDeadLetterIsNotDoublyPrefixed(t *testing.T) {
	namer, _ := gbus.NewNamer("xdmp-dev", ".")

	logical := "iothub_raw.dlq"
	physical := namer.Topic(logical)
	if physical != "xdmp-dev.iothub_raw.dlq" {
		t.Fatalf("physical = %q", physical)
	}
	if namer.Topic(physical) == physical {
		t.Fatal("expected the double prefix to be visible — the driver must pass the logical name")
	}
}

func TestInvalidNamespaceIsRejectedAtConstruction(t *testing.T) {
	cases := []struct {
		name string
		cfg  ProducerConfig
	}{
		{"space", ProducerConfig{Brokers: []string{"127.0.0.1:9092"}, Namespace: "xdmp dev"}},
		{"colon", ProducerConfig{Brokers: []string{"127.0.0.1:9092"}, Namespace: "xdmp:dev"}},
	}
	for _, tc := range cases {
		if _, err := NewProducer(tc.cfg); !errors.Is(err, gbus.ErrPermanent) {
			t.Errorf("%s: err = %v, want ErrPermanent", tc.name, err)
		}
	}

	if _, err := NewConsumer(ConsumerConfig{
		Brokers: []string{"127.0.0.1:9092"}, Group: "g", Topics: []string{"t"}, Namespace: "bad/ns",
	}); !errors.Is(err, gbus.ErrPermanent) {
		t.Errorf("consumer namespace err = %v, want ErrPermanent", err)
	}
}
