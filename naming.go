package gbus

import (
	"fmt"
	"strings"

	"github.com/maxid/gns"
)

// Namer maps logical names onto the physical ones a shared Kafka cluster uses.
//
// A cluster is commonly shared by several environments or tenants, so topic and group
// names carry a namespace prefix. Business code keeps using the logical name
// (`iothub_raw`); the driver applies the prefix at the boundary and strips it again on
// delivery, so a handler never sees a deployment detail.
//
// The separator is configurable because the prefix scheme is a deployment decision:
// `xdmp-dev.iothub_raw` and `xdmp_dev_iothub_raw` are both common.
type Namer struct {
	// Prefix is empty when the deployment has a dedicated cluster.
	Prefix string
	// Separator defaults to ".".
	Separator string
}

// NewNamer builds a namer, validating that the prefix cannot produce an invalid Kafka
// name.
func NewNamer(prefix, separator string) (Namer, error) {
	if separator == "" {
		separator = "."
	}
	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		if strings.HasSuffix(prefix, separator) {
			return Namer{}, fmt.Errorf("%w: prefix %q must not end with the separator", ErrPermanent, prefix)
		}
		if err := validateKafkaName(prefix); err != nil {
			return Namer{}, err
		}
	}
	return Namer{Prefix: prefix, Separator: separator}, nil
}

// Topic returns the physical topic name.
func (n Namer) Topic(logical string) string { return n.join(logical) }

// Group returns the physical consumer-group name. A shared cluster also shares group
// ids, and two environments in one group would steal each other's partitions.
func (n Namer) Group(logical string) string { return n.join(logical) }

// Strip removes the prefix from a physical name. An unexpected name is returned
// unchanged so a handler still sees something rather than an empty topic.
func (n Namer) Strip(physical string) string {
	if n.Prefix == "" {
		return physical
	}
	return strings.TrimPrefix(physical, n.Prefix+n.Separator)
}

func (n Namer) join(logical string) string {
	if n.Prefix == "" || logical == "" {
		return logical
	}
	return gns.Join(n.Separator, n.Prefix, logical)
}

// validateKafkaName rejects the characters Kafka disallows in topic names.
func validateKafkaName(name string) error {
	if len(name) > 249 {
		return fmt.Errorf("%w: namespace %q is too long", ErrPermanent, name)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '_', r == '-':
		default:
			return fmt.Errorf("%w: namespace %q contains %q", ErrPermanent, name, r)
		}
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%w: namespace %q is reserved", ErrPermanent, name)
	}
	return nil
}
