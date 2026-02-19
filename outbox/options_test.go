package outbox

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	relay := NewRelay(nil, nil)

	if relay.config.PollInterval != time.Second {
		t.Errorf("PollInterval = %v, want %v", relay.config.PollInterval, time.Second)
	}
	if relay.config.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", relay.config.BatchSize)
	}
}

func TestWithPollInterval(t *testing.T) {
	relay := NewRelay(nil, nil, WithPollInterval(5*time.Second))

	if relay.config.PollInterval != 5*time.Second {
		t.Errorf("PollInterval = %v, want %v", relay.config.PollInterval, 5*time.Second)
	}
}

func TestWithBatchSize(t *testing.T) {
	relay := NewRelay(nil, nil, WithBatchSize(50))

	if relay.config.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want 50", relay.config.BatchSize)
	}
}
