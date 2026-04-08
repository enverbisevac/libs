package inmem_test

import (
	"testing"

	"github.com/enverbisevac/libs/queue"
	"github.com/enverbisevac/libs/queue/inmem"
	"github.com/enverbisevac/libs/queue/queuetest"
)

func TestConformance(t *testing.T) {
	queuetest.Run(t, func(t *testing.T) (queue.Service, func()) {
		svc := inmem.New()
		return svc, func() { _ = svc.Close(t.Context()) }
	})
}
