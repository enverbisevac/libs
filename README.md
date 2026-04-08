# libs

🚀 Usage:

Install/Update Tools:

make update-tools

Run Modernize:

make modernize

Run CI Tasks:

make ci

## queue

Work queue with single-worker delivery, priority + FIFO, delays, retries, and
dead letter routing. Backends: `inmem`, `sqlite`, `pgx` (Postgres), `redis`.

```go
import (
    "github.com/enverbisevac/libs/queue"
    "github.com/enverbisevac/libs/queue/inmem"
)

svc := inmem.New()
defer svc.Close(ctx)

cons, _ := svc.Subscribe(ctx, "emails", func(ctx context.Context, j *queue.Job) error {
    return sendEmail(j.Payload)
}, queue.WithConcurrency(4))
defer cons.Close()

_ = svc.Enqueue(ctx, "emails", []byte("hello@example.com"))
```

See `queue/doc.go` for the idempotency contract and
`docs/superpowers/specs/2026-04-08-queue-design.md` for design rationale.
