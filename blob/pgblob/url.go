package pgblob

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"
	"gocloud.dev/blob"
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, &URLOpener{})
}

// Scheme is the URL scheme pgblob registers its URLOpener under on blob.DefaultMux.
const Scheme = "pgblob"

// URLOpener opens URLs like "pgblob://user:pass@host:port/dbname?table=custom&sslmode=disable".
//
// The following query parameters are consumed by the opener:
//   - table: Sets the table name (default "blobs").
//
// All other query parameters are passed through as Postgres connection string parameters.
//
// If Pool is set, the connection string from the URL is ignored and the
// existing pool is used instead.
type URLOpener struct {
	Pool *pgxpool.Pool
}

// OpenBucketURL opens a blob.Bucket based on u.
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	var opts []Option

	q := u.Query()
	if t := q.Get("table"); t != "" {
		opts = append(opts, WithTableName(t))
		q.Del("table")
	}

	if o.Pool != nil {
		return OpenBucket(o.Pool, opts...), nil
	}

	// Build connection string from URL.
	connURL := *u
	connURL.Scheme = "postgres"
	connURL.RawQuery = q.Encode()

	pool, err := pgxpool.New(ctx, connURL.String())
	if err != nil {
		return nil, fmt.Errorf("pgblob: open pool: %w", err)
	}

	cfg := Config{TableName: "blobs"}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	b := &bucket{
		pool:   pool,
		config: cfg,
		closeFn: func() {
			pool.Close()
		},
	}
	return blob.NewBucket(b), nil
}
