package natsblob

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gocloud.dev/blob"
)

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, &URLOpener{})
}

// Scheme is the URL scheme natsblob registers its URLOpener under on blob.DefaultMux.
const Scheme = "natsblob"

// URLOpener opens URLs like "natsblob://user:pass@host:port/bucket-name?token=xxx&create=true".
//
// The path component is the object store bucket name.
//
// The following query parameters are consumed by the opener:
//   - token: NATS auth token.
//   - create: If "true", create the object store bucket if it doesn't exist.
//
// If Store is set, the connection info from the URL is ignored and the
// existing store is used instead.
type URLOpener struct {
	Store jetstream.ObjectStore
}

// OpenBucketURL opens a blob.Bucket based on u.
func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	if o.Store != nil {
		return OpenBucket(o.Store), nil
	}

	q := u.Query()

	// Build NATS connection options.
	var natsOpts []nats.Option
	if u.User != nil {
		username := u.User.Username()
		password, _ := u.User.Password()
		natsOpts = append(natsOpts, nats.UserInfo(username, password))
	}
	if token := q.Get("token"); token != "" {
		natsOpts = append(natsOpts, nats.Token(token))
		q.Del("token")
	}

	create := q.Get("create") == "true"
	q.Del("create")

	// Connect to NATS.
	natsURL := "nats://" + u.Host
	nc, err := nats.Connect(natsURL, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("natsblob: connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natsblob: jetstream: %w", err)
	}

	bucketName := strings.TrimPrefix(u.Path, "/")
	if bucketName == "" {
		nc.Close()
		return nil, fmt.Errorf("natsblob: bucket name required in URL path")
	}

	var store jetstream.ObjectStore
	if create {
		store, err = js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
			Bucket: bucketName,
		})
	} else {
		store, err = js.ObjectStore(ctx, bucketName)
	}
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natsblob: object store: %w", err)
	}

	b := &bucket{
		store:  store,
		config: Config{},
		closeFn: func() {
			nc.Close()
		},
	}
	return blob.NewBucket(b), nil
}
