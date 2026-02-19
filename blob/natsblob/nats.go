// Package natsblob provides a blob implementation backed by NATS JetStream Object Store.
// Use OpenBucket to construct a *blob.Bucket.
//
// # URLs
//
// For blob.OpenBucket natsblob registers for the scheme "natsblob".
// To customize the URL opener, or for more details on the URL format,
// see URLOpener.
// See https://gocloud.dev/concepts/urls/ for background information.
//
// # As
//
// natsblob exposes jetstream.JetStreamError via ErrorAs.
package natsblob

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

const defaultPageSize = 1000

var (
	errNotFound       = errors.New("blob not found")
	errNotImplemented = errors.New("not implemented")
)

// bucket implements driver.Bucket backed by NATS JetStream Object Store.
type bucket struct {
	store   jetstream.ObjectStore
	config  Config
	closeFn func()
}

// OpenBucket creates a *blob.Bucket backed by NATS JetStream Object Store.
func OpenBucket(store jetstream.ObjectStore, opts ...Option) *blob.Bucket {
	cfg := Config{}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	return blob.NewBucket(&bucket{store: store, config: cfg})
}

func (b *bucket) Close() error {
	if b.closeFn != nil {
		b.closeFn()
	}
	return nil
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	switch {
	case errors.Is(err, errNotFound),
		errors.Is(err, jetstream.ErrObjectNotFound),
		errors.Is(err, jetstream.ErrBucketNotFound):
		return gcerrors.NotFound
	case errors.Is(err, errNotImplemented):
		return gcerrors.Unimplemented
	case errors.Is(err, context.Canceled):
		return gcerrors.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return gcerrors.DeadlineExceeded
	default:
		return gcerrors.Unknown
	}
}

func (b *bucket) As(i any) bool { return false }

func (b *bucket) ErrorAs(err error, i any) bool {
	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		if target, ok := i.(*jetstream.JetStreamError); ok {
			*target = jsErr
			return true
		}
	}
	return false
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	info, err := b.store.GetInfo(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("natsblob: attributes: %w", err)
	}

	attrs := &driver.Attributes{
		Size:    int64(info.Size),
		ModTime: info.ModTime,
		ETag:    info.NUID,
	}

	if info.Headers != nil {
		attrs.ContentType = info.Headers.Get("Content-Type")
		attrs.CacheControl = info.Headers.Get("Cache-Control")
		attrs.ContentDisposition = info.Headers.Get("Content-Disposition")
		attrs.ContentEncoding = info.Headers.Get("Content-Encoding")
		attrs.ContentLanguage = info.Headers.Get("Content-Language")

		if md5Hex := info.Headers.Get("Content-MD5"); md5Hex != "" {
			if decoded, decErr := hex.DecodeString(md5Hex); decErr == nil {
				attrs.MD5 = decoded
			}
		}
	}

	if len(info.Metadata) > 0 {
		attrs.Metadata = make(map[string]string, len(info.Metadata))
		for k, v := range info.Metadata {
			attrs.Metadata[k] = v
		}
	}

	return attrs, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	result, err := b.store.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("natsblob: read: %w", err)
	}

	data, err := io.ReadAll(result)
	if err != nil {
		return nil, fmt.Errorf("natsblob: read all: %w", err)
	}

	info, err := result.Info()
	if err != nil {
		return nil, fmt.Errorf("natsblob: read info: %w", err)
	}

	var contentType string
	if info.Headers != nil {
		contentType = info.Headers.Get("Content-Type")
	}

	// Apply range.
	if offset > 0 || length >= 0 {
		end := int64(len(data))
		if offset > end {
			offset = end
		}
		if length >= 0 && offset+length < end {
			end = offset + length
		}
		data = data[offset:end]
	}

	if opts.BeforeRead != nil {
		if err := opts.BeforeRead(func(any) bool { return false }); err != nil {
			return nil, err
		}
	}

	return &reader{
		r: io.NopCloser(bytes.NewReader(data)),
		attrs: driver.ReaderAttributes{
			ContentType: contentType,
			ModTime:     info.ModTime,
			Size:        int64(info.Size),
		},
	}, nil
}

func (b *bucket) NewTypedWriter(ctx context.Context, key, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(any) bool { return false }); err != nil {
			return nil, err
		}
	}
	return &writer{
		ctx:         ctx,
		store:       b.store,
		key:         key,
		contentType: contentType,
		opts:        opts,
	}, nil
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	if opts.BeforeCopy != nil {
		if err := opts.BeforeCopy(func(any) bool { return false }); err != nil {
			return err
		}
	}

	// Read source object.
	result, err := b.store.Get(ctx, srcKey)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return errNotFound
		}
		return fmt.Errorf("natsblob: copy read: %w", err)
	}

	data, err := io.ReadAll(result)
	if err != nil {
		return fmt.Errorf("natsblob: copy read all: %w", err)
	}

	info, err := result.Info()
	if err != nil {
		return fmt.Errorf("natsblob: copy info: %w", err)
	}

	// Write to destination with same metadata.
	meta := jetstream.ObjectMeta{
		Name:     dstKey,
		Headers:  info.Headers,
		Metadata: info.Metadata,
	}

	_, err = b.store.Put(ctx, meta, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("natsblob: copy put: %w", err)
	}
	return nil
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	err := b.store.Delete(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return errNotFound
		}
		return fmt.Errorf("natsblob: delete: %w", err)
	}
	return nil
}

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	if opts.BeforeList != nil {
		if err := opts.BeforeList(func(any) bool { return false }); err != nil {
			return nil, err
		}
	}

	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	objects, err := b.store.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return &driver.ListPage{}, nil
		}
		return nil, fmt.Errorf("natsblob: list: %w", err)
	}

	// Sort by name.
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Name < objects[j].Name
	})

	var pageToken string
	if len(opts.PageToken) > 0 {
		pageToken = string(opts.PageToken)
	}

	// Filter, collapse delimiters, paginate.
	var lastPrefix string
	var result []*driver.ListObject
	for _, obj := range objects {
		if obj.Deleted {
			continue
		}
		key := obj.Name

		// Filter by prefix.
		if !strings.HasPrefix(key, opts.Prefix) {
			continue
		}

		// Skip past page token.
		if pageToken != "" && key <= pageToken {
			continue
		}

		listObj := &driver.ListObject{
			Key:     key,
			ModTime: obj.ModTime,
			Size:    int64(obj.Size),
		}

		if opts.Delimiter != "" {
			keyWithoutPrefix := key[len(opts.Prefix):]
			if idx := strings.Index(keyWithoutPrefix, opts.Delimiter); idx != -1 {
				dirPrefix := opts.Prefix + keyWithoutPrefix[:idx+len(opts.Delimiter)]
				if dirPrefix == lastPrefix {
					continue
				}
				listObj = &driver.ListObject{
					Key:   dirPrefix,
					IsDir: true,
				}
				lastPrefix = dirPrefix
			}
		}

		result = append(result, listObj)
		if len(result) > pageSize {
			break
		}
	}

	var page driver.ListPage
	if len(result) > pageSize {
		page.Objects = result[:pageSize]
		page.NextPageToken = []byte(result[pageSize-1].Key)
	} else {
		page.Objects = result
	}
	return &page, nil
}

func (b *bucket) SignedURL(_ context.Context, _ string, _ *driver.SignedURLOptions) (string, error) {
	return "", errNotImplemented
}
