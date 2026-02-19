package natsblob

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"maps"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gocloud.dev/blob/driver"
)

type writer struct {
	ctx         context.Context
	store       jetstream.ObjectStore
	key         string
	contentType string
	opts        *driver.WriterOptions
	buf         bytes.Buffer
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *writer) Close() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}

	data := w.buf.Bytes()

	hash := md5.Sum(data)
	md5Hash := hash[:]

	if len(w.opts.ContentMD5) > 0 {
		if !bytes.Equal(md5Hash, w.opts.ContentMD5) {
			return fmt.Errorf("natsblob: content MD5 mismatch")
		}
	}

	headers := make(nats.Header)
	if w.contentType != "" {
		headers.Set("Content-Type", w.contentType)
	}
	headers.Set("Content-MD5", hex.EncodeToString(md5Hash))
	if w.opts.CacheControl != "" {
		headers.Set("Cache-Control", w.opts.CacheControl)
	}
	if w.opts.ContentDisposition != "" {
		headers.Set("Content-Disposition", w.opts.ContentDisposition)
	}
	if w.opts.ContentEncoding != "" {
		headers.Set("Content-Encoding", w.opts.ContentEncoding)
	}
	if w.opts.ContentLanguage != "" {
		headers.Set("Content-Language", w.opts.ContentLanguage)
	}

	meta := jetstream.ObjectMeta{
		Name:    w.key,
		Headers: headers,
	}
	if len(w.opts.Metadata) > 0 {
		meta.Metadata = make(map[string]string)
		maps.Copy(meta.Metadata, w.opts.Metadata)
	}

	_, err := w.store.Put(w.ctx, meta, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("natsblob: put: %w", err)
	}
	return nil
}
