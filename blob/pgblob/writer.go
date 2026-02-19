package pgblob

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"gocloud.dev/blob/driver"
)

type writer struct {
	ctx         context.Context
	pool        *pgxpool.Pool
	db          *sql.DB
	tableName   string
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
	size := int64(len(data))

	hash := md5.Sum(data)
	md5Hash := hash[:]

	if len(w.opts.ContentMD5) > 0 {
		if !bytes.Equal(md5Hash, w.opts.ContentMD5) {
			return fmt.Errorf("pgblob: content MD5 mismatch")
		}
	}

	etag := fmt.Sprintf("%x", md5Hash)

	md := make(map[string]string)
	for k, v := range w.opts.Metadata {
		md[k] = v
	}
	if w.opts.CacheControl != "" {
		md["Cache-Control"] = w.opts.CacheControl
	}
	if w.opts.ContentDisposition != "" {
		md["Content-Disposition"] = w.opts.ContentDisposition
	}
	if w.opts.ContentEncoding != "" {
		md["Content-Encoding"] = w.opts.ContentEncoding
	}
	if w.opts.ContentLanguage != "" {
		md["Content-Language"] = w.opts.ContentLanguage
	}

	metadataJSON, err := json.Marshal(md)
	if err != nil {
		return fmt.Errorf("pgblob: marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`INSERT INTO %s (key, data, content_type, size, md5_hash, etag, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (key) DO UPDATE SET
	data = EXCLUDED.data,
	content_type = EXCLUDED.content_type,
	size = EXCLUDED.size,
	md5_hash = EXCLUDED.md5_hash,
	etag = EXCLUDED.etag,
	metadata = EXCLUDED.metadata,
	updated_at = NOW()`, w.tableName)

	if w.pool != nil {
		_, err = w.pool.Exec(w.ctx, query, w.key, data, w.contentType, size, md5Hash, etag, metadataJSON)
	} else {
		_, err = w.db.ExecContext(w.ctx, query, w.key, data, w.contentType, size, md5Hash, etag, metadataJSON)
	}
	if err != nil {
		return fmt.Errorf("pgblob: upsert: %w", err)
	}
	return nil
}
