// Package pgblob provides a blob implementation backed by PostgreSQL bytea storage.
// Use OpenBucket to construct a *blob.Bucket.
//
// # URLs
//
// For blob.OpenBucket pgblob registers for the scheme "pgblob".
// To customize the URL opener, or for more details on the URL format,
// see URLOpener.
// See https://gocloud.dev/concepts/urls/ for background information.
//
// # As
//
// pgblob exposes *pgconn.PgError via ErrorAs.
package pgblob

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

const defaultPageSize = 1000

var (
	errNotFound       = errors.New("blob not found")
	errNotImplemented = errors.New("not implemented")
)

// bucket implements driver.Bucket backed by PostgreSQL.
type bucket struct {
	pool    *pgxpool.Pool
	db      *sql.DB
	config  Config
	closeFn func()
}

// OpenBucket creates a *blob.Bucket backed by PostgreSQL using pgxpool.
func OpenBucket(pool *pgxpool.Pool, opts ...Option) *blob.Bucket {
	cfg := Config{TableName: "blobs"}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	return blob.NewBucket(&bucket{pool: pool, config: cfg})
}

// OpenBucketStdLib creates a *blob.Bucket backed by PostgreSQL using database/sql.
func OpenBucketStdLib(db *sql.DB, opts ...Option) *blob.Bucket {
	cfg := Config{TableName: "blobs"}
	for _, o := range opts {
		o.Apply(&cfg)
	}
	return blob.NewBucket(&bucket{db: db, config: cfg})
}

func (b *bucket) Close() error {
	if b.closeFn != nil {
		b.closeFn()
	}
	return nil
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	switch {
	case errors.Is(err, errNotFound):
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
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if target, ok := i.(**pgconn.PgError); ok {
			*target = pgErr
			return true
		}
	}
	return false
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	query := fmt.Sprintf(
		`SELECT content_type, size, md5_hash, etag, metadata, created_at, updated_at FROM %s WHERE key = $1`,
		b.config.TableName,
	)

	var (
		contentType string
		size        int64
		md5Hash     []byte
		etag        *string
		metaJSON    []byte
		createdAt   time.Time
		updatedAt   time.Time
	)

	var scanErr error
	if b.pool != nil {
		scanErr = b.pool.QueryRow(ctx, query, key).Scan(
			&contentType, &size, &md5Hash, &etag, &metaJSON, &createdAt, &updatedAt,
		)
	} else {
		scanErr = b.db.QueryRowContext(ctx, query, key).Scan(
			&contentType, &size, &md5Hash, &etag, &metaJSON, &createdAt, &updatedAt,
		)
	}
	if scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) || errors.Is(scanErr, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("pgblob: attributes: %w", scanErr)
	}

	attrs := &driver.Attributes{
		ContentType: contentType,
		Size:        size,
		MD5:         md5Hash,
		CreateTime:  createdAt,
		ModTime:     updatedAt,
	}
	if etag != nil {
		attrs.ETag = *etag
	}

	if len(metaJSON) > 0 {
		md := make(map[string]string)
		if err := json.Unmarshal(metaJSON, &md); err != nil {
			return nil, fmt.Errorf("pgblob: unmarshal metadata: %w", err)
		}
		// Extract well-known HTTP headers from metadata.
		attrs.CacheControl = md["Cache-Control"]
		delete(md, "Cache-Control")
		attrs.ContentDisposition = md["Content-Disposition"]
		delete(md, "Content-Disposition")
		attrs.ContentEncoding = md["Content-Encoding"]
		delete(md, "Content-Encoding")
		attrs.ContentLanguage = md["Content-Language"]
		delete(md, "Content-Language")
		attrs.Metadata = md
	}

	return attrs, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	var (
		data        []byte
		contentType string
		size        int64
		updatedAt   time.Time
	)

	if offset > 0 || length >= 0 {
		// Range read using SUBSTRING. PostgreSQL SUBSTRING is 1-based.
		var query string
		if length >= 0 {
			query = fmt.Sprintf(
				`SELECT SUBSTRING(data FROM %d FOR %d), content_type, size, updated_at FROM %s WHERE key = $1`,
				offset+1, length, b.config.TableName,
			)
		} else {
			query = fmt.Sprintf(
				`SELECT SUBSTRING(data FROM %d), content_type, size, updated_at FROM %s WHERE key = $1`,
				offset+1, b.config.TableName,
			)
		}
		var scanErr error
		if b.pool != nil {
			scanErr = b.pool.QueryRow(ctx, query, key).Scan(&data, &contentType, &size, &updatedAt)
		} else {
			scanErr = b.db.QueryRowContext(ctx, query, key).Scan(&data, &contentType, &size, &updatedAt)
		}
		if scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) || errors.Is(scanErr, sql.ErrNoRows) {
				return nil, errNotFound
			}
			return nil, fmt.Errorf("pgblob: range read: %w", scanErr)
		}
	} else {
		// Full read.
		query := fmt.Sprintf(
			`SELECT data, content_type, size, updated_at FROM %s WHERE key = $1`,
			b.config.TableName,
		)
		var scanErr error
		if b.pool != nil {
			scanErr = b.pool.QueryRow(ctx, query, key).Scan(&data, &contentType, &size, &updatedAt)
		} else {
			scanErr = b.db.QueryRowContext(ctx, query, key).Scan(&data, &contentType, &size, &updatedAt)
		}
		if scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) || errors.Is(scanErr, sql.ErrNoRows) {
				return nil, errNotFound
			}
			return nil, fmt.Errorf("pgblob: read: %w", scanErr)
		}
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
			ModTime:     updatedAt,
			Size:        size,
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
		pool:        b.pool,
		db:          b.db,
		tableName:   b.config.TableName,
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

	query := fmt.Sprintf(
		`INSERT INTO %s (key, data, content_type, size, md5_hash, etag, metadata, created_at, updated_at)
SELECT $1, data, content_type, size, md5_hash, etag, metadata, NOW(), NOW()
FROM %s WHERE key = $2
ON CONFLICT (key) DO UPDATE SET
	data = EXCLUDED.data,
	content_type = EXCLUDED.content_type,
	size = EXCLUDED.size,
	md5_hash = EXCLUDED.md5_hash,
	etag = EXCLUDED.etag,
	metadata = EXCLUDED.metadata,
	updated_at = NOW()`,
		b.config.TableName, b.config.TableName,
	)

	var (
		tag pgconn.CommandTag
		err error
	)
	if b.pool != nil {
		tag, err = b.pool.Exec(ctx, query, dstKey, srcKey)
	} else {
		var result sql.Result
		result, err = b.db.ExecContext(ctx, query, dstKey, srcKey)
		if err == nil {
			n, _ := result.RowsAffected()
			tag = pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", n))
		}
	}
	if err != nil {
		return fmt.Errorf("pgblob: copy: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errNotFound
	}
	return nil
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE key = $1`, b.config.TableName)

	var (
		rowsAffected int64
		err          error
	)
	if b.pool != nil {
		tag, execErr := b.pool.Exec(ctx, query, key)
		rowsAffected = tag.RowsAffected()
		err = execErr
	} else {
		result, execErr := b.db.ExecContext(ctx, query, key)
		if execErr == nil {
			rowsAffected, _ = result.RowsAffected()
		}
		err = execErr
	}
	if err != nil {
		return fmt.Errorf("pgblob: delete: %w", err)
	}
	if rowsAffected == 0 {
		return errNotFound
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

	var pageToken string
	if len(opts.PageToken) > 0 {
		pageToken = string(opts.PageToken)
	}

	query := fmt.Sprintf(
		`SELECT key, size, md5_hash, updated_at FROM %s WHERE key LIKE $1 AND key > $2 ORDER BY key LIMIT $3`,
		b.config.TableName,
	)
	prefix := opts.Prefix
	likePattern := strings.ReplaceAll(prefix, `%`, `\%`) + "%%"

	// Fetch pageSize+1 to know if there's a next page (only when no delimiter).
	// When delimiter is used, we need more rows since collapsing reduces count.
	fetchLimit := pageSize + 1
	if opts.Delimiter != "" {
		fetchLimit = pageSize*10 + 1
	}

	type rowData struct {
		key       string
		size      int64
		md5Hash   []byte
		updatedAt time.Time
	}

	var rows []rowData
	if b.pool != nil {
		pgxRows, err := b.pool.Query(ctx, query, likePattern, pageToken, fetchLimit)
		if err != nil {
			return nil, fmt.Errorf("pgblob: list: %w", err)
		}
		defer pgxRows.Close()
		for pgxRows.Next() {
			var r rowData
			if err := pgxRows.Scan(&r.key, &r.size, &r.md5Hash, &r.updatedAt); err != nil {
				return nil, fmt.Errorf("pgblob: list scan: %w", err)
			}
			rows = append(rows, r)
		}
		if err := pgxRows.Err(); err != nil {
			return nil, fmt.Errorf("pgblob: list rows: %w", err)
		}
	} else {
		sqlRows, err := b.db.QueryContext(ctx, query, likePattern, pageToken, fetchLimit)
		if err != nil {
			return nil, fmt.Errorf("pgblob: list: %w", err)
		}
		defer func() { _ = sqlRows.Close() }()
		for sqlRows.Next() {
			var r rowData
			if err := sqlRows.Scan(&r.key, &r.size, &r.md5Hash, &r.updatedAt); err != nil {
				return nil, fmt.Errorf("pgblob: list scan: %w", err)
			}
			rows = append(rows, r)
		}
		if err := sqlRows.Err(); err != nil {
			return nil, fmt.Errorf("pgblob: list rows: %w", err)
		}
	}

	// Process rows into ListObjects, handling delimiter collapsing.
	var lastPrefix string
	var objects []*driver.ListObject
	for _, r := range rows {
		obj := &driver.ListObject{
			Key:     r.key,
			ModTime: r.updatedAt,
			Size:    r.size,
			MD5:     r.md5Hash,
		}

		if opts.Delimiter != "" {
			keyWithoutPrefix := r.key[len(prefix):]
			if idx := strings.Index(keyWithoutPrefix, opts.Delimiter); idx != -1 {
				dirPrefix := prefix + keyWithoutPrefix[:idx+len(opts.Delimiter)]
				if dirPrefix == lastPrefix {
					continue
				}
				obj = &driver.ListObject{
					Key:   dirPrefix,
					IsDir: true,
				}
				lastPrefix = dirPrefix
			}
		}

		objects = append(objects, obj)
		if len(objects) > pageSize {
			break
		}
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	var result driver.ListPage
	if len(objects) > pageSize {
		result.Objects = objects[:pageSize]
		result.NextPageToken = []byte(objects[pageSize-1].Key)
	} else {
		result.Objects = objects
	}
	return &result, nil
}

func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	return "", errNotImplemented
}
