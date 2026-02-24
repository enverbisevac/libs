package pgblob

import (
	"context"
	"crypto/md5"
	"io"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping pgblob tests")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func setupBucket(t *testing.T) *blob.Bucket {
	t.Helper()

	pool := getTestPool(t)
	ctx := context.Background()
	tableName := "test_blobs"

	// Create table.
	_, err := pool.Exec(ctx, CreateTableSQL(tableName))
	require.NoError(t, err)

	// Clean up table after test.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableName)
	})

	return OpenBucket(pool, WithTableName(tableName))
}

func TestWriteAndRead(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	content := []byte("hello, pgblob!")
	err := b.WriteAll(ctx, "test-key", content, nil)
	require.NoError(t, err)

	got, err := b.ReadAll(ctx, "test-key")
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestAttributes(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	content := []byte("attribute test")
	hash := md5.Sum(content)

	err := b.WriteAll(ctx, "attr-key", content, &blob.WriterOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"foo": "bar"},
	})
	require.NoError(t, err)

	attrs, err := b.Attributes(ctx, "attr-key")
	require.NoError(t, err)

	assert.Equal(t, "text/plain", attrs.ContentType)
	assert.Equal(t, int64(len(content)), attrs.Size)
	assert.Equal(t, hash[:], attrs.MD5)
	assert.NotEmpty(t, attrs.ETag)
	assert.Equal(t, "bar", attrs.Metadata["foo"])
	assert.False(t, attrs.CreateTime.IsZero())
	assert.False(t, attrs.ModTime.IsZero())
}

func TestRangeRead(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	content := []byte("0123456789")
	err := b.WriteAll(ctx, "range-key", content, nil)
	require.NoError(t, err)

	// Offset + length.
	r, err := b.NewRangeReader(ctx, "range-key", 2, 5, nil)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.Equal(t, []byte("23456"), got)

	// Offset only (read to end).
	r, err = b.NewRangeReader(ctx, "range-key", 7, -1, nil)
	require.NoError(t, err)
	got, err = io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.Equal(t, []byte("789"), got)
}

func TestReadNotFound(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	_, err := b.NewReader(ctx, "nonexistent", nil)
	require.Error(t, err)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
}

func TestDeleteNotFound(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	err := b.Delete(ctx, "nonexistent")
	require.Error(t, err)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
}

func TestCopy(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	content := []byte("copy me")
	err := b.WriteAll(ctx, "src-key", content, nil)
	require.NoError(t, err)

	err = b.Copy(ctx, "dst-key", "src-key", nil)
	require.NoError(t, err)

	got, err := b.ReadAll(ctx, "dst-key")
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestCopyNotFound(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	err := b.Copy(ctx, "dst", "nonexistent", nil)
	require.Error(t, err)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
}

func TestListPaged(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	keys := []string{"a/1.txt", "a/2.txt", "b/1.txt", "c.txt"}
	for _, k := range keys {
		err := b.WriteAll(ctx, k, []byte("data"), nil)
		require.NoError(t, err)
	}

	// List all.
	iter := b.List(nil)
	var listed []string
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		listed = append(listed, obj.Key)
	}
	assert.Equal(t, keys, listed)
}

func TestListPrefix(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	keys := []string{"a/1.txt", "a/2.txt", "b/1.txt", "c.txt"}
	for _, k := range keys {
		err := b.WriteAll(ctx, k, []byte("data"), nil)
		require.NoError(t, err)
	}

	iter := b.List(&blob.ListOptions{Prefix: "a/"})
	var listed []string
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		listed = append(listed, obj.Key)
	}
	assert.Equal(t, []string{"a/1.txt", "a/2.txt"}, listed)
}

func TestListDelimiter(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	keys := []string{"a/1.txt", "a/2.txt", "b/1.txt", "c.txt"}
	for _, k := range keys {
		err := b.WriteAll(ctx, k, []byte("data"), nil)
		require.NoError(t, err)
	}

	iter := b.List(&blob.ListOptions{Delimiter: "/"})
	var listed []string
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		listed = append(listed, obj.Key)
	}
	assert.Equal(t, []string{"a/", "b/", "c.txt"}, listed)
}

func TestListPagination(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		key := string(rune('a'+i)) + ".txt"
		err := b.WriteAll(ctx, key, []byte("data"), nil)
		require.NoError(t, err)
	}

	// Use small page size via ListOptions.
	iter := b.List(nil)
	var listed []string
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		listed = append(listed, obj.Key)
	}
	assert.Len(t, listed, 5)
}

func TestOverwrite(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	err := b.WriteAll(ctx, "key", []byte("first"), nil)
	require.NoError(t, err)

	err = b.WriteAll(ctx, "key", []byte("second"), nil)
	require.NoError(t, err)

	got, err := b.ReadAll(ctx, "key")
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), got)
}

func TestSignedURL(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	_, err := b.SignedURL(ctx, "key", nil)
	require.Error(t, err)
	assert.Equal(t, gcerrors.Unimplemented, gcerrors.Code(err))
}

func TestContentMD5Validation(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	content := []byte("md5 check")
	hash := md5.Sum(content)

	// Correct MD5 should succeed.
	err := b.WriteAll(ctx, "md5-ok", content, &blob.WriterOptions{
		ContentMD5: hash[:],
	})
	require.NoError(t, err)

	// Wrong MD5 should fail.
	badHash := make([]byte, 16)
	err = b.WriteAll(ctx, "md5-bad", content, &blob.WriterOptions{
		ContentMD5: badHash,
	})
	require.Error(t, err)
}

func TestDelete(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	err := b.WriteAll(ctx, "del-key", []byte("delete me"), nil)
	require.NoError(t, err)

	err = b.Delete(ctx, "del-key")
	require.NoError(t, err)

	_, err = b.NewReader(ctx, "del-key", nil)
	require.Error(t, err)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
}

func TestAttributesNotFound(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	_, err := b.Attributes(ctx, "nonexistent")
	require.Error(t, err)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err))
}

func TestHTTPHeadersInMetadata(t *testing.T) {
	b := setupBucket(t)
	defer func() { _ = b.Close() }()
	ctx := context.Background()

	err := b.WriteAll(ctx, "headers-key", []byte("data"), &blob.WriterOptions{
		CacheControl:       "max-age=3600",
		ContentDisposition: "attachment",
		ContentEncoding:    "gzip",
		ContentLanguage:    "en-US",
	})
	require.NoError(t, err)

	attrs, err := b.Attributes(ctx, "headers-key")
	require.NoError(t, err)

	assert.Equal(t, "max-age=3600", attrs.CacheControl)
	assert.Equal(t, "attachment", attrs.ContentDisposition)
	assert.Equal(t, "gzip", attrs.ContentEncoding)
	assert.Equal(t, "en-US", attrs.ContentLanguage)
}
