package s3_log

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Record is a WAL record stored as an S3 object.
type Record struct {
	Offset uint64
	Data   []byte
}

// WAL defines the minimal interface you used originally.
type WAL interface {
	Append(ctx context.Context, data []byte) (uint64, error)
	Read(ctx context.Context, offset uint64) (Record, error)
	LastRecord(ctx context.Context) (Record, error)
}

// S3WAL stores each record in its own S3 object under the configured prefix.
// Object key format: <prefix>/<zero-padded-20-digit-offset>
type S3WAL struct {
	client     *s3.Client
	bucketName string
	prefix     string

	mu     sync.Mutex // protects length
	length uint64     // last known offset, 0 means unknown/empty
}

// NewS3WAL constructs a WAL instance. It does NOT touch S3. Call Recover(ctx) to sync length.
func NewS3WAL(client *s3.Client, bucketName, prefix string) *S3WAL {
	// normalize prefix: remove leading/trailing slashes
	trimmed := strings.Trim(prefix, "/")
	return &S3WAL{
		client:     client,
		bucketName: bucketName,
		prefix:     trimmed,
		length:     0,
	}
}

// getObjectKey builds the object key for an offset.
func (w *S3WAL) getObjectKey(offset uint64) string {
	return w.prefix + "/" + fmt.Sprintf("%020d", offset)
}

// getOffsetFromKey extracts offset from an object key. It handles keys like "prefix/000...".
func (w *S3WAL) getOffsetFromKey(key string) (uint64, error) {
	// find last slash and take substring after it
	idx := strings.LastIndexByte(key, '/')
	if idx < 0 || idx == len(key)-1 {
		return 0, fmt.Errorf("invalid key format: %q", key)
	}
	numStr := key[idx+1:]
	return strconv.ParseUint(numStr, 10, 64)
}

// prepareBody writes: [8-byte offset BE][data][32-byte sha256(offset+data)]
// It streams writes to the hasher to avoid extra copies.
func prepareBody(offset uint64, data []byte) ([]byte, error) {
	buf := &bytes.Buffer{}
	hasher := sha256.New()
	writer := io.MultiWriter(buf, hasher)

	// write offset to both buffer and hasher
	if err := binary.Write(writer, binary.BigEndian, offset); err != nil {
		return nil, fmt.Errorf("write offset: %w", err)
	}

	// write data to both buffer and hasher
	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("write data: %w", err)
	}

	// append checksum (hasher.Sum(nil) does not modify hasher)
	checksum := hasher.Sum(nil) // 32 bytes
	if len(checksum) != sha256.Size {
		return nil, errors.New("unexpected checksum length")
	}
	if _, err := buf.Write(checksum); err != nil {
		return nil, fmt.Errorf("append checksum: %w", err)
	}
	return buf.Bytes(), nil
}

// validateChecksum returns true if the trailing 32 bytes equal sha256(dataWithoutChecksum).
func validateChecksum(full []byte) bool {
	if len(full) < sha256.Size {
		return false
	}
	dataPart := full[:len(full)-sha256.Size]
	stored := full[len(full)-sha256.Size:]

	sum := sha256.Sum256(dataPart)
	return bytes.Equal(sum[:], stored)
}

// Append uploads a new object and bumps w.length. It's mutex-protected.
func (w *S3WAL) Append(ctx context.Context, data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	next := w.length + 1
	body, err := prepareBody(next, data)
	if err != nil {
		return 0, fmt.Errorf("prepare body: %w", err)
	}

	input := &s3.PutObjectInput{
    Bucket:      aws.String(w.bucketName),
    Key:         aws.String(w.getObjectKey(next)),
    Body:        bytes.NewReader(body),
    
}


	if _, err := w.client.PutObject(ctx, input); err != nil {
		return 0, fmt.Errorf("put object (offset=%d): %w", next, err)
	}

	w.length = next
	return next, nil
}

// Read downloads object at offset and returns parsed Record.
func (w *S3WAL) Read(ctx context.Context, offset uint64) (Record, error) {
	key := w.getObjectKey(offset)
	input := &s3.GetObjectInput{
		Bucket: aws.String(w.bucketName),
		Key:    aws.String(key),
	}
	out, err := w.client.GetObject(ctx, input)
	if err != nil {
		return Record{}, fmt.Errorf("get object %s: %w", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return Record{}, fmt.Errorf("read object %s body: %w", key, err)
	}

	if len(data) < 8+sha256.Size {
		return Record{}, fmt.Errorf("invalid record (too short) for key %s", key)
	}

	// read offset prefix
	var storedOffset uint64
	if err := binary.Read(bytes.NewReader(data[:8]), binary.BigEndian, &storedOffset); err != nil {
		return Record{}, fmt.Errorf("parse offset from object %s: %w", key, err)
	}
	if storedOffset != offset {
		return Record{}, fmt.Errorf("offset mismatch for key %s: expected %d, got %d", key, offset, storedOffset)
	}

	if !validateChecksum(data) {
		return Record{}, fmt.Errorf("checksum mismatch for offset %d (key %s)", offset, key)
	}

	recordData := make([]byte, len(data)-8-sha256.Size)
	copy(recordData, data[8:len(data)-sha256.Size])

	return Record{
		Offset: storedOffset,
		Data:   recordData,
	}, nil
}

// LastRecord finds the object with the highest offset and returns it.
// It updates w.length accordingly and uses a mutex to avoid races.
func (w *S3WAL) LastRecord(ctx context.Context) (Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// List objects with prefix + "/"
	prefix := w.prefix + "/"
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucketName),
		Prefix: aws.String(prefix),
	}

	paginator := s3.NewListObjectsV2Paginator(w.client, input)

	var lastKey string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return Record{}, fmt.Errorf("list objects: %w", err)
		}
		// pick the last key of this page if any; because keys are lexicographically ordered,
		// the last key across all pages will be the last key on the last non-empty page.
		if len(page.Contents) > 0 {
			lastKey = *page.Contents[len(page.Contents)-1].Key
		}
	}

	if lastKey == "" {
		// WAL empty
		w.length = 0
		return Record{}, fmt.Errorf("WAL is empty")
	}

	offset, err := w.getOffsetFromKey(lastKey)
	if err != nil {
		return Record{}, fmt.Errorf("parse offset from last key %s: %w", lastKey, err)
	}

	// update cached length
	w.length = offset

	// read and return the record
	return w.Read(ctx, offset)
}

// Recover inspects S3 and sets w.length to the highest offset present.
// It is safe to call at startup to initialize the in-memory offset state.
func (w *S3WAL) Recover(ctx context.Context) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	prefix := w.prefix + "/"
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucketName),
		Prefix: aws.String(prefix),
	}

	paginator := s3.NewListObjectsV2Paginator(w.client, input)

	var maxOffset uint64 = 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("list objects during recover: %w", err)
		}
		for _, obj := range page.Contents {
			if reflect.DeepEqual(obj, types.Object{}) || obj.Key == nil {
				continue
			}
			offset, err := w.getOffsetFromKey(*obj.Key)
			if err != nil {
				// ignore keys that don't match pattern instead of failing outright
				continue
			}
			if offset > maxOffset {
				maxOffset = offset
			}
		}
	}

	w.length = maxOffset
	return maxOffset, nil
}

// Truncate deletes all objects with offset > afterOffset. It performs batched DeleteObjects calls.
// If afterOffset == 0, it deletes all objects under the prefix.
func (w *S3WAL) Truncate(ctx context.Context, afterOffset uint64) error {
	// We do not need to hold w.mu for the duration of the listing and deletion,
	// but we will update length under lock at the end.
	prefix := w.prefix + "/"
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucketName),
		Prefix: aws.String(prefix),
	}

	paginator := s3.NewListObjectsV2Paginator(w.client, input)

	var keysToDelete []types.ObjectIdentifier
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects during truncate: %w", err)
		}
		for _, obj := range page.Contents {
			if reflect.DeepEqual(obj, types.Object{}) || obj.Key == nil {
				continue
			}
			offset, err := w.getOffsetFromKey(*obj.Key)
			if err != nil {
				// ignore non-matching keys
				continue
			}
			if offset > afterOffset {
				keysToDelete = append(keysToDelete, types.ObjectIdentifier{Key: obj.Key})
			}
			// batch-delete in chunks of 1000 (S3 limit is 1000)
			if len(keysToDelete) == 1000 {
				if err := w.batchDelete(ctx, keysToDelete); err != nil {
					return err
				}
				keysToDelete = keysToDelete[:0]
			}
		}
	}
	if len(keysToDelete) > 0 {
		if err := w.batchDelete(ctx, keysToDelete); err != nil {
			return err
		}
	}

	// update cached length
	w.mu.Lock()
	if afterOffset == 0 {
		w.length = 0
	} else {
		w.length = afterOffset
	}
	w.mu.Unlock()
	return nil
}

func (w *S3WAL) batchDelete(ctx context.Context, keys []types.ObjectIdentifier) error {
	if len(keys) == 0 {
		return nil
	}
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(w.bucketName),
		Delete: &types.Delete{
			Objects: keys,
			Quiet:   aws.Bool(false),
		},
	}
	out, err := w.client.DeleteObjects(ctx, input)
	if err != nil {
		return fmt.Errorf("delete objects: %w", err)
	}
	if len(out.Errors) > 0 {
		// concatenate errors for better debugging
		var parts []string
		for _, e := range out.Errors {
			parts = append(parts, fmt.Sprintf("%s: %s", aws.ToString(e.Key), aws.ToString(e.Message)))
		}
		return fmt.Errorf("delete objects errors: %s", strings.Join(parts, "; "))
	}
	return nil
}
