package store

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Values used for testing.
// The test bucket has a lifetime of 255 days, and
// protected read/write/append access. The timestamp is 0
// so the bucket should be garbage collected when running GC.
var (
	TestBktID     = BucketID([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255, 7})
	TestBktKey    = BucketKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})
	TestBktData   = append([]byte{0, 0, 0, 0}, TestBktKey[:]...)
	TestBktValues = []BucketValue{
		{Idx: 0, Value: []byte("1")},
		{Idx: 2, Value: []byte("2")},
		{Idx: 3, Value: []byte("3")},
		{Idx: 4, Value: []byte("4")},
		{Idx: 5, Value: []byte("5")},
		{Idx: 0, Value: []byte("6")},
		{Idx: 0, Value: []byte("7")},
		{Idx: 0, Value: []byte("8")},
		{Idx: 9, Value: []byte("9")},
		{Idx: 10, Value: []byte("10")},
	}
	ExpectedBktValues = []BucketValue{
		{Idx: 1, Value: []byte("1")},
		{Idx: 2, Value: []byte("2")},
		{Idx: 3, Value: []byte("3")},
		{Idx: 4, Value: []byte("4")},
		{Idx: 5, Value: []byte("5")},
		{Idx: 6, Value: []byte("6")},
		{Idx: 7, Value: []byte("7")},
		{Idx: 8, Value: []byte("8")},
		{Idx: 9, Value: []byte("9")},
		{Idx: 10, Value: []byte("10")},
	}
)

// setupTestStore creates a new test store.
//
// The test store uses a memory-based filesystem for testing
// purposes. The store contains 1 bucket with 10 values. The
// used CacheTTL is set to 24 hours.
func SetupTestStore(t *testing.T, addValues bool) Store {
	str, err := OpenStore("", &StoreOptions{
		PebbleOpts: &pebble.Options{FS: vfs.NewMem()},
		CacheTTL:   24,
	})
	require.NoError(t, err, "could not open test store")
	if !addValues {
		return str
	}

	// Add bucket to the store.
	db := str.(*pebbleStore).db
	batch := db.NewBatch()
	require.NoError(
		t,
		batch.Set(append([]byte{bucketTable}, TestBktID[:]...), TestBktData, nil),
		"could not add bucket to test store",
	)

	// Add bucket values to the store.
	key := make([]byte, 1+BucketIDLength+2)
	key[0] = valueTable
	copy(key[1:17], TestBktID[:])
	for _, val := range ExpectedBktValues {
		binary.BigEndian.PutUint16(key[17:], val.Idx)
		require.NoError(
			t,
			batch.Set(key, val.Value, nil),
			"could not add bucket value to test store",
		)
	}

	require.NoError(t, db.Apply(batch, nil), "could not commit batch to test store")
	return str
}

func TestGetBucket(t *testing.T) {
	str := SetupTestStore(t, true)
	defer str.Close()

	// Fetch bucket.
	bkt, err := str.GetBucket(TestBktID)
	assert.NoError(t, err, "error occurred while fetching bucket")
	assert.Equal(t, TestBktID, bkt.(*pebbleBucket).id, "fetched bucket has incorrect ID")
	assert.Equal(t, TestBktData, bkt.(*pebbleBucket).data[:], "fetched bucket has incorrect bucket data")
	assert.Equal(t, uint16(len(ExpectedBktValues)), bkt.(*pebbleBucket).lastIdx, "fetched bucket has incorrect lastIdx")
	assert.Same(t, str, bkt.(*pebbleBucket).store, "fetched bucket does not belong to the right store")

	// Test whether the cache is working correctly.
	bkt2, err := str.GetBucket(TestBktID)
	assert.NoError(t, err, "error occurred while fetching bucket for the second time")
	assert.Same(t, bkt, bkt2, "bucket cache is not working correctly, second fetch returned a new instance")

	// Test whether error is returned when bucket is not found.
	_, err = str.GetBucket(BucketID(make([]byte, 16)))
	assert.Equal(t, err, ErrBucketNotFound, "bucket not found but no error / invalid error returned")
}

func TestCreateBucket(t *testing.T) {
	str := SetupTestStore(t, false)
	defer str.Close()

	// Create bucket.
	bkt, err := str.CreateBucket(TestBktID, TestBktKey)
	assert.NoError(t, err, "error occurred while creating bucket")
	assert.Equal(t, TestBktID, bkt.GetBucketID(), "created bucket has incorrect ID")
	assert.Equal(t, getCurrentTimestamp(), getTimestamp(bkt.(*pebbleBucket)), "created bucket has incorrect timestamp")
	assert.Equal(t, TestBktKey, bkt.GetBucketKey(), "created bucket has incorrect bucket key")
	assert.Equal(t, uint16(0), bkt.(*pebbleBucket).lastIdx, "created bucket has incorrect lastIdx")
	assert.Same(t, str, bkt.(*pebbleBucket).store, "created bucket does not belong to the right store")

	// Test whether bucket can be fetched and is cached.
	bkt2, err := str.GetBucket(TestBktID)
	assert.NoError(t, err, "error occurred while fetching created bucket from cache")
	assert.Same(t, bkt, bkt2, "bucket cache is not working correctly, fetching the created bucket returned a new instance")

	// Test whether bucket is persisted to the underlying pebble store.
	str.(*pebbleStore).cache.Delete(TestBktID) // Remove bucket from cache.
	fetchedBucket, _ := str.GetBucket(TestBktID)
	assert.Equal(t, bkt, fetchedBucket, "error occurred while fetching created bucket without cache")

	// Test whether error is returned when bucket is not found.
	_, err = str.CreateBucket(TestBktID, TestBktKey)
	assert.Equal(t, err, ErrBucketAlreadyExists, "bucket already exists but no error returned")
}

func TestDeleteBucket(t *testing.T) {
	str := SetupTestStore(t, true)
	defer str.Close()

	// Fetch bucket.
	bkt, err := str.GetBucket(TestBktID)
	assert.NoError(t, err, "error occurred while fetching bucket")

	// Delete bucket.
	err = str.DeleteBucket(bkt)
	assert.NoError(t, err, "error occurred while deleting bucket")

	// Test whether bucket is deleted.
	_, err = str.GetBucket(TestBktID)
	assert.Equal(t, err, ErrBucketNotFound, "deleted bucket could still be found")

	// Test whether the bucket values are deleted.
	values, err := bkt.GetValues(BucketRange{Start: 0, End: math.MaxUint16})
	assert.NoError(t, err, "error occurred while fetching bucket values of a deleted bucket")
	assert.Empty(t, values, "bucket values of deleted bucket still exist")
}

func TestGC(t *testing.T) {
	str := SetupTestStore(t, true)
	defer str.Close()
	_, _ = str.GetBucket(TestBktID) // Load bucket into the cache.

	// Run first GC, testBucket has a timestamp of 0
	// so should be deleted from cache and backend store.
	assert.NoError(t, str.GC())
	_, ok := str.(*pebbleStore).cache.Load(TestBktID)
	_, err := str.GetBucket(TestBktID)

	// Test whether bucket is deleted.
	assert.False(t, ok, "bucket is not garbage collected from cache while expired")
	assert.Equal(t, ErrBucketNotFound, err, "bucket is not garbage collected from store while expired")

	// Now recreate bucket with a valid timestamp.
	_, err = str.CreateBucket(TestBktID, BucketKey(TestBktData))
	assert.NoError(t, err, "error occurred while creating bucket")
	assert.NoError(t, str.GC())

	// Test whether bucket is still in the cache and backend store.
	_, ok = str.(*pebbleStore).cache.Load(TestBktID)
	str.(*pebbleStore).cache.Delete(TestBktID) // Remove bucket from cache.
	_, err = str.GetBucket(TestBktID)
	assert.True(t, ok, "bucket is garbage collected from cache while not expired")
	assert.NoError(t, err, "bucket is garbage collected from store while not expired")
}
