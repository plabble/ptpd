package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBucketLifetime(t *testing.T) {
	assert.Equal(t, byte(255), GetBucketLifetime(TestBktID), "lifetime is not parsed correctly")
}

func TestGetBucketPermissions(t *testing.T) {
	tests := []struct {
		name              string
		id                BucketID
		expectedPublic    BucketPermissions
		expectedProtected BucketPermissions
	}{
		{
			name: "protected read, protected write, protected append",
			id:   BucketID([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 56}),
			expectedPublic: BucketPermissions{
				Read:   false,
				Write:  false,
				Append: false,
			},
			expectedProtected: BucketPermissions{
				Read:   true,
				Write:  true,
				Append: true,
			},
		},
		{
			name: "public read, public write, public append",
			id:   BucketID([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 7}),
			expectedPublic: BucketPermissions{
				Read:   true,
				Write:  true,
				Append: true,
			},
			expectedProtected: BucketPermissions{
				Read:   true,
				Write:  true,
				Append: true,
			},
		},
		{
			name: "public read, protected write, protected append",
			id:   BucketID([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 49}),
			expectedPublic: BucketPermissions{
				Read:   true,
				Write:  false,
				Append: false,
			},
			expectedProtected: BucketPermissions{
				Read:   true,
				Write:  true,
				Append: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedPublic, GetBucketPermissions(test.id, false), "permissions are not parsed correctly for public access")
			assert.Equal(t, test.expectedProtected, GetBucketPermissions(test.id, true), "permissions are not parsed correctly for protected access")
		})
	}
}

func TestGetValues(t *testing.T) {
	str := SetupTestStore(t, true)
	defer str.Close()
	bkt, err := str.GetBucket(TestBktID)
	require.NoError(t, err, "error occurred while fetching bucket")

	// Fetch values.
	values, err := bkt.GetValues(BucketRange{Start: 0, End: 500})
	assert.NoError(t, err, "error occurred while fetching bucket values")
	assert.Len(t, values, len(ExpectedBktValues), "fetched bucket values have incorrect length")
	assert.Equal(t, ExpectedBktValues, values, "fetched bucket values are incorrect")
}

func TestPutValues(t *testing.T) {
	str := SetupTestStore(t, false)
	defer str.Close()
	bkt, err := str.CreateBucket(TestBktID, TestBktKey)
	require.NoError(t, err, "error occurred while creating bucket")

	// Insert new values.
	err = bkt.PutValues(TestBktValues)
	assert.NoError(t, err, "error occurred while putting values")
	assert.Equal(t, uint16(len(ExpectedBktValues)), bkt.(*pebbleBucket).lastIdx, "lastIdx is not updated correctly")

	// Fetch new values.
	values, err := bkt.GetValues(BucketRange{Start: 0, End: 500})
	assert.NoError(t, err, "error occurred while fetching bucket values")
	assert.Len(t, values, len(ExpectedBktValues), "fetched bucket values have incorrect length")
	assert.Equal(t, ExpectedBktValues, values, "fetched bucket values are incorrect")
}

func TestAppendValues(t *testing.T) {
	str := SetupTestStore(t, false)
	defer str.Close()
	bkt, err := str.CreateBucket(TestBktID, TestBktKey)
	require.NoError(t, err, "error occurred while creating bucket")

	// Append new values.
	err = bkt.AppendValues(TestBktValues)
	assert.NoError(t, err, "error occurred while appending values")
	assert.Equal(t, uint16(len(ExpectedBktValues)), bkt.(*pebbleBucket).lastIdx, "lastIdx is not updated correctly")

	// Fetch new values.
	values, err := bkt.GetValues(BucketRange{Start: 0, End: 500})
	assert.NoError(t, err, "error occurred while fetching bucket values")
	assert.Len(t, values, len(ExpectedBktValues), "fetched bucket values have incorrect length")
	assert.Equal(t, ExpectedBktValues, values, "fetched bucket values are incorrect")

	// Test whether check for invalid idx is working.
	err = bkt.AppendValues([]BucketValue{{Idx: 5, Value: []byte("test")}})
	assert.Equal(t, ErrInvalidAppend, err, "no error returned while doing an invalid append")
}

func TestDeleteValues(t *testing.T) {
	str := SetupTestStore(t, true)
	defer str.Close()
	bkt, err := str.GetBucket(TestBktID)
	require.NoError(t, err, "error occurred while fetching bucket")

	err = bkt.DeleteValues(BucketRange{Start: 0, End: 500})
	assert.NoError(t, err, "error occurred while deleting values")
	assert.Equal(t, uint16(0), bkt.(*pebbleBucket).lastIdx, "lastIdx is not reset while deleting values")

	// Test whether values are deleted.
	values, err := bkt.GetValues(BucketRange{Start: 0, End: 500})
	assert.NoError(t, err, "error occurred while fetching bucket values")
	assert.Len(t, values, 0, "bucket values are not deleted")
}
