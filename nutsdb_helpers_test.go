package nutbreaker

import (
	"fmt"
	"os"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/stretchr/testify/require"
)

func initNutsDB(t *testing.T, withInfBoundaries bool) (db *nutsdb.DB, bucketName string, sortedSetKey []byte, cleanup func()) {

	require := require.New(t)
	dir := generateRandomDbDirName()
	var err error
	db, err = nutsdb.Open(nutsdb.DefaultOptions,
		nutsdb.WithSegmentSize(1024),
		nutsdb.WithDir(dir),
	)
	require.NoError(err)

	bucketName = "blacklist"
	sortedSetKey = []byte("sortedSetKey")

	err = db.Update(func(tx *nutsdb.Tx) error {
		err = tx.NewSortSetBucket(bucketName)
		if err != nil {
			return err
		}
		err = tx.NewKVBucket(bucketName)
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(err)

	if withInfBoundaries {
		err = db.Update(func(tx *nutsdb.Tx) error {
			err = negInfBoundary.InsertInf(tx, bucketName, sortedSetKey)
			if err != nil {
				return err
			}
			return posInfBoundary.InsertInf(tx, bucketName, sortedSetKey)
		})
		require.NoError(err)
	}

	return db, bucketName, sortedSetKey, func() {
		require.NoError(db.Close())
		require.NoError(os.RemoveAll(dir))
	}
}

func getBoundary(t *testing.T, tx *nutsdb.Tx, bucket string, zKey []byte, score float64) boundary {
	require := require.New(t)
	ms, err := tx.ZRangeByScore(bucket, zKey, score, score, &nutsdb.GetByScoreRangeOptions{
		ExcludeStart: false,
		ExcludeEnd:   false,
		Limit:        1,
	})
	require.NoError(err)
	require.Len(ms, 1)

	b, err := newBoundaryFromDB(tx, bucket, ms[0])
	require.NoError(err)
	return b
}

func getAllBoundaries(t *testing.T, tx *nutsdb.Tx, bucket string, zKey []byte, withStartEnd bool) []boundary {
	opts := &nutsdb.GetByScoreRangeOptions{
		ExcludeStart: !withStartEnd,
		ExcludeEnd:   !withStartEnd,
	}
	require := require.New(t)
	ms, err := tx.ZRangeByScore(
		bucket,
		zKey,
		negInf,
		posInf,
		opts,
	)
	require.NoError(err)

	var b []boundary
	for _, m := range ms {
		bb, err := newBoundaryFromDB(tx, bucket, m)
		require.NoError(err)
		b = append(b, bb)
	}
	return b
}

func insertRanges(t *testing.T, tx *nutsdb.Tx, sameValue bool, bucket string, zKey []byte, ipRanges ...string) []boundary {
	require := require.New(t)
	var b []boundary
	var value []byte = []byte("same value")
	for idx, r := range ipRanges {
		if !sameValue {
			value = []byte(fmt.Sprintf("value %d", idx))
		}

		lo, hi, err := parseRange(r, value)
		require.NoError(err, "parseRange() error = %v, wantErr %v", err, true)

		if lo.Equal(hi) {
			b = append(b, lo)
			require.NoError(lo.Insert(tx, bucket, zKey))
		} else {
			b = append(b, lo, hi)
			require.NoError(lo.Insert(tx, bucket, zKey))
			require.NoError(hi.Insert(tx, bucket, zKey))
		}

		require.NoError(err, "Insert() error = %v, wantErr %v", err, true)
	}
	return b
}
