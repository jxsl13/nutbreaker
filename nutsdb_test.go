package nutbreaker

import (
	"math"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/stretchr/testify/require"
)

func TestInsertInf(t *testing.T) {
	db, bucketName, key, cleanup := initNutsDB(t)
	defer cleanup()

	nInf := math.Inf(-1)
	pInf := math.Inf(1)

	err := db.Update(func(tx *nutsdb.Tx) error {
		err := tx.ZAdd(bucketName, key, nInf, []byte("-Inf"))
		if err != nil {
			return err
		}
		return tx.ZAdd(bucketName, key, pInf, []byte("+Inf"))
	})
	require.NoError(t, err)

	err = db.View(func(tx *nutsdb.Tx) error {
		members, err := tx.ZRangeByScore(
			bucketName,
			key,
			nInf,
			pInf,
			&nutsdb.GetByScoreRangeOptions{
				ExcludeStart: false,
				ExcludeEnd:   false,
				Limit:        0,
			})
		if err != nil {
			return err
		}
		require.Len(t, members, 2)
		return err
	})
	require.NoError(t, err)

	err = db.Update(func(tx *nutsdb.Tx) error {
		members, err := tx.ZRangeByScore(
			bucketName,
			key,
			nInf,
			pInf,
			&nutsdb.GetByScoreRangeOptions{
				ExcludeStart: false,
				ExcludeEnd:   false,
				Limit:        0,
			})
		if err != nil {
			return err
		}
		require.Len(t, members, 2)
		return err
	})
	require.NoError(t, err)

}

func TestInsertNonOverlappingBoundaries(t *testing.T) {
	db, bucket, key, cleanup := initNutsDB(t)
	defer cleanup()

	low1, high1, err := parseRange(
		"123.0.0.1 - 123.0.0.4",
		[]byte("value"),
	)
	require.NoError(t, err)

	low2, high2, err := parseRange(
		"123.0.0.6 - 123.0.0.8",
		[]byte("value"),
	)

	err = db.Update(func(tx *nutsdb.Tx) error {
		err = low1.Insert(tx, bucket, key)
		if err != nil {
			return err
		}
		err = high1.Insert(tx, bucket, key)
		if err != nil {
			return err
		}

		err = low2.Insert(tx, bucket, key)
		if err != nil {
			return err
		}

		err = high2.Insert(tx, bucket, key)
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)

	var lb1, hb1, lb2, hb2 boundary
	err = db.View(func(tx *nutsdb.Tx) error {
		lb1 = getBoundary(t, tx, bucket, key, low1.Score)
		hb1 = getBoundary(t, tx, bucket, key, high1.Score)
		lb2 = getBoundary(t, tx, bucket, key, low2.Score)
		hb2 = getBoundary(t, tx, bucket, key, high2.Score)
		return nil
	})

	require.NoError(t, err)

	require.Equal(t, low1.String(), lb1.String())  // 123.0.0.1
	require.Equal(t, high1.String(), hb1.String()) // 123.0.0.4

	require.Equal(t, low2.String(), lb2.String())  // 123.0.0.6
	require.Equal(t, high2.String(), hb2.String()) // 123.0.0.8

}
