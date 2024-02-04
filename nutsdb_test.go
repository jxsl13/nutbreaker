package nutbreaker

import (
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/stretchr/testify/require"
)

func TestInsertInf(t *testing.T) {
	db, bucketName, key, cleanup := initNutsDB(t, false)
	defer cleanup()

	err := db.Update(func(tx *nutsdb.Tx) error {
		err := tx.ZAdd(bucketName, key, negInf, []byte("-Inf"))
		if err != nil {
			return err
		}
		return tx.ZAdd(bucketName, key, posInf, []byte("+Inf"))
	})
	require.NoError(t, err)

	err = db.View(func(tx *nutsdb.Tx) error {
		members, err := tx.ZRangeByScore(
			bucketName,
			key,
			negInf,
			posInf,
			&nutsdb.GetByScoreRangeOptions{})
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
			negInf,
			posInf,
			&nutsdb.GetByScoreRangeOptions{},
		)
		if err != nil {
			return err
		}
		require.Len(t, members, 2)
		return err
	})
	require.NoError(t, err)

	require.NoError(t, err)
}

func TestInsertNonOverlappingBoundaries(t *testing.T) {
	db, bucket, key, cleanup := initNutsDB(t, true)
	defer cleanup()

	var inserted []boundary
	var lb1, hb1, lb2, hb2 boundary

	err := db.Update(func(tx *nutsdb.Tx) error {
		inserted = insertRanges(
			t,
			tx,
			true,
			bucket,
			key,
			"123.0.0.1 - 123.0.0.4", // 0, 1
			"123.0.0.6 - 123.0.0.8", // 2, 3
		)
		return nil
	})
	require.NoError(t, err)

	var (
		l1 = inserted[0]
		h1 = inserted[1]
		l2 = inserted[2]
		h2 = inserted[3]
	)
	err = db.View(func(tx *nutsdb.Tx) error {
		lb1 = getBoundary(t, tx, bucket, key, l1.Score)
		hb1 = getBoundary(t, tx, bucket, key, h1.Score)
		lb2 = getBoundary(t, tx, bucket, key, l2.Score)
		hb2 = getBoundary(t, tx, bucket, key, h2.Score)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, l1.String(), lb1.String()) // 123.0.0.1
	require.Equal(t, h1.String(), hb1.String()) // 123.0.0.4

	require.Equal(t, l2.String(), lb2.String()) // 123.0.0.6
	require.Equal(t, h2.String(), hb2.String()) // 123.0.0.8

	err = db.View(func(tx *nutsdb.Tx) error {
		actual := getAllBoundaries(t, tx, bucket, key, true)
		expected := []boundary{negInfBoundary, l1, h1, l2, h2, posInfBoundary}
		equalBoundaries(t, expected, actual, "all boundaries not equal")
		return nil
	})
	require.NoError(t, err)
}

func TestInsertOverlappingBoundariesLB(t *testing.T) {
	db, bucket, key, cleanup := initNutsDB(t, true)
	defer cleanup()

	var inserted []boundary
	err := db.Update(func(tx *nutsdb.Tx) error {
		inserted = insertRanges(
			t,
			tx,
			true,
			bucket,
			key,
			"123.0.0.2 - 123.0.0.6", // 0, 1
			"123.0.0.0 - 123.0.0.4", // 2, 3
		)
		return nil
	})
	require.NoError(t, err)

	var (
		l = inserted[2]
		h = inserted[1]
	)

	var lb1, hb1 boundary
	err = db.View(func(tx *nutsdb.Tx) error {
		lb1 = getBoundary(t, tx, bucket, key, l.Score)
		hb1 = getBoundary(t, tx, bucket, key, h.Score)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, l.String(), lb1.String()) // 123.0.0.0
	require.Equal(t, h.String(), hb1.String()) // 123.0.0.6
}

func TestInsertOverlappingBoundariesUB(t *testing.T) {
	db, bucket, key, cleanup := initNutsDB(t, true)
	defer cleanup()

	var inserted []boundary
	err := db.Update(func(tx *nutsdb.Tx) error {
		inserted = insertRanges(
			t,
			tx,
			true,
			bucket,
			key,
			"123.0.0.0 - 123.0.0.4", // 0, 1
			"123.0.0.2 - 123.0.0.6", // 2, 3
		)
		return nil
	})
	require.NoError(t, err)

	var (
		l = inserted[0]
		h = inserted[3]
	)

	var lb1, hb1 boundary
	err = db.View(func(tx *nutsdb.Tx) error {
		lb1 = getBoundary(t, tx, bucket, key, l.Score)
		hb1 = getBoundary(t, tx, bucket, key, h.Score)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, l.String(), lb1.String()) // 123.0.0.0
	require.Equal(t, h.String(), hb1.String()) // 123.0.0.6
}

func TestInsertCloseOverlappingBoundariesLB(t *testing.T) {
	db, bucket, key, cleanup := initNutsDB(t, true)
	defer cleanup()

	var inserted []boundary
	err := db.Update(func(tx *nutsdb.Tx) error {
		inserted = insertRanges(
			t,
			tx,
			true,
			bucket,
			key,
			"123.0.0.1 - 123.0.0.3", // 0, 1
			"123.0.0.0 - 123.0.0.2", // 2, 3
		)
		return nil
	})
	require.NoError(t, err)

	var (
		l = inserted[2]
		h = inserted[1]
	)

	var lb1, hb1 boundary
	err = db.View(func(tx *nutsdb.Tx) error {
		lb1 = getBoundary(t, tx, bucket, key, l.Score)
		hb1 = getBoundary(t, tx, bucket, key, h.Score)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, l.String(), lb1.String()) // 123.0.0.0
	require.Equal(t, h.String(), hb1.String()) // 123.0.0.6
}

func TestInsertCloseOverlappingBoundariesUB(t *testing.T) {
	db, bucket, key, cleanup := initNutsDB(t, true)
	defer cleanup()

	var inserted []boundary
	err := db.Update(func(tx *nutsdb.Tx) error {
		inserted = insertRanges(
			t,
			tx,
			true,
			bucket,
			key,
			"123.0.0.0 - 123.0.0.2", // 0, 1
			"123.0.0.1 - 123.0.0.3", // 2, 3
		)
		return nil
	})
	require.NoError(t, err)

	var (
		l = inserted[0]
		h = inserted[3]
	)

	var lb1, hb1 boundary
	err = db.View(func(tx *nutsdb.Tx) error {
		lb1 = getBoundary(t, tx, bucket, key, l.Score)
		hb1 = getBoundary(t, tx, bucket, key, h.Score)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, l.String(), lb1.String()) // 123.0.0.0
	require.Equal(t, h.String(), hb1.String()) // 123.0.0.6
}
