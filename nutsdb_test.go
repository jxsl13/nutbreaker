package nutbreaker

import (
	"math"
	"os"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/stretchr/testify/require"
)

func TestInsertInf(t *testing.T) {
	dir := generateRandomDbDirName()
	defer os.RemoveAll(dir)

	db, err := nutsdb.Open(nutsdb.DefaultOptions,
		nutsdb.WithSegmentSize(1024),
		nutsdb.WithDir(dir),
	)
	if err != nil {
		require.NoError(t, err)
	}
	defer db.Close()

	sortedSetBucketName := "sortedSet"
	sortedSetKey := []byte("sortedSetKey")

	err = db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewSortSetBucket(sortedSetBucketName)
	})
	require.NoError(t, err)

	nInf := math.Inf(-1)
	pInf := math.Inf(1)

	err = db.Update(func(tx *nutsdb.Tx) error {
		err = tx.ZAdd(sortedSetBucketName, sortedSetKey, nInf, []byte("-Inf"))
		if err != nil {
			return err
		}
		return tx.ZAdd(sortedSetBucketName, sortedSetKey, pInf, []byte("+Inf"))
	})
	require.NoError(t, err)

	err = db.View(func(tx *nutsdb.Tx) error {
		members, err := tx.ZRangeByScore(
			sortedSetBucketName,
			sortedSetKey,
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
			sortedSetBucketName,
			sortedSetKey,
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
