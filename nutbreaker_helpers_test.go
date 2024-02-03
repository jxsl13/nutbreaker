package nutbreaker

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/stretchr/testify/require"
)

func generateRandomDbDirName() string {
	pwd, err := os.Getwd()
	if err != nil {
		pwd = "./"
	}
	return filepath.Join(pwd, "testdata", strconv.FormatInt(rand.Int63(), 10))
}

func vicinity(t *testing.T, ndb *NutBreaker, ipRanges string, num ...int) (below, inside, above []boundary) {
	n := 1
	if len(num) > 0 {
		n = num[0]
	}
	require := require.New(t)

	low, high, err := parseRange(ipRanges, []byte("vicinity value"))
	require.NoError(err, "parseRange() error = %v, wantErr %v", err, true)

	err = ndb.db.View(func(tx *nutsdb.Tx) error {
		b, i, a, err := ndb.vicinity(tx, low, high, n)
		require.NoError(err, "vicinity() error = %v, wantErr %v", err, true)
		below = append(below, b...)
		inside = append(inside, i...)
		above = append(above, a...)
		return nil
	})
	require.NoError(err, "ndb.db.View() error")
	return below, inside, above
}

func consistent(t *testing.T, ndb *NutBreaker) {
	require := require.New(t)
	err := ndb.isConsistent()
	require.NoError(err, "ndb.isConsistent() error: Database INCONSISTENT")
}

func insert(t *testing.T, ndb *NutBreaker, sameValue bool, ipRanges ...string) (inserted []boundary) {

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
		} else {
			b = append(b, lo, hi)
		}

		err = ndb.Insert(r, value)
		require.NoError(err, "Insert() error = %v, wantErr %v", err, true)
	}
	return b
}

func equal(t *testing.T, ndb *NutBreaker, expected ...boundary) {
	require := require.New(t)
	expectedStr := make([]string, 0, len(expected))
	for _, e := range expected {
		expectedStr = append(expectedStr, e.String())
	}

	actual, err := ndb.getAll()
	require.NoError(err, "ndb.getAll() error")

	actualStr := make([]string, 0, len(actual))
	for _, a := range actual {
		actualStr = append(actualStr, a.String())
	}

	require.Equal(expectedStr, actualStr, "equal() error")
}

func equalBoundaries(t *testing.T, expected, got []boundary, err string) {
	require := require.New(t)
	expectedStr := make([]string, 0, len(expected))
	for _, e := range expected {
		expectedStr = append(expectedStr, e.String())
	}

	actualStr := make([]string, 0, len(got))
	for _, a := range got {
		actualStr = append(actualStr, a.String())
	}

	require.Equalf(expectedStr, actualStr, "equal() error: %s", err)
}
