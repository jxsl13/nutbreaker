package nutbreaker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzBoundary(f *testing.F) {
	for _, r := range ranges {
		f.Add(r.Range)
	}

	f.Add("255.255.255.0 - 255.255.255.255")

	f.Fuzz(func(t *testing.T, ipRange string) {
		require.NotPanics(t, func() {
			lo, hi, err := parseRange(ipRange, []byte("val"))
			if err != nil {
				return
			}
			loDB, err := newBoundaryFloat64(lo.Score, lo.LowerBound, lo.UpperBound, lo.Value)
			require.NoError(t, err)
			require.Equal(t, lo.String(), loDB.String())

			hiDB, err := newBoundaryFloat64(hi.Score, hi.LowerBound, hi.UpperBound, hi.Value)
			require.NoError(t, err)
			require.Equal(t, hi.String(), hiDB.String())

			la := lo.Above()
			laDB, err := newBoundaryFloat64(la.Score, la.LowerBound, la.UpperBound, la.Value)
			require.NoError(t, err)
			require.Equal(t, la.String(), laDB.String())

			lb := lo.Below()
			lbDB, err := newBoundaryFloat64(lb.Score, lb.LowerBound, lb.UpperBound, lb.Value)
			require.NoError(t, err)
			require.Equal(t, lb.String(), lbDB.String())

			ha := hi.Above()
			haDB, err := newBoundaryFloat64(ha.Score, ha.LowerBound, ha.UpperBound, ha.Value)
			require.NoError(t, err)
			require.Equal(t, ha.String(), haDB.String())

			hb := hi.Below()
			hbDB, err := newBoundaryFloat64(hb.Score, hb.LowerBound, hb.UpperBound, hb.Value)
			require.NoError(t, err)
			require.Equal(t, hb.String(), hbDB.String())

		})

	})
}
