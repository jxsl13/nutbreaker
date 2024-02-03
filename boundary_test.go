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

			lo.Above()
			lo.Below()
			hi.Above()
			hi.Below()
		})

	})
}
