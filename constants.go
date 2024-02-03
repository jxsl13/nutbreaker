package nutbreaker

import (
	"errors"
	"math"
	"net/netip"
	"regexp"
)

var (
	negInf = math.Inf(-1)
	posInf = math.Inf(1)

	negInfKey = []byte("-inf")
	posInfKey = []byte("+inf")

	negInfValue = negInfKey
	posInfValue = posInfKey

	negInfBoundary = boundary{
		Key:        negInfKey,
		IP:         netip.Addr{},
		Score:      negInf,
		UpperBound: true,
		Value:      negInfValue,
	}

	posInfBoundary = boundary{
		Key:        posInfKey,
		IP:         netip.Addr{},
		Score:      posInf,
		LowerBound: true,
		Value:      posInfValue,
	}
)

var (
	customIPRangeRegex = regexp.MustCompile(`([0-9a-f:.]{7,41})\s*-\s*([0-9a-f:.]{7,41})`)
)

var (
	// ErrIPv6NotSupported is returned if an IPv6 range or IP input is detected.
	ErrIPv6NotSupported = errors.New("IPv6 ranges are not supported")

	// ErrInvalidRange is returned when a passed string is not a valid range
	ErrInvalidRange = errors.New("invalid range passed, use either of these: <IP>, <IP>/<1-32>, <IP> - <IP>")

	// ErrIPNotFound is returned if the passed IP is not contained in any ranges
	ErrIPNotFound = errors.New("the given IP was not found in any database ranges")
)
