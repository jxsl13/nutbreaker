package nutbreaker

import (
	"fmt"

	"net/netip"

	"github.com/xgfone/go-netaddr"
)

func parseRange(r string, value []byte) (low, high boundary, err error) {
	ip, err := netip.ParseAddr(r)
	if err == nil {
		if ip.Is6() {
			return empty, empty, ErrIPv6NotSupported
		}

		r, err := newBoundary(ip, true, true, value)
		if err != nil {
			return empty, empty, err
		}
		return r, r, nil
	}
	// parsing as IP failed

	net, err := netaddr.NewIPNetwork(r)
	if err == nil {
		fip := net.First().IP()
		first, ok := netip.AddrFromSlice(fip)
		if !ok {
			return empty, empty, fmt.Errorf("%w: first ip: %s: %w", ErrInvalidRange, fip, err)
		}
		lip := net.Last().IP()
		last, ok := netip.AddrFromSlice(lip)
		if !ok {
			return empty, empty, fmt.Errorf("%w: last ip: %s: %w", ErrInvalidRange, lip, err)
		}

		low, err = newBoundary(first, true, false, value)
		if err != nil {
			return empty, empty, err
		}

		high, err = newBoundary(last, false, true, value)
		if err != nil {
			return empty, empty, err
		}
		return low, high, nil

	}
	// parsing as cidr failed x.x.x.x/24

	if matches := customIPRangeRegex.FindStringSubmatch(r); len(matches) == 3 {
		lowerBound := matches[1]
		upperBound := matches[2]

		lowIP, err := netip.ParseAddr(lowerBound)
		if err != nil {
			return empty, empty, fmt.Errorf("%w: %w", ErrInvalidRange, err)
		}
		highIP, err := netip.ParseAddr(upperBound)
		if err != nil {
			return empty, empty, fmt.Errorf("%w: %w", ErrInvalidRange, err)
		}

		if lowIP.Compare(highIP) > 0 {
			return empty, empty, fmt.Errorf("%w: first ip must be smaller than the second", ErrInvalidRange)
		}

		low, err = newBoundary(lowIP, true, false, value)
		if err != nil {
			return empty, empty, err
		}
		high, err = newBoundary(highIP, false, true, value)
		if err != nil {
			return empty, empty, err
		}
		return low, high, nil
	}
	return empty, empty, ErrInvalidRange
}
