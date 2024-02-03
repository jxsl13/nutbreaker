package nutbreaker

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"

	"github.com/nutsdb/nutsdb"
)

type NutBreaker struct {
	db                    *nutsdb.DB
	dataDir               string
	blacklistBucket       string
	blacklistSortedSetKey []byte
	whitelistBucket       string
}

func NewNutBreaker(opts ...Option) (nb *NutBreaker, err error) {
	dir, err := os.Getwd()
	if err != nil {
		dir = "./nutsdata"
	} else {
		dir = filepath.Join(dir, "nutsdata")
	}

	opt := options{
		dataDir:               dir,
		blacklistBucket:       "blacklist",
		blacklistSortedSetKey: "blacklist-zkey",
		whitelistBucket:       "whitelist",
	}

	for _, o := range opts {
		if err := o(&opt); err != nil {
			return nil, err
		}
	}

	db, err := nutsdb.Open(nutsdb.DefaultOptions,
		nutsdb.WithSegmentSize(1024*1024),
		nutsdb.WithDir(opt.dataDir),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open nutsdb: %v", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(db.Close())
		}
	}()

	nb = &NutBreaker{
		db:                    db,
		dataDir:               opt.dataDir,
		blacklistBucket:       opt.blacklistBucket,
		whitelistBucket:       opt.whitelistBucket,
		blacklistSortedSetKey: []byte(opt.blacklistSortedSetKey),
	}

	// init database
	err = nb.db.Update(nb.initBuckets)
	if err != nil {
		return nil, err
	}
	err = nb.db.Update(nb.init)
	if err != nil {
		return nil, err
	}

	return nb, nil
}

func (n *NutBreaker) DataDir() string {
	return n.dataDir
}

func (n *NutBreaker) initBuckets(tx *nutsdb.Tx) (err error) {
	if !tx.ExistBucket(nutsdb.DataStructureBTree, n.blacklistBucket) {
		err = tx.NewKVBucket(n.blacklistBucket)
		if err != nil {
			return fmt.Errorf("failed to create blacklist kv bucket: %v", err)
		}
	}

	if !tx.ExistBucket(nutsdb.DataStructureSortedSet, n.blacklistBucket) {
		err = tx.NewSortSetBucket(n.blacklistBucket)
		if err != nil {
			return fmt.Errorf("failed to create blacklist sorted set bucket: %v", err)
		}
	}

	if !tx.ExistBucket(nutsdb.DataStructureBTree, n.whitelistBucket) {
		err = tx.NewKVBucket(n.whitelistBucket)
		if err != nil {
			return fmt.Errorf("failed to create whitelist kv bucket: %v", err)
		}
	}

	return nil
}

func (n *NutBreaker) init(tx *nutsdb.Tx) (err error) {

	err = negInfBoundary.InsertInf(tx,
		n.blacklistBucket,
		n.blacklistSortedSetKey,
	)
	if err != nil {
		return fmt.Errorf("failed to insert negInfBoundary: %v", err)
	}

	err = posInfBoundary.InsertInf(tx,
		n.blacklistBucket,
		n.blacklistSortedSetKey,
	)
	if err != nil {
		return fmt.Errorf("failed to insert posInfBoundary: %v", err)
	}
	return nil
}

func (n *NutBreaker) Close() error {
	return n.db.Close()
}

func (n *NutBreaker) flush(tx *nutsdb.Tx) (err error) {

	if tx.ExistBucket(
		nutsdb.DataStructureBTree,
		n.blacklistBucket,
	) {
		err = tx.DeleteBucket(nutsdb.DataStructureBTree, n.blacklistBucket)
		if err != nil {
			return fmt.Errorf("failed to delete blacklist kv bucket: %v", err)
		}
	}

	if tx.ExistBucket(nutsdb.DataStructureBTree, n.whitelistBucket) {
		err = tx.DeleteBucket(nutsdb.DataStructureBTree, n.whitelistBucket)
		if err != nil {
			return fmt.Errorf("failed to create whitelist bucket: %v", err)
		}
	}
	return nil
}

func (n *NutBreaker) Flush() error {
	return n.db.Update(n.flush)
}

func (n *NutBreaker) Reset() error {
	err := n.db.Update(n.flush)
	if err != nil {
		return err
	}

	err = n.db.Update(n.initBuckets)
	if err != nil {
		return err
	}

	err = n.db.Update(n.init)
	if err != nil {
		return err
	}

	return nil
}

func (n *NutBreaker) getAll() ([]boundary, error) {
	result := make([]boundary, 0, 3)
	err := n.db.View(func(tx *nutsdb.Tx) error {
		inside, err := n.all(tx)
		if err != nil {
			return err
		}

		result = append(result, inside...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (n *NutBreaker) all(tx *nutsdb.Tx) (inside []boundary, err error) {
	members, err := tx.ZRangeByScore(
		n.blacklistBucket,
		n.blacklistSortedSetKey,
		negInf,
		posInf,
		&nutsdb.GetByScoreRangeOptions{
			ExcludeStart: false,
			ExcludeEnd:   false,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get all: %v", err)
	}

	inside = make([]boundary, 0, len(members))
	var b boundary
	for _, m := range members {
		b, err = newBoundaryFromDB(m.Score, m.Value)
		if err != nil {
			return nil, err
		}
		inside = append(inside, b)
	}
	return inside, nil
}

func (n *NutBreaker) vicinity(tx *nutsdb.Tx, low, high boundary, num int) (below, inside, above []boundary, err error) {
	if num < 0 {
		panic(fmt.Sprintf("passed num parameter must be >= 0, got %d", num))
	}
	defer func() {
		if err != nil {
			if low.Equal(high) {
				err = fmt.Errorf("failed to get vicinity of %s: %v", low, err)
			} else {
				err = fmt.Errorf("failed to get vicinity of %s - %s: %v", low, high, err)
			}
		}
	}()

	membersBelow, err := tx.ZRangeByScore(
		n.blacklistBucket,
		n.blacklistSortedSetKey,
		low.Below().Score, //reverse order of scores in order to get nearest below
		negInf,
		&nutsdb.GetByScoreRangeOptions{
			ExcludeStart: false,
			ExcludeEnd:   false,
			Limit:        num,
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	membersInside, err := tx.ZRangeByScore(
		n.blacklistBucket,
		n.blacklistSortedSetKey,
		low.Score,
		high.Score,
		&nutsdb.GetByScoreRangeOptions{
			ExcludeStart: false,
			ExcludeEnd:   false,
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	membersAbove, err := tx.ZRangeByScore(
		n.blacklistBucket,
		n.blacklistSortedSetKey,
		high.Above().Score,
		posInf,
		&nutsdb.GetByScoreRangeOptions{
			ExcludeStart: false,
			ExcludeEnd:   false,
			Limit:        num,
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	var b boundary
	below = make([]boundary, 0, len(membersBelow))
	inside = make([]boundary, 0, len(membersInside))
	above = make([]boundary, 0, len(membersAbove))

	// create below IPs
	for _, m := range membersBelow {
		b, err = newBoundaryFromDB(m.Score, m.Value)
		if err != nil {
			return nil, nil, nil, err
		}
		below = append(below, b)
	}

	// should be faster than prepending values to a slice
	if len(below) > 1 {
		sort.Sort(byIP(below))
	}

	// create inside IPs
	for _, m := range membersInside {
		b, err = newBoundaryFromDB(m.Score, m.Value)
		if err != nil {
			return nil, nil, nil, err
		}
		inside = append(inside, b)
	}

	if len(inside) > 1 {
		sort.Sort(byIP(inside))
	}

	// create above IPs
	for _, m := range membersAbove {
		b, err = newBoundaryFromDB(m.Score, m.Value)
		if err != nil {
			return nil, nil, nil, err
		}
		above = append(above, b)
	}

	if len(above) > 1 {
		sort.Sort(byIP(above))
	}
	return below, inside, above, nil
}

func (n *NutBreaker) Insert(ipRange string, value []byte) error {
	return n.db.Update(func(tx *nutsdb.Tx) error {
		return n.insert(tx, ipRange, value)
	})
}

// Insert inserts a new IP range or IP into the database with an associated reason string
func (n *NutBreaker) insert(tx *nutsdb.Tx, ipRange string, value []byte) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to insert %s: %v", ipRange, err)
		}
	}()

	low, high, err := parseRange(ipRange, value)
	if err != nil {
		return err
	}

	belowN, inside, aboveN, err := n.vicinity(tx, low, high, 1)
	if err != nil {
		return err
	}

	if len(belowN) == 0 || len(aboveN) == 0 {
		return fmt.Errorf("database inconsistent: %d below, %d above", len(belowN), len(aboveN))
	}

	// remove inside
	err = n.removeInside(tx, inside)
	if err != nil {
		return err
	}

	belowNearest := belowN[0]
	belowCut := low.Below(belowNearest.Value)

	aboveNearest := aboveN[0]
	aboveCut := high.Above(aboveNearest.Value)

	insertLowerBound := true
	insertUpperBound := true

	if belowNearest.IsLowerBound() {
		// need to cut below
		if !belowNearest.EqualIP(belowCut) {
			// can cut below |----
			if !belowNearest.EqualReason(low) {
				// only insert if reasons differ
				err = belowCut.Insert(tx,
					n.blacklistBucket,
					n.blacklistSortedSetKey,
				)
				if err != nil {
					return err
				}
			} else {
				// extend range towards belowNearest
				insertLowerBound = false
			}
		} else {
			// cannot cut below
			if !belowNearest.EqualReason(low) {
				// if reasons differ, make beLowNearest a single bound
				belowNearest.SetDoubleBound()
				err = belowNearest.Insert(
					tx,
					n.blacklistBucket,
					n.blacklistSortedSetKey,
				)
				if err != nil {
					return err
				}
			} else {
				insertLowerBound = false
			}
		}
	} else if belowNearest.IsDoubleBound() && belowNearest.EqualIP(belowCut) && belowNearest.EqualReason(low) {
		// one IP below we have a single boundary range with the same reason
		belowNearest.SetLowerBound()
		err = belowNearest.Insert(
			tx,
			n.blacklistBucket,
			n.blacklistSortedSetKey,
		)
		if err != nil {
			return err
		}
	}

	if aboveNearest.IsUpperBound() {
		// need to cut above
		if !aboveNearest.EqualIP(aboveCut) {
			// can cut above -----|
			if !aboveNearest.EqualReason(high) {
				// insert if reasons differ
				err = aboveCut.Insert(
					tx,
					n.blacklistBucket,
					n.blacklistSortedSetKey,
				)
				if err != nil {
					return err
				}
			} else {
				// don't insert, because extends range
				// to upperbound above
				insertUpperBound = false
			}

		} else {
			// cannot cut above
			if !aboveNearest.EqualReason(high) {
				aboveNearest.SetDoubleBound()
				err = aboveNearest.Insert(
					tx,
					n.blacklistBucket,
					n.blacklistSortedSetKey,
				)
				if err != nil {
					return err
				}
			} else {
				insertUpperBound = false
			}
		}
	} else if aboveNearest.IsDoubleBound() && aboveNearest.EqualIP(aboveCut) && aboveNearest.EqualReason(high) {
		// one IP above we have a single boundary range with the same reason
		aboveNearest.SetUpperBound()
		err = aboveNearest.Insert(
			tx,
			n.blacklistBucket,
			n.blacklistSortedSetKey,
		)
		if err != nil {
			return err
		}
	}

	return n.insertRange(tx, low, high, insertLowerBound, insertUpperBound)
}

// simply inserts a range, either a double boundary or a single boundary based on the boolean flags
func (n *NutBreaker) insertRange(tx *nutsdb.Tx, low, high boundary, insertLow, insertHigh bool) (err error) {
	if insertLow && insertHigh {

		// double boundary, single insertion
		if low.Equal(high) {
			doubleBoundary := low
			doubleBoundary.SetDoubleBound()
			return doubleBoundary.Insert(
				tx,
				n.blacklistBucket,
				n.blacklistSortedSetKey,
			)
		}

		// insert two different boundaries
		err = low.Insert(
			tx,
			n.blacklistBucket,
			n.blacklistSortedSetKey,
		)
		if err != nil {
			return err
		}
		err = high.Insert(
			tx,
			n.blacklistBucket,
			n.blacklistSortedSetKey,
		)
		if err != nil {
			return err
		}
		return nil
	} else if insertLow {
		err = low.Insert(
			tx,
			n.blacklistBucket,
			n.blacklistSortedSetKey,
		)
		if err != nil {
			return err
		}
	} else if insertHigh {
		err = high.Insert(
			tx,
			n.blacklistBucket,
			n.blacklistSortedSetKey,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *NutBreaker) Remove(ipRange string) error {
	return n.db.Update(func(tx *nutsdb.Tx) error {
		return n.remove(tx, ipRange)
	})
}

func (n *NutBreaker) remove(tx *nutsdb.Tx, ipRange string) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to remove %s: %v", ipRange, err)
		}
	}()

	low, high, err := parseRange(ipRange, nil)
	if err != nil {
		return err
	}

	below, inside, above, err := n.vicinity(tx, low, high, 1)
	if err != nil {
		return err
	}

	if len(below) == 0 || len(above) == 0 {
		return fmt.Errorf("database inconsistent: %d below, %d above", len(below), len(above))
	}

	err = n.removeInside(tx, inside)
	if err != nil {
		return err
	}

	err = n.removeLowerBound(tx, low, below[0])
	if err != nil {
		return err
	}

	err = n.removeUpperBound(tx, high, above[0])
	if err != nil {
		return err
	}

	return nil
}

func (n *NutBreaker) removeInside(tx *nutsdb.Tx, inside []boundary) (err error) {
	for _, bnd := range inside {
		err = bnd.Remove(
			tx,
			n.blacklistBucket,
			n.blacklistSortedSetKey,
		)
		if err != nil {
			return fmt.Errorf("failed to remove inside %s: %v", bnd, err)
		}
	}
	return nil
}

func (n *NutBreaker) removeLowerBound(tx *nutsdb.Tx, low, belowNearest boundary) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to removeLow %s: %w", low, err)
		}
	}()

	if belowNearest.IsUpperBound() || belowNearest.IsDoubleBound() {
		// nothin to do
		return nil
	}

	belowCut := low.Below(belowNearest.Value)
	if belowNearest.EqualIP(belowCut) {
		// ip is one ip below and was cut to be a range that only contains a single ip
		belowNearest.SetDoubleBound()
		return belowNearest.Update(
			tx,
			n.blacklistBucket,
		)
	}

	// we cut a different range with the removal
	// we need to add the upper boundary of the cut range
	return belowCut.Insert(
		tx,
		n.blacklistBucket,
		n.blacklistSortedSetKey,
	)
}

func (n *NutBreaker) removeUpperBound(tx *nutsdb.Tx, high, aboveNearest boundary) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to removeHigh %s: %w", high, err)
		}
	}()

	if aboveNearest.IsLowerBound() || aboveNearest.IsDoubleBound() {
		// nothin to do
		return nil
	}

	aboveCut := high.Above(aboveNearest.Value)
	if aboveNearest.EqualIP(aboveCut) {
		// ip is one ip above and was cut to be a range that only contains a single ip
		aboveNearest.SetDoubleBound()
		return aboveNearest.Update(
			tx,
			n.blacklistBucket,
		)
	}

	// we cut a different range with the removal
	// we need to add the lower boundary of the cut range
	return aboveCut.Insert(
		tx,
		n.blacklistBucket,
		n.blacklistSortedSetKey,
	)
}

func (n *NutBreaker) Find(ip string) (value []byte, err error) {
	err = n.db.View(func(tx *nutsdb.Tx) (err error) {
		v, err := n.find(tx, ip)
		if err != nil {
			return err
		}
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Find searches for the requested IP in the database. If the IP is found within any previously inserted range,
// the associated reason is returned. If it is not found, an error is returned instead.
// returns a reason or either
// ErrIPNotFound if no IP was found
// ErrDatabaseInconsistent if the database has become inconsistent.
func (n *NutBreaker) find(tx *nutsdb.Tx, ip string) (value []byte, err error) {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, err
	}

	if !addr.Is4() {
		return nil, ErrIPv6NotSupported
	}

	bnd, err := newBoundary(addr, true, true, nil)
	if err != nil {
		return nil, err
	}

	below, inside, above, err := n.vicinity(tx, bnd, bnd, 1)
	if err != nil {
		return nil, err
	}

	if len(inside) == 1 {
		found := inside[0]
		return found.Value, nil
	}

	if len(below) == 0 || len(above) == 0 {
		return nil, fmt.Errorf("database inconsistent: %d below, %d above", len(below), len(above))
	}

	belowNearest := below[0]
	aboveNearest := above[0]

	if belowNearest.IsLowerBound() && aboveNearest.IsUpperBound() {
		if belowNearest.EqualReason(aboveNearest) {
			return belowNearest.Value, nil
		}
		return nil, fmt.Errorf("reasons inconsistent: %v != %v", belowNearest.Value, aboveNearest.Value)
	}

	return nil, ErrIPNotFound
}

func (n *NutBreaker) isConsistent(ipRange ...string) error {
	return n.db.View(func(tx *nutsdb.Tx) error {
		return n.consistent(tx, ipRange...)
	})
}

func (n *NutBreaker) consistent(tx *nutsdb.Tx, ipRange ...string) error {
	ipr := ""
	if len(ipRange) > 0 {
		ipr = ipRange[0]
	}

	attributes, err := n.all(tx)
	if err != nil {
		return err
	}

	const (
		LowerBound = 0
		UpperBound = 1
	)

	if ipr != "" {
		low, high, err := parseRange(ipr, nil)
		if err != nil {
			return err
		}

		foundLow, foundHigh := false, false
		for _, attr := range attributes {
			if attr.EqualIP(low) && attr.LowerBound {
				foundLow = true
			}

			if attr.EqualIP(high) && attr.UpperBound {
				foundHigh = true
			}
		}
		if !foundLow || !foundHigh {
			if !foundLow && !foundHigh {
				return fmt.Errorf("did neither find inserted LOWERBOUND neither UPPERBOUND")
			} else if !foundLow {
				return fmt.Errorf("did not find inserted LOWERBOUND")
			}
			return fmt.Errorf("did not find inserted UPPERBOUND")
		}
	}

	cnt := 0
	state := LowerBound
	for idx, attr := range attributes {

		if attr.LowerBound && attr.UpperBound {
			if state != UpperBound {
				return fmt.Errorf("database inconsistent: double boundary: idx=%d state=%d, expected state=%d", idx, state, UpperBound)
			}

			cnt += 2
		} else if attr.LowerBound {
			if state != UpperBound {
				return fmt.Errorf("database inconsistent: lower boundary: idx=%d state=%d, expected state=%d", idx, state, UpperBound)
			}
			cnt++
			state = cnt % 2
		} else if attr.UpperBound {
			if state != LowerBound {
				return fmt.Errorf("database inconsistent: upper boundary: idx=%d state=%d, expected state=%d", idx, state, LowerBound)
			}

			// reasons consistent
			if idx > 0 && !bytes.Equal(attr.Value, attributes[idx-1].Value) {
				return fmt.Errorf("reason mismatch: idx=%4d reason=%q idx=%4d reason=%q", idx-1, attributes[idx-1].Value, idx, attr.Value)
			}

			cnt++
			state = cnt % 2
		}
	}

	if state != LowerBound {
		return fmt.Errorf("database inconsistent: final boundary is supposed to be a lower boundary")
	}

	return nil
}
