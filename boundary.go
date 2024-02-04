package nutbreaker

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"

	"github.com/nutsdb/nutsdb"
)

var (
	empty = boundary{}
)

type boundary struct {
	Key        []byte
	IP         netip.Addr
	Score      float64
	LowerBound bool
	UpperBound bool
	Value      []byte
}

func (b boundary) String() string {
	suf := "invalid"
	if b.LowerBound && b.UpperBound {
		suf = "db"
	} else if b.LowerBound {
		suf = "lb"
	} else if b.UpperBound {
		suf = "ub"
	}

	switch b.Score {
	case negInf:
		return fmt.Sprintf("-inf:%s", suf)
	case posInf:
		return fmt.Sprintf("+inf:%s", suf)
	default:
		return fmt.Sprintf("%s:%s", b.IP, suf)
	}
}

// value is the database value of the boundary
// the marshaled json string
func newBoundaryFromDB(tx *nutsdb.Tx, bucket string, m *nutsdb.SortedSetMember) (b boundary, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("failed to create boundary: score %f: %w", m.Score, err)
		}
	}()

	key := m.Value

	value, err := tx.Get(bucket, key)
	if err != nil {
		return empty, err
	}

	var v dbValue
	err = json.Unmarshal(value, &v)
	if err != nil {
		return empty, err
	}

	return newBoundaryFloat64(m.Score, v.Low, v.High, v.Value)
}

func newBoundaryFloat64(ip float64, lower, upper bool, value []byte) (boundary, error) {

	switch ip {
	case negInf:
		return negInfBoundary, nil
	case posInf:
		return posInfBoundary, nil
	}

	ui32 := uint32(ip)
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, ui32)

	nip, ok := netip.AddrFromSlice(key)
	if !ok {
		return empty, errors.New("bad ip")
	}

	if !nip.IsValid() {
		return empty, errors.New("invalid ip")
	}

	b := boundary{
		Key:        key, // is always ipv4
		IP:         nip,
		Score:      ip,
		LowerBound: lower,
		UpperBound: upper,
		Value:      value,
	}
	return b, nil
}

func newBoundary(ip netip.Addr, lower, upper bool, value []byte) (boundary, error) {
	if !ip.IsValid() {
		return empty, fmt.Errorf("invalid ip: %s", ip)
	}

	if ip.Is6() && !ip.Is4In6() {
		return empty, ErrIPv6NotSupported
	}

	key := ip.As4()
	ui32 := binary.BigEndian.Uint32(key[:])

	b := boundary{
		Key:        key[:], // is always ipv4
		IP:         ip,
		Score:      float64(ui32),
		LowerBound: lower,
		UpperBound: upper,
		Value:      append(make([]byte, 0, len(value)), value...), // copy
	}
	return b, nil
}

// invBounds returns the inverse of the current boundaries.
func (b *boundary) invBounds() (lb, ub bool) {
	return !b.LowerBound, !b.UpperBound
}

// Below returns a new boundary that is one IP below the current one.
// The returned boundary (lb/ub) is inversed to the one that the current boundary has
func (b *boundary) Below(value ...[]byte) boundary {
	if b.Score == negInf {
		return negInfBoundary
	}

	prev := b.IP.Prev()
	if !prev.IsValid() {
		return negInfBoundary
	}

	v := b.Value
	if len(value) > 0 {
		v = value[0]
	}

	var lb, ub bool
	if b.IsDoubleBound() {
		lb, ub = false, true
	} else {
		lb, ub = b.invBounds()
	}
	bl, err := newBoundary(prev, lb, ub, v)
	if err != nil {
		panic(fmt.Errorf("failed to get boundary below of %s: %w", b, err))
	}
	return bl
}

// Above returns a new boundary that is one IP above the current one.
// The returned boundary (lb/ub) is inversed to the one that the current boundary has
func (b *boundary) Above(value ...[]byte) boundary {
	if b.Score == posInf {
		return posInfBoundary
	}

	next := b.IP.Next()
	if !next.IsValid() {
		return posInfBoundary
	}

	v := b.Value
	if len(value) > 0 {
		v = value[0]
	}
	var lb, ub bool
	if b.IsDoubleBound() {
		lb, ub = true, false
	} else {
		lb, ub = b.invBounds()
	}
	ab, err := newBoundary(next, lb, ub, v) //TODO: do we need to pass the value?
	if err != nil {
		panic(fmt.Errorf("failed to get boundary above of %s: %w", b, err))
	}
	return ab
}

// IsSingleBoundary returns true if b is only one of both boundaries, either only lower or only upperbound
func (b *boundary) IsSingleBound() bool {
	return b.LowerBound != b.UpperBound
}

// SetLowerBound sets b to be a single lower boundary.
func (b *boundary) SetLowerBound() {
	b.LowerBound = true
	b.UpperBound = false
}

// IsLowerBound only returns true if the boundary is a single boundary as well as a lower boundary.
func (b *boundary) IsLowerBound() bool {
	return b.IsSingleBound() && b.LowerBound
}

func (b boundary) AsLowerBound() boundary {
	if b.IsLowerBound() {
		return b
	}
	bCopy := b
	bCopy.SetLowerBound()
	return bCopy
}

// SetLowerBound sets b to be a single upper boundary.
func (b *boundary) SetUpperBound() {
	b.LowerBound = false
	b.UpperBound = true
}

func (b boundary) AsUpperBound() boundary {
	if b.IsUpperBound() {
		return b
	}
	bCopy := b
	bCopy.SetUpperBound()
	return bCopy
}

// IsUpperBound only returns true if the boundary is a single boundary as well as an upper boundary.
func (b *boundary) IsUpperBound() bool {
	return b.IsSingleBound() && b.UpperBound
}

// SetDoubleBound sets b to be a lower as well as an upper boundary
func (b *boundary) SetDoubleBound() {
	b.LowerBound = true
	b.UpperBound = true
}

func (b boundary) AsDoubleBound() boundary {
	if b.IsDoubleBound() {
		return b
	}
	bCopy := b
	bCopy.SetDoubleBound()
	return bCopy
}

// IsDoubleBound only returns true if both lower and upper bounds are true
func (b *boundary) IsDoubleBound() bool {
	if !b.LowerBound && !b.UpperBound {
		panic("invalid boundary state")
	}
	return b.LowerBound && b.UpperBound
}

// Equal tests, whether both b and other have exactly the same members.
func (b *boundary) Equal(other boundary) bool {
	return bytes.Equal(b.Key, other.Key) &&
		b.IP == other.IP &&
		b.Score == other.Score &&
		b.LowerBound == other.LowerBound &&
		b.UpperBound == other.UpperBound &&
		bytes.Equal(b.Value, other.Value)
}

func (b *boundary) EqualIP(other boundary) bool {
	return b.IP == other.IP &&
		b.Score == other.Score
}

// EqualValue returns true if values are equal and not empty, false otherwise.
func (b *boundary) EqualValue(other boundary) bool {
	return bytes.Equal(b.Value, other.Value)
}

func (b *boundary) EqualIPValue(other boundary) bool {
	return b.EqualIP(other) && b.EqualValue(other)
}

func (b *boundary) ToDBValue() dbValue {
	return dbValue{
		High:  b.UpperBound,
		Low:   b.LowerBound,
		Value: b.Value,
	}
}

func (b *boundary) Bytes() []byte {
	return b.ToDBValue().Bytes()
}

func (b *boundary) IsInf() bool {
	return b.Score == posInf || b.Score == negInf
}

func (b boundary) Insert(tx *nutsdb.Tx, bucketKV string, zKey []byte) error {
	if b.IsInf() {
		panic(fmt.Sprintf("cannot insert infinite boundary with Insert: %s", b))
	}
	return b.InsertInf(tx, bucketKV, zKey)
}

// Insert adds the necessary commands to the transaction in order to be properly inserted.
func (b boundary) InsertInf(tx *nutsdb.Tx, bucketKV string, zKey []byte) error {
	err := tx.ZAdd(
		bucketKV,
		zKey,
		b.Score,
		b.Key,
	)
	if err != nil {
		return fmt.Errorf("failed to insert boundary: failed to zadd boundary: %s: %w", b, err)
	}

	err = tx.Put(bucketKV, b.Key, b.Bytes(), 0)
	if err != nil {
		return fmt.Errorf("failed to insert boundary: failed to put boundary: %s: %w", b, err)
	}
	return nil
}

// Update adds the needed commands to the transaction in order to update the assiciated attributes of the
// unserlying IP. The IP itself cannot be updated with this command.
func (b boundary) Update(tx *nutsdb.Tx, bucketKV string) error {
	err := tx.PutIfExists(bucketKV, b.Key, b.Bytes(), 0)
	if err != nil {
		return fmt.Errorf("failed to update boundary: %s: %w", b, err)
	}
	return nil
}

func (b boundary) RemoveInf(tx *nutsdb.Tx, bucketKV string, zKey []byte) error {
	err := tx.ZRem(bucketKV, zKey, b.Key)
	if err != nil {
		return fmt.Errorf("failed to remove boundary: failed to zrem boundary: %s: %w", b, err)
	}

	err = tx.Delete(bucketKV, b.Key)
	if err != nil {
		return fmt.Errorf("failed to remove boundary: failed to delete boundary: %s: %w", b, err)
	}
	return nil
}

// Remove adds the necessary commands to the transaction in order to be properly removed.
func (b boundary) Remove(tx *nutsdb.Tx, bucketKV string, zKey []byte) (err error) {
	if b.IsInf() {
		panic(fmt.Sprintf("cannot remove infinite boundary with Insert: %s", b))
	}
	return b.RemoveInf(tx, bucketKV, zKey)
}

// Get adds the necessary commands to the transaction in order to retrieve the attributs from the database.
func (b boundary) Get(tx *nutsdb.Tx, bucketKV string) (dbValue, error) {
	data, err := tx.Get(bucketKV, b.Key)
	if err != nil {
		return dbValue{}, fmt.Errorf("failed to get boundary: %s: %w", b, err)
	}
	var v dbValue
	err = json.Unmarshal(data, &v)
	if err != nil {
		return dbValue{}, fmt.Errorf("failed to get boundary: %s: %w", b, err)
	}
	return v, nil
}

type dbValue struct {
	High  bool   `json:"high"`
	Low   bool   `json:"low"`
	Value []byte `json:"value"`
}

func (v dbValue) Bytes() []byte {
	data, _ := json.Marshal(v)
	return data
}
