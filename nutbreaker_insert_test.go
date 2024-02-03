package nutbreaker

import (
	"testing"
)

func TestInsertSingleBigRange(t *testing.T) {
	ndb, cleanup := initDB(t)
	defer cleanup()

	inserted := insert(
		t,
		ndb,
		true, // reason is relevant here
		"0.0.0.0 - 255.255.255.255",
	)

	expected := []boundary{
		negInfBoundary,
		inserted[0], // 0.0.0.0
		inserted[1], // 127.255.255.255
		posInfBoundary,
	}
	equal(t, ndb, expected...)
	consistent(t, ndb)
}

func TestInsertOverlappingUB(t *testing.T) {
	ndb, cleanup := initDB(t)
	defer cleanup()

	inserted := insert(
		t,
		ndb,
		true, // reason is relevant here
		"123.0.0.0 - 123.0.0.4",
		"123.0.0.3 - 123.0.0.5",
	)

	expected := []boundary{
		negInfBoundary,
		inserted[0], // 123.0.0.0
		inserted[3], // 123.0.0.5
		posInfBoundary,
	}

	equal(t, ndb, expected...)
	consistent(t, ndb)
}

func TestInsertOverlappingLB(t *testing.T) {
	ndb, cleanup := initDB(t)
	defer cleanup()

	inserted := insert(
		t,
		ndb,
		true, // reason is relevant here
		"123.0.0.3 - 123.0.0.5",
		"123.0.0.0 - 123.0.0.4",
	)

	expected := []boundary{
		negInfBoundary,
		inserted[2], // 123.0.0.0
		inserted[1], // 123.0.0.5
		posInfBoundary,
	}

	equal(t, ndb, expected...)
	consistent(t, ndb)
}

func TestInsertNonOverlapping(t *testing.T) {
	ndb, cleanup := initDB(t)
	defer cleanup()

	inserted := insert(
		t,
		ndb,
		true, // reason is relevant here
		"123.0.0.0 - 123.0.0.2",
		"123.0.0.4 - 123.0.0.6",
	)

	expected := []boundary{
		negInfBoundary,
		inserted[0], // 123.0.0.0
		inserted[1], // 123.0.0.2
		inserted[2], // 123.0.0.4
		inserted[3], // 123.0.0.6
		posInfBoundary,
	}

	equal(t, ndb, expected...)
	consistent(t, ndb)
}
