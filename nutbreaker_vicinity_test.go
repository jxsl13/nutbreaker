package nutbreaker

import (
	"testing"
)

func TestVicityInside(t *testing.T) {
	ndb, cleanup := initDB(t)
	defer cleanup()

	inserted := insert(
		t,
		ndb,
		false, // reason does not matter for this test
		"123.0.0.3 - 123.0.0.10",
	)

	below, inside, above := vicinity(t, ndb, "123.0.0.4", 1)
	expectedBelow := []boundary{
		inserted[0], // 123.0.0.3
	}
	expectedInside := []boundary{}
	expectedAbove := []boundary{
		inserted[1], // 123.0.0.10
	}

	equalBoundaries(t, expectedBelow, below, "below")
	equalBoundaries(t, expectedInside, inside, "inside")
	equalBoundaries(t, expectedAbove, above, "above")
}

func TestVicityAbove(t *testing.T) {
	ndb, cleanup := initDB(t)
	defer cleanup()

	inserted := insert(
		t,
		ndb,
		false, // reason does not matter for this test
		"123.0.0.3 - 123.0.0.10",
	)

	below, inside, above := vicinity(t, ndb, "123.0.0.12", 1)
	expectedBelow := []boundary{
		inserted[1], // 123.0.0.10
	}
	expectedInside := []boundary{}
	expectedAbove := []boundary{
		posInfBoundary,
	}

	equalBoundaries(t, expectedBelow, below, "below")
	equalBoundaries(t, expectedInside, inside, "inside")
	equalBoundaries(t, expectedAbove, above, "above")
}

func TestVicityBelow(t *testing.T) {
	ndb, cleanup := initDB(t)
	defer cleanup()

	inserted := insert(
		t,
		ndb,
		false, // reason does not matter for this test
		"123.0.0.3 - 123.0.0.10",
	)
	below, inside, above := vicinity(t, ndb, "123.0.0.1", 1)

	expectedBelow := []boundary{
		negInfBoundary,
	}
	expectedInside := []boundary{}
	expectedAbove := []boundary{
		inserted[0], // 123.0.0.3
	}

	equalBoundaries(t, expectedBelow, below, "below")
	equalBoundaries(t, expectedInside, inside, "inside")
	equalBoundaries(t, expectedAbove, above, "above")
}
