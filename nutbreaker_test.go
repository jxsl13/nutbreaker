package nutbreaker

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xgfone/go-netaddr"
)

type rangeReason struct {
	Range  string
	Reason string
}

func (r rangeReason) Value() []byte {
	return []byte(r.Reason)
}

var (
	ranges = []rangeReason{
		{"120.2.2.2/1", "zero"},
		{"200.0.0.0 - 230.0.0.0", "first"},
		{"210.0.0.0 - 220.0.0.0", "second"},
		{"190.0.0.0 - 205.0.0.0", "third"},
		{"205.0.0.0 - 225.0.0.0", "fourth"},
		{"201.0.0.0 - 202.0.0.0", "fifth"},
		{"203.0.0.0 - 204.0.0.0", "seventh"},
		{"205.0.0.0 - 235.0.0.0", "eighth"},
		{"190.0.0.0 - 235.0.0.0", "ninth"},
		{"190.0.0.0 - 195.0.0.0", "10th"},
		{"195.0.0.0 - 196.0.0.0", "11th"},
		{"196.0.0.0 - 197.0.0.0", "12th"},
		{"197.0.0.0 - 235.0.0.0", "13th"},
		{"188.0.0.0 - 198.0.0.0", "14th"},
		{"188.0.0.0 - 235.0.0.0", "15th"},
		{"188.0.0.0 - 235.0.0.255", "16th"},
		{"187.255.255.255 - 235.0.1.0", "17th"},
		{"188.0.0.1 - 235.0.0.254", "18th"},
		{"123.0.0.0 - 123.0.0.10", "19th"},
		{"123.0.0.1 - 123.0.0.9", "20th"},
		{"235.0.0.255", "21st"},
		{"188.0.0.0", "22nd"},
		{"188.0.0.0", "23rd"},
		{"123.0.0.0 - 123.0.0.2", "24th"},
		{"123.0.0.1", "25th"},
		{"123.0.0.2", "26th"},
		{"123.0.0.3", "27th"},
		{"123.0.0.4", "28th"},
		{"123.0.0.5", "29th"},
		{"123.0.0.6", "30th"},
		{"123.0.0.7", "31st"},
		{"123.0.0.8", "32nd"},
		{"123.0.0.1 - 123.0.0.2", "33rd"},
		{"123.0.0.1 - 123.0.0.3", "34th"},
		{"123.0.0.1 - 123.0.0.4", "35th"},
		{"123.0.0.1 - 123.0.0.5", "36th"},
		{"123.0.0.1 - 123.0.0.6", "37th"},
		{"123.0.0.1 - 123.0.0.7", "38th"},
		{"123.0.0.1 - 123.0.0.8", "39th"},
		{"123.0.0.1 - 123.0.0.9", "40th"},
		{"123.0.0.1 - 123.0.0.10", "41st"},
		{"123.0.0.2 - 123.0.0.10", "42nd"},
		{"123.0.0.3 - 123.0.0.10", "43rd"},
		{"123.0.0.4 - 123.0.0.10", "44th"},
		{"123.0.0.5 - 123.0.0.10", "45th"},
		{"98.231.84.169 - 114.253.39.105", "46th"},
		{"122.29.207.117 - 122.29.207.117", "47th"},
		{"36.194.221.128 - 118.245.65.201", "48th"},
		{"86.196.27.130 - 101.181.15.63", "49th"},
		{"101.181.15.64 - 101.181.15.95", "50th"},
		{"101.181.15.96 - 123.10.177.145", "51st"},
		{"123.10.177.146 - 127.134.179.196", "52nd"},
		{"19.188.174.203 - 101.181.207.70", "53rd"},

		// {"", "53rd"},
		// {"", "54th"},
		// {"", "55th"},

	}
)

func TestInsert(t *testing.T) {
	// generate random ranges
	// initRanges(t, 200)

	// initial test
	tests := []testCase{
		{"cut below and cut above hit a boundary",
			[]rangeReason{
				{"123.0.0.0 - 123.0.0.2", "1st"},
				{"123.0.0.4 - 123.0.0.6", "2nd"},
				{"123.0.0.3", "3rd"},
				{"123.0.0.1 - 123.0.0.5", "4th"},
			},
			false,
		},
		{"simple insert all", ranges, false},
	}

	for _, tt := range tests {
		func(t *testing.T) {
			require := require.New(t)

			ndb, cleanup := initDB(t)
			defer cleanup()

			// consistency after every insert
			for _, ipRange := range tt.ipRanges {
				v := ipRange.Value()
				err := ndb.Insert(ipRange.Range, v)
				require.Falsef((err != nil) != tt.wantErr,
					"ndb.Insert() error = %v, wantErr %v, range passed: %q",
					err,
					tt.wantErr,
					ipRange.Range,
				)

				require.NoErrorf(
					ndb.isConsistent(),
					"ndb.Insert() error: Database INCONSISTENT after inserting range: %s",
					ipRange.Range,
				)

				t.Logf("ndb.Insert() Info: Database is CONSISTENT after inserting range: %s", ipRange.Range)
			}
		}(t)
	}
}

/*
func TestFind(t *testing.T) {

	tests := initTestCasesFind(t, 100)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ndb := initDB(t)
			defer ndb.Close()
			require := require.New(t)

			for _, rir := range tt.ipRanges {
				ipToFind := rir.IP
				reasonToFind := []byte(rir.Reason)
				rangeToFind := rir.Range

				err := ndb.Insert(rangeToFind, reasonToFind)
				require.NoError(err,
					"ndb.Insert(): range passed: %q",
					rangeToFind,
				)
				require.NoError(ndb.isConsistent(), "ndb.Insert() error : Database INCONSISTENT")

				got, err := ndb.Find(ipToFind)
				require.Falsef(
					(err != nil) != tt.wantErr,
					"ndb.Find(), NOT IN RANGE error = %q, wantErr %v\nRange: %q IP: %s",
					err.Error(),
					tt.wantErr,
					rangeToFind,
					ipToFind,
				)

				require.Equal(reasonToFind, got, "ndb.Find(), WRONG REASON")

			}

		})
	}
}
*/

func TestClientRemove(t *testing.T) {

	tests := []testCaseFind{}

	tests = append(tests, initTestCasesFind(t, 100)...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ndb, cleanup := initDB(t)
			defer cleanup()
			require := require.New(t)

			for idx, rir := range tt.ipRanges {
				ipToFind := rir.IP
				reasonToFind := []byte(rir.Reason)
				rangeToFind := rir.Range

				err := ndb.Insert(rangeToFind, reasonToFind)
				require.NoError(err, "ndb.Insert() error")

				require.NoErrorf(ndb.isConsistent(rangeToFind),
					"ndb.Insert() error : Database INCONSISTENT (idx=%d)after inserting range: %s",
					idx,
					rangeToFind,
				)

				t.Logf("rdb.Insert() Info  : Database is CONSISTENT after inserting range: %s", rangeToFind)

				got, err := ndb.Find(ipToFind)
				if tt.wantErr {
					require.ErrorIsf(err, ErrIPNotFound,
						"ndb.Find(), IN RANGE (idx=%d)\nRange: %v IP: %v",
						idx,
						rangeToFind,
						ipToFind,
					)
				} else {
					require.NoErrorf(
						err,
						"ndb.Find(), IN RANGE (idx=%d)\nRange: %v IP: %v",
						idx,
						rangeToFind,
						ipToFind,
					)
				}

				require.Equal(reasonToFind, got, "ndb.Find(), WRONG REASON")
				require.NoError(ndb.Remove(rangeToFind))
				require.NoErrorf(ndb.isConsistent(),
					"ndb.Remove() error : Database INCONSISTENT after removing range: %s",
					rangeToFind,
				)
				t.Logf("rdb.Remove() Info  : Database is CONSISTENT after inserting range: %s", rangeToFind)

				// should not be found after range deletion
				_, err = ndb.Find(ipToFind)
				require.ErrorIsf(err,
					ErrIPNotFound,
					"ndb.Find(),FOUND AFTER RANGE DELETION\nRange: %q IP: %s",
					rangeToFind,
				)

			}
		})
	}
}

type testCase struct {
	name     string
	ipRanges []rangeReason
	wantErr  bool
}

func generateBetween(low, high int64) int64 {

	if low > high {
		low, high = high, low
	}

	between := low
	if high-low > 0 {
		between = low + rand.Int63n(high-low)
	}

	return between
}

// generateRange generates a valid IP range
// and and returns a random IP that is within the range
func generateRange(t *testing.T) (ipRange string, insideIP string) {

	const minIP = math.MaxInt32 / 128 // don't want empty IP bytes
	const maxIP = math.MaxInt32 / 2

	const randBorder = maxIP - minIP

	low := minIP + rand.Int63n(randBorder)
	high := minIP + rand.Int63n(randBorder)

	if low > high {
		low, high = high, low
	}

	between := generateBetween(low, high)

	if between < low || high < between {
		t.Fatal("invalid ip generated")
	}

	lowIP := netaddr.MustNewIPAddress(low).String()
	highIP := netaddr.MustNewIPAddress(high).String()

	testregex := regexp.MustCompile(`[.:0-9]+`)

	if !(testregex.MatchString(lowIP) && testregex.MatchString(highIP)) {
		t.Fatalf("invalid ip generatred: low: %q high: %q", lowIP, highIP)
	}

	betweenIPStr := netaddr.MustNewIPAddress(between).String()
	hyphenRange := fmt.Sprintf("%s - %s", lowIP, highIP)

	if rand.Int()%2 == 0 {
		return hyphenRange, betweenIPStr
	}

	mask := rand.Intn(32-1) + 1

	cidrRange := fmt.Sprintf("%s/%d", lowIP, mask)

	net := netaddr.MustNewIPNetwork(cidrRange)

	lowerInt := net.First().BigInt().Int64()
	higherInt := net.Last().BigInt().Int64()

	between = generateBetween(lowerInt, higherInt)

	if between < lowerInt || higherInt < between {
		t.Fatal("invalid ip generated")
	}

	betweenIP := netaddr.MustNewIPAddress(between)

	return cidrRange, betweenIP.String()
}

func initDB(t *testing.T) (n *NutBreaker, cleanup func()) {
	require := require.New(t)

	dataDir := generateRandomDbDirName()

	// new default client
	n, err := NewNutBreaker(
		WithDir(dataDir),
	)
	require.NoError(err)

	require.NoError(n.Reset())
	return n, func() {
		require.NoError(n.Close())
		require.NoError(os.RemoveAll(dataDir))
	}

}

func shuffled[T any](a []T) []T {
	result := make([]T, len(a))
	copy(result, a)
	rand.Shuffle(len(result), func(i, j int) { result[i], result[j] = result[j], result[i] })
	return a
}

func initRanges(t *testing.T, num int) (result []rangeReason) {
	// generate ranges
	for i := 1; i <= num; i++ {
		ipRange, _ := generateRange(t)

		result = append(result, rangeReason{
			Range:  ipRange,
			Reason: fmt.Sprintf("random %v", rand.Int63n(int64(i))),
		})
	}
	return result
}

type rangeIPReason struct {
	Range  string
	IP     string
	Reason string
}

type testCaseFind struct {
	name     string
	ipRanges []rangeIPReason
	wantErr  bool
}

var (
	findRanges = []rangeIPReason{
		{"17.115.210.3/30", "17.115.210.0", "manual 1"},
	}
)

func initRangesAndIPsWithin(t *testing.T, num int) {
	// generate ranges
	for i := 1; i <= num; i++ {
		ipRange, ip := generateRange(t)
		findRanges = append(findRanges, rangeIPReason{
			Range:  ipRange,
			IP:     ip,
			Reason: fmt.Sprintf("random %v", rand.Int63n(int64(i))),
		})
	}
}

func initTestCasesFind(t *testing.T, num int) (testCases []testCaseFind) {
	initRangesAndIPsWithin(t, 100)
	testCases = make([]testCaseFind, num)

	for i := 0; i < num; i++ {

		shuffledRange := shuffled(findRanges)
		if i == 0 {
			// first one is not shuffled
			shuffledRange = findRanges
		}

		testCases[i] = testCaseFind{
			name:     fmt.Sprintf("random test case find %5d", i),
			ipRanges: shuffledRange,
			wantErr:  false,
		}
	}
	return
}
