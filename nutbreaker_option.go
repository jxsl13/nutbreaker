package nutbreaker

type Option func(*options) error

type options struct {
	dataDir               string
	blacklistBucket       string
	blacklistSortedSetKey string
	whitelistBucket       string
}

func WithDir(dir string) Option {
	return func(o *options) error {
		o.dataDir = dir
		return nil
	}
}
