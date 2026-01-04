package atomicache

import "time"

// DefaultSyncInterval is the default interval for RCU sync operations.
const DefaultSyncInterval = 1 * time.Minute

// Options configures the cache behavior.
type Options struct {
	// SyncInterval is the interval between RCU sync operations.
	// During sync, expired entries are removed and the write buffer
	// is merged into the read map.
	// Default: 1 minute
	SyncInterval time.Duration
}

// Option is a functional option for configuring the cache.
type Option func(*Options)

// WithSyncInterval sets the interval between sync operations.
// The sync interval controls how often:
//   - Expired entries are cleaned up
//   - Writes become visible to readers
//
// Shorter intervals mean faster visibility but more CPU overhead.
// Longer intervals are more efficient but delay write visibility.
func WithSyncInterval(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.SyncInterval = d
		}
	}
}

// defaultOptions returns the default cache options.
func defaultOptions() Options {
	return Options{
		SyncInterval: DefaultSyncInterval,
	}
}

// applyOptions applies functional options to the default options.
func applyOptions(opts []Option) Options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
