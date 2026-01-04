// Package cache provides a high-performance, lock-free in-memory cache
// using atomic pointers and RCU-style synchronization.
package cache

// Entry represents a cache entry with a value and expiration time.
// The Exp field stores the expiration as a Unix timestamp in nanoseconds.
type Entry[T any] struct {
	Val T     // The cached value
	Exp int64 // Expiration time (Unix nanoseconds)
}

// IsExpired checks if the entry has expired based on the given current time.
func (e *Entry[T]) IsExpired(now int64) bool {
	return e.Exp <= now
}
