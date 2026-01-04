package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

// Cache is a high-performance, generic in-memory cache with lock-free reads.
//
// It uses an RCU (Read-Copy-Update) style design:
//   - Reads are lock-free using atomic.Pointer
//   - Writes go to a separate write buffer protected by mutex
//   - Periodic sync merges writes and removes expired entries
//
// This design is optimized for read-heavy workloads where read performance
// is critical and write latency can be slightly higher.
type Cache[T any] struct {
	// readMap holds the immutable read-only map.
	// Only accessed via atomic load, never mutated after publish.
	readMap atomic.Pointer[map[string]*Entry[T]]

	// writeMap holds pending writes until next sync.
	// Protected by mu.
	writeMap map[string]*Entry[T]

	// deleted tracks keys marked for deletion until next sync.
	// Protected by mu.
	deleted map[string]struct{}

	// mu protects writeMap and deleted.
	mu sync.Mutex

	// ticker triggers periodic sync operations.
	ticker *time.Ticker

	// done signals the sync goroutine to stop.
	done chan struct{}

	// opts holds the cache configuration.
	opts Options
}

// NewCache creates a new generic cache with the given options.
//
// Example:
//
//	userCache := cache.NewCache[*User]()
//	userCache.Set("user:123", user, 5*time.Minute)
//
//	if u, ok := userCache.Get("user:123"); ok {
//	    // use u
//	}
func NewCache[T any](opts ...Option) *Cache[T] {
	o := applyOptions(opts)

	c := &Cache[T]{
		writeMap: make(map[string]*Entry[T]),
		deleted:  make(map[string]struct{}),
		ticker:   time.NewTicker(o.SyncInterval),
		done:     make(chan struct{}),
		opts:     o,
	}

	// Initialize read map with empty map
	emptyMap := make(map[string]*Entry[T])
	c.readMap.Store(&emptyMap)

	// Start background sync goroutine
	go c.syncLoop()

	return c
}

// Get retrieves a value from the cache.
// Returns the value and true if found and not expired, zero value and false otherwise.
//
// This operation is lock-free and safe for concurrent use.
// It only uses atomic.Load and map lookup - no allocations, no mutations.
func (c *Cache[T]) Get(key string) (T, bool) {
	m := c.readMap.Load()

	if entry, ok := (*m)[key]; ok {
		if entry.Exp > time.Now().UnixNano() {
			return entry.Val, true
		}
	}

	var zero T
	return zero, false
}

// Set adds or updates a value in the cache with the given TTL.
// The value will be visible to readers after the next sync operation.
//
// This operation is protected by mutex and safe for concurrent use.
func (c *Cache[T]) Set(key string, val T, ttl time.Duration) {
	exp := time.Now().Add(ttl).UnixNano()

	c.mu.Lock()
	c.writeMap[key] = &Entry[T]{
		Val: val,
		Exp: exp,
	}
	delete(c.deleted, key) // Remove from deleted if present
	c.mu.Unlock()
}

// Delete removes a key from the cache.
// The deletion will be visible to readers after the next sync operation.
//
// This operation is protected by mutex and safe for concurrent use.
func (c *Cache[T]) Delete(key string) {
	c.mu.Lock()
	delete(c.writeMap, key) // Remove from pending writes
	c.deleted[key] = struct{}{}
	c.mu.Unlock()
}

// Len returns the number of entries in the read map.
// Note: This may include expired entries that haven't been cleaned up yet.
func (c *Cache[T]) Len() int {
	m := c.readMap.Load()
	return len(*m)
}

// Close stops the background sync goroutine and releases resources.
// The cache should not be used after Close is called.
func (c *Cache[T]) Close() {
	close(c.done)
	c.ticker.Stop()
}

// syncLoop runs the periodic sync operation.
func (c *Cache[T]) syncLoop() {
	for {
		select {
		case <-c.ticker.C:
			c.sync()
		case <-c.done:
			return
		}
	}
}

// sync performs the RCU-style synchronization:
// 1. Creates a new map
// 2. Copies non-expired entries from read map
// 3. Merges pending writes (excluding deleted keys)
// 4. Atomically swaps the read map pointer
// 5. Clears the write buffer
func (c *Cache[T]) sync() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixNano()
	oldRead := c.readMap.Load()

	// Estimate capacity for new map
	capacity := len(*oldRead) + len(c.writeMap)
	newMap := make(map[string]*Entry[T], capacity)

	// Copy non-expired, non-deleted entries from read map
	for k, v := range *oldRead {
		if _, deleted := c.deleted[k]; deleted {
			continue
		}
		if v.Exp > now {
			newMap[k] = v
		}
	}

	// Merge pending writes (non-expired only)
	for k, v := range c.writeMap {
		if v.Exp > now {
			newMap[k] = v
		}
	}

	// Atomically publish new map to readers
	c.readMap.Store(&newMap)

	// Reset write buffer and deleted set
	c.writeMap = make(map[string]*Entry[T])
	c.deleted = make(map[string]struct{})
}

// Sync forces an immediate synchronization.
// This is useful for testing or when immediate visibility is required.
func (c *Cache[T]) Sync() {
	c.sync()
}
