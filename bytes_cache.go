package atomicache

import (
	"sync"
	"sync/atomic"
	"time"
)

// ByteEntry represents a cache entry for []byte values.
type ByteEntry struct {
	Val []byte // The cached bytes
	Exp int64  // Expiration time (Unix nanoseconds)
}

// CacheByte is a high-performance, non-generic cache specialized for []byte.
// It avoids generic overhead for maximum performance in hot paths.
//
// Designed for:
//   - API responses (JSON)
//   - Pre-marshaled payloads
//   - Direct HTTP response with zero-copy
//
// This cache is optimized for Fiber/net/http handlers.
type CacheByte struct {
	// readMap holds the immutable read-only map.
	// Only accessed via atomic load, never mutated after publish.
	readMap atomic.Pointer[map[string]*ByteEntry]

	// writeMap holds pending writes until next sync.
	// Protected by mu.
	writeMap map[string]*ByteEntry

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

// NewCacheByte creates a new cache specialized for []byte values.
//
// Example with Fiber:
//
//	cache := atomicache.NewCacheByte()
//	defer cache.Close()
//
//	// Cache the JSON response
//	cache.Set("users:list", jsonBytes, 5*time.Minute)
//
//	// In handler
//	if v, ok := cache.Get(key); ok {
//	    return c.Send(v)  // Zero-copy response
//	}
func NewCacheByte(opts ...Option) *CacheByte {
	o := applyOptions(opts)

	c := &CacheByte{
		writeMap: make(map[string]*ByteEntry),
		deleted:  make(map[string]struct{}),
		ticker:   time.NewTicker(o.SyncInterval),
		done:     make(chan struct{}),
		opts:     o,
	}

	// Initialize read map with empty map
	emptyMap := make(map[string]*ByteEntry)
	c.readMap.Store(&emptyMap)

	// Start background sync goroutine
	go c.syncLoop()

	return c
}

// Get retrieves a []byte value from the cache.
// Returns the value and true if found and not expired, nil and false otherwise.
//
// This operation is lock-free and safe for concurrent use.
// It only uses atomic.Load and map lookup - no allocations, no mutations.
func (c *CacheByte) Get(key string) ([]byte, bool) {
	m := c.readMap.Load()

	if entry, ok := (*m)[key]; ok {
		if entry.Exp > time.Now().UnixNano() {
			return entry.Val, true
		}
	}

	return nil, false
}

// Set adds or updates a []byte value in the cache with the given TTL.
// The value will be visible to readers after the next sync operation.
//
// This operation is protected by mutex and safe for concurrent use.
func (c *CacheByte) Set(key string, val []byte, ttl time.Duration) {
	exp := time.Now().Add(ttl).UnixNano()

	c.mu.Lock()
	c.writeMap[key] = &ByteEntry{
		Val: val,
		Exp: exp,
	}
	delete(c.deleted, key)
	c.mu.Unlock()
}

// SetJSON is a convenience method for caching JSON data.
// It's functionally identical to Set but provides clearer intent.
func (c *CacheByte) SetJSON(key string, data []byte, ttl time.Duration) {
	c.Set(key, data, ttl)
}

// Delete removes a key from the cache.
// The deletion will be visible to readers after the next sync operation.
//
// This operation is protected by mutex and safe for concurrent use.
func (c *CacheByte) Delete(key string) {
	c.mu.Lock()
	delete(c.writeMap, key)
	c.deleted[key] = struct{}{}
	c.mu.Unlock()
}

// Len returns the number of entries in the read map.
// Note: This may include expired entries that haven't been cleaned up yet.
func (c *CacheByte) Len() int {
	m := c.readMap.Load()
	return len(*m)
}

// Close stops the background sync goroutine and releases resources.
// The cache should not be used after Close is called.
func (c *CacheByte) Close() {
	close(c.done)
	c.ticker.Stop()
}

// GetOrSet retrieves a value from the cache, or sets it using the provided
// function if not found or expired.
//
// This is useful for cache-aside patterns:
//
//	data, _ := cache.GetOrSet("users:list", 5*time.Minute, func() ([]byte, error) {
//	    return json.Marshal(users)
//	})
func (c *CacheByte) GetOrSet(key string, ttl time.Duration, fn func() ([]byte, error)) ([]byte, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}

	data, err := fn()
	if err != nil {
		return nil, err
	}

	c.Set(key, data, ttl)
	return data, nil
}

// syncLoop runs the periodic sync operation.
func (c *CacheByte) syncLoop() {
	for {
		select {
		case <-c.ticker.C:
			c.sync()
		case <-c.done:
			return
		}
	}
}

// sync performs the RCU-style synchronization.
func (c *CacheByte) sync() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixNano()
	oldRead := c.readMap.Load()

	// Estimate capacity for new map
	capacity := len(*oldRead) + len(c.writeMap)
	newMap := make(map[string]*ByteEntry, capacity)

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
	c.writeMap = make(map[string]*ByteEntry)
	c.deleted = make(map[string]struct{})
}

// Sync forces an immediate synchronization.
// This is useful for testing or when immediate visibility is required.
func (c *CacheByte) Sync() {
	c.sync()
}
