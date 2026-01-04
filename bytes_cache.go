package cache

import "time"

// BytesCache is a specialized cache for []byte values.
// It's optimized for storing pre-marshaled data like JSON API responses.
//
// This cache is designed for zero-copy reads, making it ideal for
// Fiber/net/http handlers that can directly send the cached bytes.
type BytesCache struct {
	*Cache[[]byte]
}

// NewBytesCache creates a new cache specialized for []byte values.
//
// Example with Fiber:
//
//	cache := cache.NewBytesCache()
//
//	// Cache the JSON response
//	cache.Set("users:list", jsonBytes, 5*time.Minute)
//
//	// In handler
//	if v, ok := cache.Get(key); ok {
//	    return c.Send(v)  // Zero-copy response
//	}
func NewBytesCache(opts ...Option) *BytesCache {
	return &BytesCache{
		Cache: NewCache[[]byte](opts...),
	}
}

// SetJSON is a convenience method for caching JSON data.
// It's functionally identical to Set but provides clearer intent.
func (c *BytesCache) SetJSON(key string, data []byte, ttl time.Duration) {
	c.Set(key, data, ttl)
}

// GetOrSet retrieves a value from the cache, or sets it using the provided
// function if not found or expired.
//
// This is useful for cache-aside patterns:
//
//	data, _ := cache.GetOrSet("users:list", 5*time.Minute, func() ([]byte, error) {
//	    return json.Marshal(users)
//	})
func (c *BytesCache) GetOrSet(key string, ttl time.Duration, fn func() ([]byte, error)) ([]byte, error) {
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
