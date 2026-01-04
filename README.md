# Atomicache

High-Performance In-Memory Cache for Go with Atomic Pointer, Lock-Free Reads, and TTL Support.

## Features

- 🚀 **Lock-free reads** using `atomic.Pointer`
- 🔒 **Thread-safe** writes with minimal contention
- ⏰ **TTL support** with automatic expiration
- 🧹 **RCU-style garbage collection** of expired entries
- 📦 **Generic** - works with any type
- 🎯 **Specialized `[]byte` cache** for HTTP responses
- ⚡ **Zero allocations** on read path

## Installation

```bash
go get github.com/masbur/atomicache
```

## Quick Start

### Generic Cache

```go
package main

import (
    "fmt"
    "time"

    "github.com/masbur/atomicache"
)

type User struct {
    ID   int
    Name string
}

func main() {
    // Create a cache for *User
    userCache := atomicache.NewCache[*User]()
    defer userCache.Close()

    // Store a user with 5 minute TTL
    user := &User{ID: 123, Name: "Alice"}
    userCache.Set("user:123", user, 5*time.Minute)

    // Force sync for immediate visibility (optional)
    userCache.Sync()

    // Retrieve the user
    if u, ok := userCache.Get("user:123"); ok {
        fmt.Printf("Found user: %s\n", u.Name)
    }
}
```

### Bytes Cache (for HTTP/API responses)

```go
package main

import (
    "encoding/json"
    "time"

    "github.com/masbur/atomicache"
)

func main() {
    // Create a bytes cache for JSON responses
    apiCache := atomicache.NewBytesCache()
    defer apiCache.Close()

    // Cache a JSON response
    response := []byte(`{"status":"ok","data":[1,2,3]}`)
    apiCache.Set("api:users", response, 10*time.Minute)

    // With Fiber
    // if v, ok := apiCache.Get(key); ok {
    //     return c.Send(v)  // Zero-copy response
    // }
}
```

### GetOrSet Pattern

```go
data, err := apiCache.GetOrSet("api:users", 5*time.Minute, func() ([]byte, error) {
    // This function is only called if cache miss
    return json.Marshal(fetchUsers())
})
```

## Configuration

```go
// Custom sync interval (default: 1 minute)
cache := cache.NewCache[string](
    cache.WithSyncInterval(30 * time.Second),
)
```

## Architecture

### RCU (Read-Copy-Update) Design

Atomicache uses an RCU-style design optimized for read-heavy workloads:

```
┌─────────────────────────────────────────────────────────────────┐
│                        Cache Structure                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   readMap (atomic.Pointer)          writeMap (mutex-protected)  │
│   ┌─────────────────────┐           ┌─────────────────────┐     │
│   │  Immutable Map      │           │  Mutable Map        │     │
│   │  ───────────────    │           │  ───────────────    │     │
│   │  key1 → entry1      │           │  keyA → entryA      │     │
│   │  key2 → entry2      │           │  keyB → entryB      │     │
│   │  key3 → entry3      │           │                     │     │
│   └─────────────────────┘           └─────────────────────┘     │
│            ▲                                  │                 │
│            │                                  │                 │
│      atomic.Load()                      sync.Mutex              │
│      (lock-free)                        (on write)              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

                        Periodic Sync (RCU)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. Lock mutex                                                   │
│ 2. Create new map                                               │
│ 3. Copy non-expired entries from readMap                        │
│ 4. Merge entries from writeMap                                  │
│ 5. atomic.Store(&newMap) ← Readers see new data                 │
│ 6. Clear writeMap                                               │
│ 7. Unlock mutex                                                 │
│ 8. Old map becomes garbage (GC will clean it up)                │
└─────────────────────────────────────────────────────────────────┘
```

### Read Path (Lock-Free)

```go
func (c *Cache[T]) Get(key string) (T, bool) {
    m := c.readMap.Load()  // Atomic load, no lock
    if entry, ok := (*m)[key]; ok {
        if entry.Exp > time.Now().UnixNano() {
            return entry.Val, true
        }
    }
    var zero T
    return zero, false
}
```

**Guarantees:**

- No locks
- No allocations
- O(1) map lookup
- Readers never block writers

### Write Path

Writes go to a separate `writeMap` protected by mutex. They become visible to readers after the next sync operation.

### Sync Path

Periodic background sync:

1. Creates a new map
2. Copies non-expired entries from read map
3. Merges pending writes
4. Atomically swaps the pointer
5. Old map is garbage collected

## TTL Strategy

- Each entry has an expiration timestamp (Unix nanoseconds)
- Expired entries are **ignored on read** (lazy expiration)
- Expired entries are **removed during sync** (batch cleanup)
- No cleanup on every request (reduces overhead)
- Default sync interval: 1 minute

## Performance

### Benchmark Results

```
goos: linux
goarch: amd64
pkg: github.com/masbur/atomicache
cpu: 12th Gen Intel(R) Core(TM) i5-12600K
BenchmarkCache_Get-16                    24257065          48.25 ns/op           0 B/op          0 allocs/op
BenchmarkCache_Get_Parallel-16          276185654          4.329 ns/op           0 B/op          0 allocs/op
BenchmarkCache_Set-16                    17284814          66.70 ns/op          24 B/op          1 allocs/op
BenchmarkCache_Set_Parallel-16            4621704          264.7 ns/op          51 B/op          2 allocs/op
BenchmarkCache_MixedReadWrite-16         21921151          54.14 ns/op          16 B/op          1 allocs/op
BenchmarkMapAny_Get-16                   79652491          14.53 ns/op           0 B/op          0 allocs/op
BenchmarkMapAny_Get_Parallel-16          44235235          27.15 ns/op           0 B/op          0 allocs/op
BenchmarkBytesCache_Get-16               24803900          46.93 ns/op           0 B/op          0 allocs/op
BenchmarkBytesCache_Get_Parallel-16     270160477          4.287 ns/op           0 B/op          0 allocs/op
```

Key metrics:

- **Read latency**: ~48ns single-threaded, ~4ns parallel
- **Zero allocations** on read path
- **~5x faster** than RWMutex map in parallel reads

Run benchmarks yourself:

```bash
go test -bench=. -benchmem ./...
```

### Race Detection

```bash
go test -race ./...
```

## Limitations

1. **Write visibility delay**: Writes become visible after the next sync (configurable interval)
2. **Memory usage**: During sync, both old and new maps exist briefly
3. **No LRU eviction**: No automatic eviction based on size (TTL only)

## Best Practices

1. **Choose appropriate TTL**: Balance freshness vs cache hit rate
2. **Tune sync interval**:
   - Shorter = faster visibility, more CPU
   - Longer = better efficiency, delayed visibility
3. **Use `Sync()` sparingly**: For testing or when immediate visibility is critical
4. **Close caches**: Always call `Close()` to stop background goroutines

## API Reference

### Cache[T]

| Method                                      | Description                |
| ------------------------------------------- | -------------------------- |
| `NewCache[T](opts ...Option)`               | Create a new generic cache |
| `Get(key string) (T, bool)`                 | Get a value (lock-free)    |
| `Set(key string, val T, ttl time.Duration)` | Set a value                |
| `Delete(key string)`                        | Delete a key               |
| `Len() int`                                 | Get entry count            |
| `Sync()`                                    | Force immediate sync       |
| `Close()`                                   | Stop background sync       |

### BytesCache

| Method                                                               | Description          |
| -------------------------------------------------------------------- | -------------------- |
| `NewBytesCache(opts ...Option)`                                      | Create a bytes cache |
| `SetJSON(key string, data []byte, ttl time.Duration)`                | Set JSON data        |
| `GetOrSet(key string, ttl time.Duration, fn func() ([]byte, error))` | Get or compute       |

### Options

| Option                              | Description       | Default  |
| ----------------------------------- | ----------------- | -------- |
| `WithSyncInterval(d time.Duration)` | Set sync interval | 1 minute |

## License

MIT License
