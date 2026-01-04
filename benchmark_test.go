package atomicache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Unit Tests
// =============================================================================

func TestCache_SetAndGet(t *testing.T) {
	c := NewCache[string](WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	// Set a value
	c.Set("key1", "value1", 1*time.Minute)

	// Force sync to make it visible
	c.Sync()

	// Get should return the value
	val, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}
}

func TestCache_GetNonExistent(t *testing.T) {
	c := NewCache[string]()
	defer c.Close()

	val, ok := c.Get("nonexistent")
	if ok {
		t.Fatal("expected key to not exist")
	}
	if val != "" {
		t.Fatalf("expected empty string, got %s", val)
	}
}

func TestCache_TTLExpiration(t *testing.T) {
	c := NewCache[string](WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	// Set with very short TTL
	c.Set("expiring", "value", 50*time.Millisecond)
	c.Sync()

	// Should exist immediately
	if _, ok := c.Get("expiring"); !ok {
		t.Fatal("expected key to exist before expiration")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be expired now
	if _, ok := c.Get("expiring"); ok {
		t.Fatal("expected key to be expired")
	}
}

func TestCache_Delete(t *testing.T) {
	c := NewCache[string](WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	c.Set("key", "value", 1*time.Minute)
	c.Sync()

	// Verify it exists
	if _, ok := c.Get("key"); !ok {
		t.Fatal("expected key to exist")
	}

	// Delete it
	c.Delete("key")
	c.Sync()

	// Should be gone
	if _, ok := c.Get("key"); ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestCache_Len(t *testing.T) {
	c := NewCache[int](WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	for i := range 100 {
		c.Set(fmt.Sprintf("key%d", i), i, 1*time.Minute)
	}
	c.Sync()

	if c.Len() != 100 {
		t.Fatalf("expected len 100, got %d", c.Len())
	}
}

func TestCache_SyncCleansExpired(t *testing.T) {
	c := NewCache[string](WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	// Add some entries with different TTLs
	c.Set("short", "value", 50*time.Millisecond)
	c.Set("long", "value", 1*time.Minute)
	c.Sync()

	if c.Len() != 2 {
		t.Fatalf("expected len 2, got %d", c.Len())
	}

	// Wait for short TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Force sync to clean up
	c.Sync()

	// Only long-lived entry should remain
	if c.Len() != 1 {
		t.Fatalf("expected len 1 after cleanup, got %d", c.Len())
	}
}

func TestCache_ConcurrentAccess(t *testing.T) {
	c := NewCache[int](WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	var wg sync.WaitGroup
	iterations := 1000
	goroutines := 10

	// Writers
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range iterations {
				key := fmt.Sprintf("key-%d-%d", id, i)
				c.Set(key, i, 1*time.Minute)
			}
		}(g)
	}

	// Readers
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range iterations {
				key := fmt.Sprintf("key-%d-%d", id, i%100)
				c.Get(key)
			}
		}(g)
	}

	wg.Wait()
}

func TestCacheByte_SetAndGet(t *testing.T) {
	c := NewCacheByte(WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	data := []byte(`{"user":"test"}`)
	c.SetJSON("api:response", data, 1*time.Minute)
	c.Sync()

	val, ok := c.Get("api:response")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(val) != string(data) {
		t.Fatalf("expected %s, got %s", data, val)
	}
}

func TestCacheByte_GetOrSet(t *testing.T) {
	c := NewCacheByte(WithSyncInterval(10 * time.Millisecond))
	defer c.Close()

	callCount := 0
	fn := func() ([]byte, error) {
		callCount++
		return []byte("computed"), nil
	}

	// First call should invoke fn
	result, err := c.GetOrSet("key", 1*time.Minute, fn)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "computed" {
		t.Fatalf("expected computed, got %s", result)
	}
	if callCount != 1 {
		t.Fatalf("expected fn to be called once, called %d times", callCount)
	}

	// Force sync
	c.Sync()

	// Second call should use cached value
	result, err = c.GetOrSet("key", 1*time.Minute, fn)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "computed" {
		t.Fatalf("expected computed, got %s", result)
	}
	if callCount != 1 {
		t.Fatalf("expected fn to be called only once, called %d times", callCount)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkCache_Get(b *testing.B) {
	c := NewCache[string](WithSyncInterval(1 * time.Minute))
	defer c.Close()

	// Pre-populate cache
	for i := range 1000 {
		c.Set(fmt.Sprintf("key%d", i), "value", 1*time.Hour)
	}
	c.Sync()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		c.Get("key500")
	}
}

func BenchmarkCache_Get_Parallel(b *testing.B) {
	c := NewCache[string](WithSyncInterval(1 * time.Minute))
	defer c.Close()

	// Pre-populate cache
	for i := range 1000 {
		c.Set(fmt.Sprintf("key%d", i), "value", 1*time.Hour)
	}
	c.Sync()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("key500")
		}
	})
}

func BenchmarkCache_Set(b *testing.B) {
	c := NewCache[string](WithSyncInterval(1 * time.Minute))
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		c.Set("key", "value", 1*time.Hour)
	}
}

func BenchmarkCache_Set_Parallel(b *testing.B) {
	c := NewCache[string](WithSyncInterval(1 * time.Minute))
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(fmt.Sprintf("key%d", i), "value", 1*time.Hour)
			i++
		}
	})
}

func BenchmarkCache_MixedReadWrite(b *testing.B) {
	c := NewCache[string](WithSyncInterval(1 * time.Minute))
	defer c.Close()

	// Pre-populate cache
	for i := range 1000 {
		c.Set(fmt.Sprintf("key%d", i), "value", 1*time.Hour)
	}
	c.Sync()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				// 10% writes
				c.Set(fmt.Sprintf("key%d", i%1000), "newvalue", 1*time.Hour)
			} else {
				// 90% reads
				c.Get(fmt.Sprintf("key%d", i%1000))
			}
			i++
		}
	})
}

// BenchmarkMapAny compares with map[string]any for reference
func BenchmarkMapAny_Get(b *testing.B) {
	m := make(map[string]any)
	var mu sync.RWMutex

	// Pre-populate
	for i := range 1000 {
		m[fmt.Sprintf("key%d", i)] = "value"
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		mu.RLock()
		_ = m["key500"]
		mu.RUnlock()
	}
}

func BenchmarkMapAny_Get_Parallel(b *testing.B) {
	m := make(map[string]any)
	var mu sync.RWMutex

	// Pre-populate
	for i := range 1000 {
		m[fmt.Sprintf("key%d", i)] = "value"
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.RLock()
			_ = m["key500"]
			mu.RUnlock()
		}
	})
}

func BenchmarkCacheByte_Get(b *testing.B) {
	c := NewCacheByte(WithSyncInterval(1 * time.Minute))
	defer c.Close()

	data := []byte(`{"id":123,"name":"test","email":"test@example.com","active":true}`)

	// Pre-populate cache
	for i := range 1000 {
		c.Set(fmt.Sprintf("key%d", i), data, 1*time.Hour)
	}
	c.Sync()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		c.Get("key500")
	}
}

func BenchmarkCacheByte_Get_Parallel(b *testing.B) {
	c := NewCacheByte(WithSyncInterval(1 * time.Minute))
	defer c.Close()

	data := []byte(`{"id":123,"name":"test","email":"test@example.com","active":true}`)

	// Pre-populate cache
	for i := range 1000 {
		c.Set(fmt.Sprintf("key%d", i), data, 1*time.Hour)
	}
	c.Sync()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("key500")
		}
	})
}
