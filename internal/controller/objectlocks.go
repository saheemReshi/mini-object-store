package controller

import "sync"

type objectLockEntry struct {
	mu   sync.RWMutex
	refs int
}

type objectLockTable struct {
	mu      sync.Mutex
	entries map[string]*objectLockEntry
}

func newObjectLockTable() *objectLockTable {
	return &objectLockTable{
		entries: make(map[string]*objectLockEntry),
	}
}

// lock returns a release function that must be called exactly once. The table
// mutex protects lookup/refcounts, while the entry mutex protects the object.
func (t *objectLockTable) lock(
	id string,
	write bool,
) func() {
	t.mu.Lock()

	entry, exists := t.entries[id]
	if !exists {
		entry = &objectLockEntry{}
		t.entries[id] = entry
	}

	// Count waiters before dropping the table mutex. Otherwise the last holder
	// could remove this entry while a waiter still depends on the same lock.
	entry.refs++
	t.mu.Unlock()

	if write {
		entry.mu.Lock()
	} else {
		entry.mu.RLock()
	}

	return func() {
		// Unlock before dropping the reference; a live mutex must not disappear
		// from the table and be replaced by another mutex for the same object.
		if write {
			entry.mu.Unlock()
		} else {
			entry.mu.RUnlock()
		}

		t.mu.Lock()
		entry.refs--

		if entry.refs == 0 {
			delete(t.entries, id)
		}

		t.mu.Unlock()
	}
}
