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

	// Count both holders and waiters.
	entry.refs++
	t.mu.Unlock()

	if write {
		entry.mu.Lock()
	} else {
		entry.mu.RLock()
	}

	return func() {
		// Release the chunk/object lock FIRST.
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
