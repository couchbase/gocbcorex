package memdx

import "sync"

// OpaqueMap implements an opaque map which guarantees a number of important
// properties, such as the ability to register and invoke handlers based on
// an opaque uint32 identifier.  It also upholds guarantees about the ordering
// and uniqueness of the handler invocations, namely that each handler is
// guaranteed to never be invoked concurrently, as well as the fact that if a
// handler returns hasMorePackets=false, it will not be called again.
// Additionally, if an error is sent to a particular handler, it is also
// guaranteed that it will not be invoked again.
type OpaqueMap struct {
	lock sync.Mutex

	counter uint32
	entries map[uint32]*opaqueMapEntry
}

func NewOpaqueMap() *OpaqueMap {
	return &OpaqueMap{
		entries: make(map[uint32]*opaqueMapEntry),
	}
}

func (m *OpaqueMap) Register(handler DispatchCallback) uint32 {
	// this function causes the handler to escape the local context and thus
	// triggers a heap allocation.  we do this outside the lock to ensure that
	// this heap allocation isn't done while the lock is held.
	entry := &opaqueMapEntry{
		handler: handler,
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.counter++
	opaqueID := m.counter

	m.entries[opaqueID] = entry

	return opaqueID
}

func (m *OpaqueMap) get(opaqueID uint32) (*opaqueMapEntry, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	entry, ok := m.entries[opaqueID]
	return entry, ok
}

func (m *OpaqueMap) getAndRemove(opaqueID uint32) (*opaqueMapEntry, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()

	entry, ok := m.entries[opaqueID]
	if !ok {
		return nil, false
	}

	delete(m.entries, opaqueID)
	return entry, true
}

func (m *OpaqueMap) remove(opaqueID uint32) {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.entries, opaqueID)
}

func (m *OpaqueMap) Invoke(opaqueID uint32, pak *Packet, err error) (bool, bool) {
	entry, ok := m.get(opaqueID)
	if !ok {
		return false, false
	}

	hasMorePackets, wasInvoked := entry.invoke(pak, err)
	if !hasMorePackets {
		m.remove(opaqueID)
	}

	return hasMorePackets, wasInvoked
}

func (m *OpaqueMap) Invalidate(opaqueID uint32) bool {
	entry, ok := m.getAndRemove(opaqueID)
	if !ok {
		return false
	}

	return entry.invalidate()
}

func (m *OpaqueMap) stealAllEntries() map[uint32]*opaqueMapEntry {
	m.lock.Lock()
	defer m.lock.Unlock()

	entries := m.entries
	m.entries = make(map[uint32]*opaqueMapEntry)

	return entries
}

func (m *OpaqueMap) CancelAll(err error) {
	entries := m.stealAllEntries()
	for _, entry := range entries {
		entry.invoke(nil, err)
	}
}

type opaqueMapEntry struct {
	lock    sync.Mutex
	handler DispatchCallback
}

func (e *opaqueMapEntry) invoke(pak *Packet, err error) (bool, bool) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.handler == nil {
		return false, false
	}

	hasMorePackets := e.handler(pak, err)
	if err != nil || !hasMorePackets {
		e.handler = nil
	}

	return hasMorePackets, true
}

func (e *opaqueMapEntry) invalidate() bool {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.handler == nil {
		return false
	}

	e.handler = nil
	return true
}
