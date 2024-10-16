package natskv

import (
	"bytes"
	"sync"
	"time"

	store "github.com/blevesearch/upsidedown_store_api"
)

var _ store.KVIterator = (*RangeIterator)(nil)

type RangeIterator struct {
	store *Store
	m     sync.Mutex

	cursor     <-chan keyValueEntry
	cursorStop func() error

	date time.Time

	key   []byte
	start []byte
	end   []byte
}

func (it *RangeIterator) inRange(value []byte) bool {
	return inRange(it.start, it.end, value)
}

func inRange(start, end, value []byte) bool {
	if start != nil {
		if bytes.Compare(value, start) < 0 {
			return false
		}
	}

	if end != nil {
		if bytes.Compare(end, value) <= 0 {
			return false
		}
	}

	return true
}

// Seek will advance the iterator to the specified key
func (it *RangeIterator) Seek(key []byte) {
	it.reset()

	if key == nil {
		key = it.start
	}

	for e := range it.cursor {
		keyAtCursor, _ := e.KeyEncoded()

		if e.Created().After(it.date) {
			continue
		}

		if !inRange(key, it.end, keyAtCursor) {
			continue
		}

		if bytes.Compare(keyAtCursor, it.start) < 0 {
			continue
		}

		it.key = keyAtCursor
		break
	}

}

func (it *RangeIterator) Next() {
	it.key = nil

	for e := range it.cursor {
		// TODO: err
		keyAtCursor, _ := e.KeyEncoded()

		if e.Created().After(it.date) {
			continue
		}

		if !it.inRange(keyAtCursor) {
			continue
		}

		it.key = keyAtCursor
		break
	}

	if it.key == nil {
		_ = it.cursorStop()
		return
	}
}

func (it *RangeIterator) Key() []byte {
	return it.key
}

func (it *RangeIterator) Valid() bool {
	return it.key != nil
}

func (it *RangeIterator) Current() ([]byte, []byte, bool) {
	it.m.Lock()
	defer it.m.Unlock()
	if it.cursor == nil || !it.Valid() {
		return nil, nil, false
	}

	return it.key, it.value(), true
}

func (it *RangeIterator) Value() []byte {
	_, v, ok := it.Current()
	if !ok {
		return nil
	}
	return v
}

func (it *RangeIterator) value() []byte {
	entry, err := it.store.kv.Get(it.key)
	if err != nil {
		return nil
	}
	return entry.Value()
}

func (it *RangeIterator) reset() {
	it.m.Lock()
	defer it.m.Unlock()

	it.key = nil
	if it.cursorStop != nil {
		_ = it.cursorStop()
	}

	keyLister, _ := it.store.kv.ListKeys()
	it.cursor = keyLister.kvEntry
	it.cursorStop = keyLister.Stop
}

func (it *RangeIterator) Close() error {
	it.m.Lock()
	defer it.m.Unlock()
	it.key = nil
	_ = it.cursorStop()

	return nil
}

// === === === === === ===

var _ store.KVIterator = (*Iterator)(nil)

type Iterator struct {
	store *Store
	m     sync.Mutex

	cursor     <-chan keyValueEntry
	cursorStop func() error

	date   time.Time
	key    []byte
	prefix []byte
}

// Seek will advance the iterator to the specified key
func (it *Iterator) Seek(key []byte) {
	it.reset()
	it.seek(key)
}

func (it *Iterator) seek(key []byte) {
	for e := range it.cursor {
		keyAtCursorBytes, _ := e.KeyEncoded()

		if e.Created().After(it.date) {
			continue
		}

		if it.prefix != nil {
			if !bytes.HasPrefix(keyAtCursorBytes, it.prefix) {
				continue
			}
		}

		if bytes.Equal(key, keyAtCursorBytes) {
			it.key = keyAtCursorBytes
			break
		}

		// This is potentially problmenatic
		// There is still an underlying assumtion that all keys are ordered
		// which chould be true??? if the _id for the document int he index
		// is always the guarrented to be in order we do not have an issue???.
		// (This statemtne makes no sense at all but it's just a future  note).
		if bytes.Compare(key, keyAtCursorBytes) < 0 {
			it.key = keyAtCursorBytes
			break
		}
	}
}

// func (it *Iterator) seekPrefix(key []byte) {
// 	// prefix := key
//
// 	lookup := it.prefix
// 	if key != nil {
// 		lookup = key
// 	}
//
// 	if bytes.HasPrefix(key, it.prefix) {
// 		for e := range it.cursor {
//
// 			if e.Created().After(it.date) {
// 				continue
// 			}
//
// 			keyAtCursorBytes := []byte(e.Key())
// 			if bytes.HasPrefix(keyAtCursorBytes, lookup) {
// 				it.key = keyAtCursorBytes
// 				break
// 			}
// 		}
// 	}
// }

func (it *Iterator) Next() {
	if it.key == nil {
		return
	}

	if it.prefix != nil {
		it.nextPrefix()
	} else {
		it.next()
	}

}

func (it *Iterator) next() {

	it.key = nil
	for e := range it.cursor {
		if e.Created().After(it.date) {
			continue
		}

		it.key, _ = e.KeyEncoded()
		break
	}
}

func (it *Iterator) nextPrefix() {
	// var value []byte

	it.key = nil
	for e := range it.cursor {
		if e.Created().After(it.date) {
			continue
		}

		keyAtCursorBytes, _ := e.KeyEncoded()
		if bytes.HasPrefix(keyAtCursorBytes, it.prefix) {
			it.key = keyAtCursorBytes
			break
		}
	}

	if it.key == nil {
		_ = it.cursorStop()
		return
	}

	// it.key = value
}

func (it *Iterator) Current() ([]byte, []byte, bool) {
	it.m.Lock()
	defer it.m.Unlock()
	if it.cursor == nil || !it.Valid() {
		return nil, nil, false
	}

	return it.key, it.value(), true
}

func (it *Iterator) Key() []byte {
	return it.key
}

func (it *Iterator) Valid() bool {
	return it.key != nil
}

func (it *Iterator) Value() []byte {
	_, v, ok := it.Current()
	if !ok {
		return nil
	}
	return v
}

func (it *Iterator) value() []byte {
	entry, err := it.store.kv.Get(it.key)
	if err != nil {
		return nil
	}
	return entry.Value()
}

func (it *Iterator) reset() {
	it.m.Lock()
	defer it.m.Unlock()

	it.key = nil
	if it.cursorStop != nil {
		_ = it.cursorStop()
	}

	keyLister, _ := it.store.kv.ListKeys()
	it.cursor = keyLister.kvEntry
	it.cursorStop = keyLister.Stop
}

func (it *Iterator) Close() error {
	it.m.Lock()
	defer it.m.Unlock()
	it.key = nil
	_ = it.cursorStop()

	return nil
}
