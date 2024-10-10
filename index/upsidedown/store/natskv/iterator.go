package natskv

import (
	"bytes"
	"sync"

	store "github.com/blevesearch/upsidedown_store_api"
)

var _ store.KVIterator = (*RangeIterator)(nil)

type RangeIterator struct {
	store *Store
	m     sync.Mutex

	cursor     <-chan string
	cursorStop func() error

	key   []byte
	start []byte
	end   []byte
}

// TODO: remove first return
// return starting bytes
func (it *RangeIterator) inRange(value []byte) ([]byte, bool) {

	if it.start != nil {
		if bytes.Compare(value, it.start) < 0 {
			return it.start, false
		}
	}

	if it.end != nil {
		if bytes.Compare(it.end, value) < 0 {
			return it.end, false
		}
	}

	return value, true
}

// Seek will advance the iterator to the specified key
func (it *RangeIterator) Seek(key []byte) {
	it.reset()

	_, ok := it.inRange(key)
	if !ok {
		return
	}

	for keyAtCursor := range it.cursor {
		keyAtCursor := []byte(keyAtCursor)

		_, ok := it.inRange(keyAtCursor)
		if !ok {
			continue
		}

		it.key = keyAtCursor
		break
	}

}

func (it *RangeIterator) Next() {
	var value []byte

	for keyAtCursor := range it.cursor {
		keyAtCursor := []byte(keyAtCursor)

		_, ok := it.inRange(keyAtCursor)
		if !ok {
			continue
		}

		value = keyAtCursor
		break
	}

	if value == nil {
		it.key = nil
		_ = it.cursorStop()
		return
	}

	it.key = value
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
	if it.cursor == nil {
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
	entry, err := it.store.natsKV.Get(string(it.key))
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

	keyLister, _ := it.store.natsKV.ListKeys()
	it.cursor = keyLister.Keys()
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

	cursor     <-chan string
	cursorStop func() error

	key    []byte
	prefix []byte
}

// Seek will advance the iterator to the specified key
func (it *Iterator) Seek(key []byte) {

	it.reset()
	if it.prefix != nil {
		it.seekPrefix(key)
	} else {
		it.seek(key)
	}

}

func (it *Iterator) seek(key []byte) {
	for keyAtCursor := range it.cursor {
		keyAtCursorBytes := []byte(keyAtCursor)
		if bytes.Equal(key, keyAtCursorBytes) {
			it.key = keyAtCursorBytes
			break
		}
	}
}

func (it *Iterator) seekPrefix(key []byte) {
	if bytes.HasPrefix(key, it.prefix) {
		for keyAtCursor := range it.cursor {
			keyAtCursorBytes := []byte(keyAtCursor)
			if bytes.HasPrefix(key, keyAtCursorBytes) {
				it.key = keyAtCursorBytes
				break
			}
		}
	}
}

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
	value, ok := <-it.cursor
	if !ok {
		it.key = nil
		_ = it.cursorStop()
		return
	}

	it.key = []byte(value)
}

func (it *Iterator) nextPrefix() {
	var value []byte

	for keyAtCursor := range it.cursor {
		keyAtCursorBytes := []byte(keyAtCursor)
		if bytes.HasPrefix(keyAtCursorBytes, it.prefix) {
			value = keyAtCursorBytes
			break
		}
	}

	if value == nil {
		it.key = nil
		_ = it.cursorStop()
		return
	}

	it.key = value
}

func (it *Iterator) Current() ([]byte, []byte, bool) {
	it.m.Lock()
	defer it.m.Unlock()
	if it.cursor == nil {
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
	entry, err := it.store.natsKV.Get(string(it.key))
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

	keyLister, _ := it.store.natsKV.ListKeys()
	it.cursor = keyLister.Keys()
	it.cursorStop = keyLister.Stop
}

func (it *Iterator) Close() error {
	it.m.Lock()
	defer it.m.Unlock()
	it.key = nil
	_ = it.cursorStop()

	return nil
}

// var _ store.KVIterator = (*Iterator)(nil)

// type Iterator struct {
// 	store *Store
//
// 	m sync.Mutex
//
// 	keyLister nats.KeyLister
// 	nextCh    <-chan string
// 	curr      []byte
// 	currOk    bool
//
// 	prefix []byte
// 	start  []byte
// 	end    []byte
// }
//
// // TODO: errr handling
// func (it *Iterator) Seek(k []byte) {
// 	fmt.Println(">> seek", k)
//
// 	if it.start != nil && bytes.Compare(k, it.start) < 0 {
// 		k = it.start
// 	}
//
// 	if it.prefix != nil && !bytes.HasPrefix(k, it.prefix) {
// 		if bytes.Compare(k, it.prefix) < 0 {
// 			k = it.prefix
// 		} else {
// 			var end []byte
// 			for i := len(it.prefix) - 1; i >= 0; i-- {
// 				c := it.prefix[i]
// 				if c < 0xff {
// 					end = make([]byte, i+1)
// 					copy(end, it.prefix)
// 					end[i] = c + 1
// 					break
// 				}
// 			}
// 			k = end
// 		}
// 	}
//
// 	it.restart(k)
// }
//
// func (it *Iterator) restart(k []byte) *Iterator {
// 	// TODO: errs
// 	keyLister, _ := it.store.natsKV.ListKeys()
//
// 	it.m.Lock()
// 	if it.keyLister != nil {
// 		_ = keyLister.Stop()
// 	}
// 	it.keyLister = keyLister // TODO:
// 	it.nextCh = keyLister.Keys()
// 	it.curr = nil
// 	it.currOk = false
// 	it.m.Unlock()
//
// 	if k != nil {
// 		for key := range it.nextCh {
// 			fmt.Println(">>", key)
// 			if it.prefix != nil && bytes.HasPrefix(k, []byte(key)) {
// 				it.curr = []byte(key)
// 				it.currOk = true
// 				break
// 			} else if bytes.Equal(k, []byte(key)) {
// 				it.curr = []byte(key)
// 				it.currOk = true
// 				break
// 			}
// 		}
// 	} else {
// 		it.keyLister.Stop()
// 	}
//
// 	// it.Next()
//
// 	return it
// }
//
// func (it *Iterator) Next() {
// 	if !it.currOk {
// 		return
// 	}
// 	curr, ok := <-it.nextCh
// 	it.curr = []byte(curr)
// 	it.currOk = ok
// }
//
// func (it *Iterator) Current() ([]byte, []byte, bool) {
// 	it.m.Lock()
// 	defer it.m.Unlock()
// 	if !it.currOk || it.curr == nil {
// 		return nil, nil, false
// 	}
//
// 	if it.prefix != nil && !bytes.HasPrefix(it.curr, it.prefix) {
// 		return nil, nil, false
// 	} else if it.end != nil && bytes.Compare(it.curr, it.end) >= 0 {
// 		return nil, nil, false
// 	}
//
// 	return it.curr, it.value(), it.currOk
// }
//
// func (it *Iterator) value() []byte {
// 	entry, err := it.store.natsKV.Get(string(it.curr))
// 	if err != nil {
// 		return nil
// 	}
// 	return entry.Value()
// }
//
// func (it *Iterator) Key() []byte {
// 	k, _, ok := it.Current()
// 	if !ok {
// 		return nil
// 	}
// 	return k
// }
//
// func (it *Iterator) Value() []byte {
// 	_, v, ok := it.Current()
// 	if !ok {
// 		return nil
// 	}
// 	return v
// }
//
// // / Valid returns whether or not the iterator is in a valid state
// func (it *Iterator) Valid() bool {
// 	_, _, ok := it.Current()
// 	return ok
// }
//
// func (it *Iterator) Close() error {
// 	it.m.Lock()
// 	if it.keyLister != nil {
// 		it.keyLister.Stop()
// 	}
// 	it.keyLister = nil
// 	it.nextCh = nil
// 	it.curr = nil
// 	it.currOk = false
// 	it.m.Unlock()
//
// 	return nil
// }
