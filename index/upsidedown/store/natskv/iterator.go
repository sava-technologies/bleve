package natskv

import (
	"bytes"
	"sync"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/nats-io/nats.go"
)

var _ store.KVIterator = (*Iterator)(nil)

// TODO: We can save a small buffer of next
// few keys to have smaller refetches
type Iterator struct {
	store *Store

	m sync.Mutex

	keyLister nats.KeyLister
	nextCh    <-chan string
	curr      []byte
	currOk    bool

	prefix []byte
	start  []byte
	end    []byte
}

// TODO: errr handling
func (it *Iterator) Seek(k []byte) {
	if it.start != nil && bytes.Compare(k, it.start) < 0 {
		k = it.start
	}

	if it.prefix != nil && !bytes.HasPrefix(k, it.prefix) {
		if bytes.Compare(k, it.prefix) < 0 {
			k = it.prefix
		} else {
			var end []byte
			for i := len(it.prefix) - 1; i >= 0; i-- {
				c := it.prefix[i]
				if c < 0xff {
					end = make([]byte, i+1)
					copy(end, it.prefix)
					end[i] = c + 1
					break
				}
			}
			k = end
		}
	}

	it.restart(k)
}

func (it *Iterator) restart(k []byte) *Iterator {
	// TODO: errs
	keyLister, _ := it.store.natsKV.ListKeys()

	it.m.Lock()
	if it.keyLister != nil {
		_ = keyLister.Stop()
	}
	it.keyLister = keyLister // TODO:
	it.nextCh = keyLister.Keys()
	it.curr = nil
	it.currOk = false
	it.m.Unlock()

	if k != nil {
		for key := range it.nextCh {
			if it.prefix != nil && bytes.HasPrefix(k, []byte(key)) {
				it.curr = []byte(key)
				it.currOk = true
			} else if bytes.Equal(k, []byte(key)) {
				it.curr = []byte(key)
				it.currOk = true
			}
		}
	} else {
		it.keyLister.Stop()
	}

	// it.Next()

	return it
}

func (it *Iterator) Next() {
	if !it.currOk {
		return
	}
	curr, ok := <-it.nextCh
	it.curr = []byte(curr)
	it.currOk = ok
}

func (it *Iterator) Current() ([]byte, []byte, bool) {
	it.m.Lock()
	defer it.m.Unlock()
	if !it.currOk || it.curr == nil {
		return nil, nil, false
	}

	if it.prefix != nil && !bytes.HasPrefix(it.curr, it.prefix) {
		return nil, nil, false
	} else if it.end != nil && bytes.Compare(it.curr, it.end) >= 0 {
		return nil, nil, false
	}

	return it.curr, it.value(), it.currOk
}

func (it *Iterator) value() []byte {
	entry, err := it.store.natsKV.Get(string(it.curr))
	if err != nil {
		return nil
	}
	return entry.Value()
}

func (it *Iterator) Key() []byte {
	k, _, ok := it.Current()
	if !ok {
		return nil
	}
	return k
}

func (it *Iterator) Value() []byte {
	_, v, ok := it.Current()
	if !ok {
		return nil
	}
	return v
}

// / Valid returns whether or not the iterator is in a valid state
func (it *Iterator) Valid() bool {
	_, _, ok := it.Current()
	return ok
}

func (it *Iterator) Close() error {
	it.m.Lock()
	if it.keyLister != nil {
		it.keyLister.Stop()
	}
	it.keyLister = nil
	it.nextCh = nil
	it.curr = nil
	it.currOk = false
	it.m.Unlock()

	return nil
}
