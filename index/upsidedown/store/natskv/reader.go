package natskv

import (
	"context"
	"time"

	store "github.com/blevesearch/upsidedown_store_api"
)

var _ store.KVReader = (*Reader)(nil)

type Reader struct {
	date  time.Time
	store *Store
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	e, err := r.store.natsKV.Get(string(key))
	if err != nil {
		return nil, err
	}
	if e.Created().After(r.date) {
		return nil, nil
	}
	return e.Value(), nil
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	ctx := context.Background()

	// TODO: read isolation timestamps/date after
	kvResults, err := r.store.ffKV.GetAll(ctx, byteSlicesToStrings(keys))
	if err != nil {
		return nil, err
	}

	values := make([][]byte, len(kvResults))
	for i := range kvResults {
		values[i] = kvResults[i].Value
	}
	return values, nil
}

// PrefixIterator returns a KVIterator that will
// visit all K/V pairs with the provided prefix
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	it := &Iterator{
		date:   r.date,
		store:  r.store,
		prefix: prefix,
	}
	it.reset()
	it.nextPrefix()
	return it
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	it := &RangeIterator{
		date:  r.date,
		store: r.store,
		start: start,
		end:   end,
	}
	it.Seek(nil)
	return it
}

func (r *Reader) Close() error {
	return nil
}

func byteSlicesToStrings(byteSlices [][]byte) []string {
	strings := make([]string, len(byteSlices))
	for i, bytes := range byteSlices {
		strings[i] = string(bytes)
	}
	return strings
}
