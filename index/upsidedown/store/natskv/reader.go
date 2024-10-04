package natskv

import (
	"context"

	store "github.com/blevesearch/upsidedown_store_api"
)

var _ store.KVReader = (*Reader)(nil)

type Reader struct {
	store *Store
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	ctx := context.Background()
	kvResult, _, err := r.store.ffKV.Get(ctx, string(key))
	if err != nil {
		return nil, err
	}
	return kvResult.Value, nil
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	ctx := context.Background()
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
		store:  r.store,
		prefix: prefix,
	}
	it.restart(prefix)
	return it
}

// RangeIterator returns a KVIterator that will
// visit all K/V pairs >= start AND < end
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	it := &Iterator{
		store: r.store,
		start: start,
		end:   end,
	}
	it.restart(start)
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
