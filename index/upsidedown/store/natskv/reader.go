package natskv

import (
	"context"
	"errors"
	"time"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/nats-io/nats.go"
)

var _ store.KVReader = (*Reader)(nil)

type Reader struct {
	date  time.Time
	store *Store
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	e, err := r.store.kv.Get(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if e.Created().After(r.date) {
		return nil, nil
	}
	return e.Value(), nil
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
		e := EncondedKey{bytes}
		strings[i] = e.encode()
	}
	return strings
}
