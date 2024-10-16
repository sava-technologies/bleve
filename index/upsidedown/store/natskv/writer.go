package natskv

import (
	"errors"
	"fmt"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/nats-io/nats.go"
)

type Writer struct {
	store *Store
}

var _ store.KVWriter = (*Writer)(nil)

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.store.mo)
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) error {
	var err error
	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	bucket := w.store.kv

	for k, mergeOps := range emulatedBatch.Merger.Merges {
		kb := []byte(k)

		var existingVal []byte
		entry, err := bucket.Get(kb)
		if err != nil {
			if !errors.Is(err, nats.ErrKeyNotFound) {
				return err
			}
		} else {
			existingVal = entry.Value()
		}

		mergedVal, fullMergeOk := w.store.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		_, err = bucket.Put(k, mergedVal)
		if err != nil {
			return err
		}
	}

	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			_, err = bucket.Put(string(op.K), op.V)
			if err != nil {
				return err
			}
		} else {
			err = bucket.Delete(string(op.K))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *Writer) Close() error {
	return nil
}
