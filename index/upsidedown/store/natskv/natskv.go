package natskv

import (
	"github.com/nats-io/nats.go"
)

type keyLister struct {
	watcher nats.KeyWatcher
	kvEntry chan keyValueEntry
}

type keyValueEntry struct {
	nats.KeyValueEntry
}

// TODO: handle error returned
func (e *keyValueEntry) KeyEncoded() ([]byte, error) {
	encKey := EncondedKey{}
	k := e.Key()
	r, err := encKey.decode(k)
	return r, err
}

type KV struct {
	// TODO: migrate to Jetsteam KeyValue interface
	bucket nats.KeyValue
}

func (kv *KV) Get(key []byte) (nats.KeyValueEntry, error) {
	encKey := EncondedKey{key}
	return kv.bucket.Get(encKey.encode())
}

func (kv *KV) Put(key string, value []byte) (revision uint64, err error) {
	encKey := EncondedKey{[]byte(key)}
	return kv.bucket.Put(encKey.encode(), value)
}

func (kv *KV) Delete(key string, opts ...nats.DeleteOpt) error {
	encKey := EncondedKey{[]byte(key)}
	return kv.bucket.Delete(encKey.encode(), opts...)
}

func (kv_ *KV) ListKeys(opts ...nats.WatchOpt) (*keyLister, error) {
	kv := kv_.bucket
	opts = append(opts, nats.IgnoreDeletes(), nats.MetaOnly())
	watcher, err := kv.WatchAll(opts...)
	if err != nil {
		return nil, err
	}
	kl := &keyLister{watcher: watcher, kvEntry: make(chan keyValueEntry, 256)}

	go func() {
		defer func() {
			close(kl.kvEntry)
			_ = watcher.Stop()
		}()

		for entry := range watcher.Updates() {
			if entry == nil {
				return
			}
			kl.kvEntry <- keyValueEntry{entry}
		}
	}()
	return kl, nil
}

func (kl *keyLister) Keys() <-chan keyValueEntry {
	return kl.kvEntry
}

func (kl *keyLister) Stop() error {
	return kl.watcher.Stop()
}
