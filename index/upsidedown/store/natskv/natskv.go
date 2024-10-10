package natskv

import "github.com/nats-io/nats.go"

type keyLister struct {
	watcher nats.KeyWatcher
	// TODO: better interface, some values are not set
	// due to nats.WatchOpta MetaOnly
	kvEntry chan nats.KeyValueEntry
}

// ListKeys will return all keys.
func ListKeys(kv nats.KeyValue, opts ...nats.WatchOpt) (*keyLister, error) {
	opts = append(opts, nats.IgnoreDeletes(), nats.MetaOnly())
	watcher, err := kv.WatchAll(opts...)
	if err != nil {
		return nil, err
	}
	kl := &keyLister{watcher: watcher, kvEntry: make(chan nats.KeyValueEntry, 256)}

	go func() {
		defer func() {
			close(kl.kvEntry)
			_ = watcher.Stop()
		}()

		for entry := range watcher.Updates() {
			if entry == nil {
				return
			}
			kl.kvEntry <- entry
		}
	}()
	return kl, nil
}

func (kl *keyLister) Keys() <-chan nats.KeyValueEntry {
	return kl.kvEntry
}

func (kl *keyLister) Stop() error {
	return kl.watcher.Stop()
}
