package natskv

import (
	"errors"
	"time"

	"github.com/blevesearch/bleve/v2/registry"
	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/nats-io/nats.go"
	"github.com/olpie101/fast-forward/kv"
)

const (
	Name = "natskv"
)

var _ store.KVStore = (*Store)(nil)

type Store struct {
	kv KV
	// TODO: only used in one place, might better
	// to import only the nats kv interface?
	ffKV kv.KeyValuer[*kvByteValue]
	mo   store.MergeOperator
}

type kvByteValue struct {
	Value []byte `json:"value"`
}

func (v *kvByteValue) MarshalValue() ([]byte, error) {
	var out []byte
	if v != nil {
		out = v.Value
	}
	return out, nil
}

func (v *kvByteValue) UnmarshalValue(b []byte) error {
	v.Value = b
	return nil
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	natsKV, ok := config["nats_key_value"].(nats.KeyValue)
	if !ok {
		return nil, errors.New("config field nats_key_value required")
	}

	ffKV := kv.New[*kvByteValue](natsKV)

	s := &Store{
		kv:   KV{bucket: natsKV},
		ffKV: ffKV,
		mo:   mo,
	}

	return s, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	return &Reader{
		date:  time.Now(),
		store: s,
	}, nil

}

func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: s,
	}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
