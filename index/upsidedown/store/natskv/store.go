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
	kvName string
	kv     KV
	// TODO: only used in one place, might better
	// to completelty use natsKV interface
	ffKV kv.KeyValuer[*kvByteValue]
	mo   store.MergeOperator
	// TODO: only pass in kv?
	nc *nats.Conn
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
	nc, ok := config["nats_conn"].(*nats.Conn)
	if !ok || nc == nil {
		return nil, errors.New("config field nats_conn required")
	}

	// TODO: use jetstream api
	jsc, ok := config["jsc"].(nats.JetStreamContext)
	if !ok || jsc == nil {
		return nil, errors.New("config field jsc required")
	}
	// jetstream.KeyValue

	kvName, ok := config["kv_name"].(string)
	if !ok || kvName == "" {
		return nil, errors.New("config field kv_name required")
	}

	natsKV, err := jsc.KeyValue(kvName)
	if err != nil {
		return nil, err
	}

	ffKV := kv.New[*kvByteValue](natsKV)

	s := &Store{
		kvName: kvName,
		kv:     KV{bucket: natsKV},
		ffKV:   ffKV,
		nc:     nc,
		mo:     mo,
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
