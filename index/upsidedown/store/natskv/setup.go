package natskv

import (
	"testing"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type testSetup struct {
	rv       store.KVStore
	kv       nats.KeyValue
	teardown func()
}

func open(t *testing.T, mo store.MergeOperator) testSetup {
	nc, teardown, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	jsc, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	kv, err := jsc.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  "test",
		Storage: nats.MemoryStorage,
	})
	if err != nil {
		t.Fatal(err)
	}

	rv, err := New(mo, map[string]interface{}{
		"nats_key_value": kv,
	})
	if err != nil {
		t.Fatal(err)
	}
	return testSetup{
		rv:       rv,
		kv:       kv,
		teardown: teardown,
	}
}

func setup() (nc *nats.Conn, teardown func(), err error) {
	var ns *server.Server
	ns, nc, err = newNatsServerWithConn()
	if err != nil {
		return
	}
	teardown = func() {
		ns.Shutdown()
		nc.Close()
	}
	return
}

func newNatsServerWithConn() (*server.Server, *nats.Conn, error) {
	ns, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
	})
	if err != nil {
		return nil, nil, err
	}
	ns.Start()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		return nil, nil, err
	}
	return ns, nc, nil
}
