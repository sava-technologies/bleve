package natskv

import (
	"encoding/base32"
)

type EncondedKey struct {
	b []byte
}

func (k *EncondedKey) encode() string {
	return base32.StdEncoding.EncodeToString(k.b)
}

func (k *EncondedKey) decode(key string) ([]byte, error) {
	var err error
	k.b, err = base32.StdEncoding.DecodeString(key)
	return k.b, err
}
