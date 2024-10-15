package natskv

import "encoding/base64"

type EncondedKey struct {
	b []byte
}

func (k *EncondedKey) encode() string {
	return base64.StdEncoding.EncodeToString(k.b)
}

func (k *EncondedKey) decode(key string) ([]byte, error) {
	var err error
	k.b, err = base64.StdEncoding.DecodeString(key)
	return k.b, err
}
