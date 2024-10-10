package natskv

import "testing"

func TestIterator(t *testing.T) {
	{

		ri := RangeIterator{
			store: nil,
			start: []byte{'4'},
		}

		_, ok := ri.inRange([]byte{'0'})
		if ok {
			t.Errorf("expected not ok")
		}

		_, ok = ri.inRange([]byte{'3'})
		if ok {
			t.Errorf("expected not ok")
		}

		_, ok = ri.inRange([]byte{'4'})
		if !ok {
			t.Errorf("expected ok")
		}

		_, ok = ri.inRange([]byte("999999"))
		if !ok {
			t.Errorf("expected ok")
		}
	}

	{
		ri := RangeIterator{
			store: nil,
			end:   []byte{'4'},
		}

		_, ok := ri.inRange([]byte{'0'})
		if !ok {
			t.Errorf("expected ok")
		}

		_, ok = ri.inRange([]byte{'1'})
		if !ok {
			t.Errorf("expected ok")
		}

		_, ok = ri.inRange([]byte("999999"))
		if ok {
			t.Errorf("expected not ok")
		}
	}
}
