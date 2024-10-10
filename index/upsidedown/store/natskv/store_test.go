package natskv

import (
	"testing"

	"github.com/blevesearch/upsidedown_store_api/test"
)

// PASSED
func TestBoltDBKVCrud(t *testing.T) {
	ss := open(t, nil)
	defer ss.teardown()
	test.CommonTestKVCrud(t, ss.rv)
}

func TestBoltDBReaderIsolation(t *testing.T) {
	ss := open(t, nil)
	defer ss.teardown()
	test.CommonTestReaderIsolation(t, ss.rv)
}

// func TestBoltDBReaderOwnsGetBytes(t *testing.T) {
// 	s := open(t, nil)
// 	defer s.teardown()
// 	test.CommonTestReaderOwnsGetBytes(t, s.rv)
// }
//
// func TestBoltDBWriterOwnsBytes(t *testing.T) {
// 	s := open(t, nil)
// 	defer s.teardown()
// 	test.CommonTestWriterOwnsBytes(t, s.rv)
// }

//
//	func TestBoltDBPrefixIterator(t *testing.T) {
//		s := open(t, nil)
//		defer s.teardown()
//		test.CommonTestPrefixIterator(t, s.rv)
//	}
//
//	func TestBoltDBPrefixIteratorSeek(t *testing.T) {
//		s := open(t, nil)
//		defer s.teardown()
//		test.CommonTestPrefixIteratorSeek(t, s.rv)
//	}
//
// //
// // func TestBoltDBRangeIterator(t *testing.T) {
// // 	s := open(t, nil)
// // 	defer cleanup(t, s)
// // 	test.CommonTestRangeIterator(t, s)
// // }
// //
// // func TestBoltDBRangeIteratorSeek(t *testing.T) {
// // 	s := open(t, nil)
// // 	defer cleanup(t, s)
// // 	test.CommonTestRangeIteratorSeek(t, s)
// // }
// //
// // func TestBoltDBMerge(t *testing.T) {
// // 	s := open(t, &test.TestMergeCounter{})
// // 	defer cleanup(t, s)
// // 	test.CommonTestMerge(t, s)
// // }
