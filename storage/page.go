package storage

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"mit.edu/dsg/godb/common"
)

// pageOffsetLSN is the byte offset of the LSN within the page.
const pageOffsetLSN = 0

// PageFrame represents a physical page of data in memory.
// It holds the raw bytes of the page and acts as the container for Buffer Pool management.
type PageFrame struct {
	// Bytes holds the raw physical data of the page.
	Bytes [common.PageSize]byte
	// PageLatch protects the content of the page from concurrent access.
	PageLatch sync.RWMutex

	// Buffer pool metadata.
	//
	// These fields are intentionally unexported. Tests and higher layers should
	// only rely on Bytes and PageLatch.
	pageID common.PageID

	// valid indicates whether this frame currently holds a real page.
	// 0 means free, 1 means contains a page.
	valid uint32

	// pinCount is the number of active users holding this page.
	// Must be atomic because it is touched by multiple goroutines.
	pinCount uint32

	// dirty marks whether Bytes differ from disk.
	dirty uint32

	// dirtyGen increments each time the page is marked dirty, used to avoid
	// clearing dirty in FlushAllPages when concurrent updates happen.
	dirtyGen uint64

	// refBit is the clock reference bit (0 or 1).
	refBit uint32

	// evicting is a claim flag to prevent multiple evictors from selecting the
	// same frame concurrently.
	evicting uint32
}

// Detect system endianness -- compiler should statically replace this with a constant
var isBigEndian = func() bool {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xCAFE)
	return buf[0] == 0xCA
}()

// LSN atomically reads the Log Sequence Number from the page header.
func (frame *PageFrame) LSN() LSN {
	ptr := (*uint64)(unsafe.Pointer(&frame.Bytes[pageOffsetLSN]))
	val := atomic.LoadUint64(ptr)
	if isBigEndian {
		val = bits.ReverseBytes64(val)
	}
	return LSN(val)
}

// MonotonicallyUpdateLSN atomically updates the LSN. The update is atomic and is only applied if the given lsn is
// larger than the current value.
func (frame *PageFrame) MonotonicallyUpdateLSN(lsn LSN) {
	ptr := (*uint64)(unsafe.Pointer(&frame.Bytes[pageOffsetLSN]))
	newVal := uint64(lsn)

	for {
		rawCurrent := atomic.LoadUint64(ptr)
		logicalCurrent := rawCurrent
		if isBigEndian {
			logicalCurrent = bits.ReverseBytes64(rawCurrent)
		}

		if newVal <= logicalCurrent {
			return
		}

		rawNew := newVal
		if isBigEndian {
			rawNew = bits.ReverseBytes64(newVal)
		}

		if atomic.CompareAndSwapUint64(ptr, rawCurrent, rawNew) {
			return
		}
	}
}
