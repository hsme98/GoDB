package execution

import (
	"errors"
	"sync"
	"sync/atomic"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  storage.LogManager
	lockManager *transaction.LockManager

	// mu serializes insert operations (finding a free slot / allocating new
	// pages). Read, update, delete, and scan operations are protected only
	// by per-page PageLatches, so they do not acquire mu.
	mu sync.Mutex
	// numPages is the number of heap pages currently in the file.
	// Written only under mu (by InsertTuple); read atomically everywhere else.
	numPages atomic.Int32
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager storage.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	types := make([]common.Type, len(table.Columns))
	for i, col := range table.Columns {
		types[i] = col.Type
	}
	desc := storage.NewRawTupleDesc(types)

	// GetDBFile creates the file if it does not yet exist.
	file, err := bufferPool.StorageManager().GetDBFile(table.Oid)
	if err != nil {
		return nil, err
	}
	n, err := file.NumPages()
	if err != nil {
		return nil, err
	}

	th := &TableHeap{
		oid:         table.Oid,
		desc:        desc,
		bufferPool:  bufferPool,
		logManager:  logManager,
		lockManager: lockManager,
	}
	th.numPages.Store(int32(n))
	return th, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	tableHeap.mu.Lock()
	defer tableHeap.mu.Unlock()

	// Fast path: try the last existing page.
	if tableHeap.numPages.Load() > 0 {
		lastPageNum := tableHeap.numPages.Load() - 1
		pid := common.PageID{Oid: tableHeap.oid, PageNum: lastPageNum}

		page, err := tableHeap.bufferPool.GetPage(pid)
		if err != nil {
			return common.RecordID{}, err
		}
		page.PageLatch.Lock()
		hp := page.AsHeapPage()
		slot := hp.FindFreeSlot()
		if slot >= 0 {
			rid := common.RecordID{PageID: pid, Slot: int32(slot)}
			hp.MarkAllocated(rid, true)
			copy(hp.AccessTuple(rid), row)
			page.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(page, true)
			return rid, nil
		}
		page.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(page, false)
	}

	// Slow path: the last page is full (or the file is empty). Allocate a new page.
	file, err := tableHeap.bufferPool.StorageManager().GetDBFile(tableHeap.oid)
	if err != nil {
		return common.RecordID{}, err
	}
	firstPage, err := file.AllocatePage(1)
	if err != nil {
		return common.RecordID{}, err
	}

	newPid := common.PageID{Oid: tableHeap.oid, PageNum: int32(firstPage)}
	page, err := tableHeap.bufferPool.GetPage(newPid)
	if err != nil {
		return common.RecordID{}, err
	}

	page.PageLatch.Lock()
	storage.InitializeHeapPage(tableHeap.desc, page)
	tableHeap.numPages.Store(int32(firstPage + 1))
	hp := page.AsHeapPage()
	slot := hp.FindFreeSlot()
	if slot < 0 {
		page.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(page, true)
		return common.RecordID{}, errors.New("internal error: newly initialized page has no free slot")
	}
	rid := common.RecordID{PageID: newPid, Slot: int32(slot)}
	hp.MarkAllocated(rid, true)
	copy(hp.AccessTuple(rid), row)
	page.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(page, true)
	return rid, nil
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	page, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	page.PageLatch.Lock()
	hp := page.AsHeapPage()

	if !hp.IsAllocated(rid) || hp.IsDeleted(rid) {
		page.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(page, false)
		return ErrTupleDeleted
	}

	hp.MarkDeleted(rid, true)
	page.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	page, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	if forUpdate {
		page.PageLatch.Lock()
	} else {
		page.PageLatch.RLock()
	}
	hp := page.AsHeapPage()

	if !hp.IsAllocated(rid) || hp.IsDeleted(rid) {
		if forUpdate {
			page.PageLatch.Unlock()
		} else {
			page.PageLatch.RUnlock()
		}
		tableHeap.bufferPool.UnpinPage(page, false)
		return ErrTupleDeleted
	}

	copy(buffer, hp.AccessTuple(rid))
	if forUpdate {
		page.PageLatch.Unlock()
	} else {
		page.PageLatch.RUnlock()
	}
	tableHeap.bufferPool.UnpinPage(page, false)
	return nil
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	page, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	page.PageLatch.Lock()
	hp := page.AsHeapPage()

	if !hp.IsAllocated(rid) || hp.IsDeleted(rid) {
		page.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(page, false)
		return ErrTupleDeleted
	}

	copy(hp.AccessTuple(rid), updatedTuple)
	page.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	page, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return err
	}

	page.PageLatch.Lock()
	hp := page.AsHeapPage()
	numSlots := hp.NumSlots()

	for slot := 0; slot < numSlots; slot++ {
		rid := common.RecordID{PageID: pageID, Slot: int32(slot)}
		// In Lab 1 there are no active transactions, so we reclaim all deleted slots.
		if hp.IsAllocated(rid) && hp.IsDeleted(rid) {
			hp.MarkAllocated(rid, false) // also clears the deleted bit (see MarkAllocated)
		}
	}

	page.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	// Snapshot numPages so the iterator sees a consistent prefix of pages.
	// Pages allocated after this point are not visited by this iterator, which
	// is the correct point-in-time semantics.
	n := tableHeap.numPages.Load()
	return TableHeapIterator{
		heap:     tableHeap,
		numPages: n,
		buffer:   buffer,
	}, nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
// It holds at most one page pinned at a time; the pin is released when advancing to a new page.
type TableHeapIterator struct {
	heap     *TableHeap
	numPages int32  // snapshot of the page count at creation time
	buffer   []byte // caller-supplied reuse buffer for zero-allocation scanning

	pageNum  int32              // page currently being scanned
	slot     int32              // next slot index to examine on pageNum
	curFrame *storage.PageFrame // currently pinned page (nil when between pages)
	curRID   common.RecordID    // RID of the last tuple returned by Next()
	err      error
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	return it.heap == nil
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	for {
		// Load the current page if we don't have one pinned.
		if it.curFrame == nil {
			if it.pageNum >= it.numPages {
				return false
			}
			pid := common.PageID{Oid: it.heap.oid, PageNum: it.pageNum}
			frame, err := it.heap.bufferPool.GetPage(pid)
			if err != nil {
				it.err = err
				return false
			}
			it.curFrame = frame
		}

		// Scan remaining slots on the current page.
		it.curFrame.PageLatch.RLock()
		hp := it.curFrame.AsHeapPage()
		numSlots := int32(hp.NumSlots())

		for it.slot < numSlots {
			slot := it.slot
			it.slot++
			rid := common.RecordID{
				PageID: common.PageID{Oid: it.heap.oid, PageNum: it.pageNum},
				Slot:   slot,
			}
			if hp.IsAllocated(rid) && !hp.IsDeleted(rid) {
				// Copy bytes into caller's buffer before releasing the latch.
				copy(it.buffer, hp.AccessTuple(rid))
				it.curRID = rid
				it.curFrame.PageLatch.RUnlock()
				return true
			}
		}
		it.curFrame.PageLatch.RUnlock()

		// Current page exhausted — advance to the next one.
		it.heap.bufferPool.UnpinPage(it.curFrame, false)
		it.curFrame = nil
		it.pageNum++
		it.slot = 0
	}
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	return storage.RawTuple(it.buffer[:it.heap.desc.BytesPerTuple()])
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	return it.curRID
}

// Error returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	return it.err
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	if it.curFrame != nil {
		it.heap.bufferPool.UnpinPage(it.curFrame, false)
		it.curFrame = nil
	}
	return nil
}

// Ensure atomic.Int32 methods are compiled away if unused (they're used via Load/Store).
var _ = atomic.Int32{}
