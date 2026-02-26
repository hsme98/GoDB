package storage

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
)

// clockEntry holds the eviction-critical metadata for one frame, padded to one
// CPU cache line (64 bytes). Keeping this separate from PageFrame means the
// clock scanner iterates over a compact 6.4 MB array instead of touching the
// 4 KB Bytes payload in each PageFrame, eliminating DRAM-speed cache misses
// when the pool is large (e.g. 100 k frames in the liveness test).
type clockEntry struct {
	pinCount uint32
	refBit   uint32
	evicting uint32
	_        [52]byte // pad struct to exactly one cache line
}

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
//
// Design summary
//   - Fixed-size slice of frames.
//   - Concurrent page table maps PageID -> *PageFrame.
//   - Clock replacer: compact clockEntry slice (one cache line per frame) for
//     fast lock-free scanning; atomicHand advances the clock hand.
//   - Pin count, dirty, ref bit, and evicting flag are in clockEntry (scan) or
//     PageFrame (content state).
//   - PageLatch protects Bytes and page identity (pageID).
type BufferPool struct {
	numPages int
	frames   []PageFrame

	// clock is a compact array parallel to frames. The clock scanner reads only
	// this array, keeping all eviction-state cache misses in a small region.
	clock []clockEntry

	pageTable *xsync.MapOf[common.PageID, *PageFrame]

	storageManager DBFileManager
	logManager     LogManager

	// replacerMu + cond are used only on the slow path (all frames pinned).
	replacerMu sync.Mutex
	atomicHand atomic.Int64
	cond       *sync.Cond

	// Buffer reuse for FlushAllPages snapshots.
	flushBufPool sync.Pool
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	bp := &BufferPool{
		numPages:       numPages,
		frames:         make([]PageFrame, numPages),
		clock:          make([]clockEntry, numPages),
		pageTable:      xsync.NewMapOf[common.PageID, *PageFrame](),
		storageManager: storageManager,
		logManager:     logManager,
		flushBufPool: sync.Pool{New: func() any {
			b := make([]byte, common.PageSize)
			return b
		}},
	}
	bp.cond = sync.NewCond(&bp.replacerMu)
	return bp
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

// frameIdx returns the index of frame within bp.frames in O(1) via pointer
// arithmetic. The frames slice is never reallocated after construction, so the
// base address is stable for the lifetime of the pool.
func (bp *BufferPool) frameIdx(frame *PageFrame) int {
	sz := unsafe.Sizeof(bp.frames[0])
	base := uintptr(unsafe.Pointer(&bp.frames[0]))
	off := uintptr(unsafe.Pointer(frame)) - base
	return int(off / sz)
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	for {
		if frame, ok := bp.pageTable.Load(pageID); ok {
			// Cache hit path.
			//
			// We take a short RLock to ensure:
			//   - the frame is still the same page
			//   - eviction cannot repurpose the frame while we pin
			frame.PageLatch.RLock()
			idx := bp.frameIdx(frame)
			ce := &bp.clock[idx]
			if atomic.LoadUint32(&ce.evicting) != 0 || atomic.LoadUint32(&frame.valid) == 0 || frame.pageID != pageID {
				frame.PageLatch.RUnlock()
				runtime.Gosched()
				continue
			}

			atomic.AddUint32(&ce.pinCount, 1)
			atomic.StoreUint32(&ce.refBit, 1)
			frame.PageLatch.RUnlock()
			return frame, nil
		}

		// Cache miss path.
		victim, victimIdx, err := bp.acquireVictim()
		if err != nil {
			return nil, err
		}

		// victim is returned with:
		//   - clock[victimIdx].evicting = 1
		//   - victim.PageLatch held in write mode
		//   - clock[victimIdx].pinCount == 0
		//
		// Reserve the pageID in the page table to avoid duplicate loads.
		existing, loaded := bp.pageTable.LoadOrStore(pageID, victim)
		if loaded {
			// Someone else won. Release victim and retry.
			atomic.StoreUint32(&bp.clock[victimIdx].evicting, 0)
			victim.PageLatch.Unlock()
			_ = existing
			runtime.Gosched()
			continue
		}

		// We are responsible for loading pageID into victim.
		// Evict existing page if present.
		if atomic.LoadUint32(&victim.valid) != 0 {
			oldID := victim.pageID
			if atomic.LoadUint32(&victim.dirty) != 0 {
				file, ferr := bp.storageManager.GetDBFile(oldID.Oid)
				if ferr != nil {
					bp.pageTable.Delete(pageID)
					atomic.StoreUint32(&victim.valid, 0)
					atomic.StoreUint32(&bp.clock[victimIdx].evicting, 0)
					victim.PageLatch.Unlock()
					return nil, ferr
				}
				if werr := file.WritePage(int(oldID.PageNum), victim.Bytes[:]); werr != nil {
					bp.pageTable.Delete(pageID)
					atomic.StoreUint32(&victim.valid, 0)
					atomic.StoreUint32(&bp.clock[victimIdx].evicting, 0)
					victim.PageLatch.Unlock()
					return nil, werr
				}
				atomic.StoreUint32(&victim.dirty, 0)
			}
			bp.pageTable.Delete(oldID)
		}

		// Load new page into victim.
		victim.pageID = pageID
		atomic.StoreUint32(&victim.valid, 1)
		atomic.StoreUint32(&bp.clock[victimIdx].pinCount, 1)
		atomic.StoreUint32(&victim.dirty, 0)
		atomic.StoreUint32(&bp.clock[victimIdx].refBit, 0) // scan-resistant: no second chance on first load

		file, ferr := bp.storageManager.GetDBFile(pageID.Oid)
		if ferr != nil {
			bp.pageTable.Delete(pageID)
			atomic.StoreUint32(&victim.valid, 0)
			atomic.StoreUint32(&bp.clock[victimIdx].evicting, 0)
			victim.PageLatch.Unlock()
			return nil, ferr
		}
		if rerr := file.ReadPage(int(pageID.PageNum), victim.Bytes[:]); rerr != nil {
			bp.pageTable.Delete(pageID)
			atomic.StoreUint32(&victim.valid, 0)
			atomic.StoreUint32(&bp.clock[victimIdx].evicting, 0)
			victim.PageLatch.Unlock()
			return nil, rerr
		}

		atomic.StoreUint32(&bp.clock[victimIdx].evicting, 0)
		victim.PageLatch.Unlock()
		return victim, nil
	}
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	if setDirty {
		atomic.StoreUint32(&frame.dirty, 1)
		atomic.AddUint64(&frame.dirtyGen, 1)
	}

	idx := bp.frameIdx(frame)
	newCount := atomic.AddUint32(&bp.clock[idx].pinCount, ^uint32(0))
	if newCount == 0 {
		bp.replacerMu.Lock()
		bp.cond.Signal()
		bp.replacerMu.Unlock()
	}
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a Checkpoint or Shutdown to ensure durability, but also useful for tests.
func (bp *BufferPool) FlushAllPages() error {
	for i := 0; i < bp.numPages; i++ {
		frame := &bp.frames[i]
		if atomic.LoadUint32(&frame.valid) == 0 {
			continue
		}
		if atomic.LoadUint32(&frame.dirty) == 0 {
			continue
		}

		// Snapshot bytes and pageID under a read latch.
		genBefore := atomic.LoadUint64(&frame.dirtyGen)
		buf := bp.flushBufPool.Get().([]byte)
		if len(buf) != common.PageSize {
			buf = make([]byte, common.PageSize)
		}

		frame.PageLatch.RLock()
		pid := frame.pageID
		copy(buf, frame.Bytes[:])
		frame.PageLatch.RUnlock()

		file, err := bp.storageManager.GetDBFile(pid.Oid)
		if err != nil {
			bp.flushBufPool.Put(buf)
			return err
		}
		if err := file.WritePage(int(pid.PageNum), buf); err != nil {
			bp.flushBufPool.Put(buf)
			return err
		}
		bp.flushBufPool.Put(buf)

		// Clear dirty only if no new dirty event happened since the snapshot.
		if atomic.LoadUint64(&frame.dirtyGen) == genBefore {
			atomic.StoreUint32(&frame.dirty, 0)
		}
	}
	return nil
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is used by the Recovery Manager (ARIES) during the Analysis phase to reconstruct the
// state of the database.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}

// acquireVictim finds and claims an evictable frame using a lock-free clock scan
// over the compact clock array. Returns the frame (PageLatch write-locked,
// clock[idx].evicting=1, clock[idx].pinCount==0) and its index in bp.frames.
//
// The lock-free fast path scans bp.clock (compact, cache-line-sized entries) so
// metadata for all frames fits in L3 cache even for large pools. replacerMu is
// only acquired on the slow path when every frame appears to be pinned.
func (bp *BufferPool) acquireVictim() (*PageFrame, int, error) {
	numPages := bp.numPages
	frames := bp.frames
	clock := bp.clock

	for {
		var candidate *PageFrame
		candidateIdx := -1

		// Fast path: lock-free scan over the compact clock array.
		// We load the hand once, advance it locally, and store it back at the
		// end. Multiple concurrent scanners may overwrite each other's store,
		// but correctness is guaranteed by the CAS on ce.evicting.
		localHand := int(bp.atomicHand.Load())
		for scanned := 0; scanned < 2*numPages; scanned++ {
			ce := &clock[localHand]
			idx := localHand
			localHand++
			if localHand == numPages {
				localHand = 0
			}

			if atomic.LoadUint32(&ce.pinCount) != 0 {
				continue
			}

			// Second chance: atomically read-and-clear refBit.
			// If it was 1 the frame gets one more round; if 0 try to claim it.
			if atomic.SwapUint32(&ce.refBit, 0) != 0 {
				continue
			}

			if atomic.CompareAndSwapUint32(&ce.evicting, 0, 1) {
				candidate = &frames[idx]
				candidateIdx = idx
				break
			}
		}
		bp.atomicHand.Store(int64(localHand))

		if candidate == nil {
			// Slow path: all frames appear pinned. Wait for an unpin signal.
			// We recheck under the lock before waiting to avoid missing a
			// signal that fired between our scan and the Lock() call.
			bp.replacerMu.Lock()
			recheckHand := int(bp.atomicHand.Load())
			for i := 0; i < numPages && candidate == nil; i++ {
				ce := &clock[recheckHand]
				idx := recheckHand
				recheckHand++
				if recheckHand == numPages {
					recheckHand = 0
				}
				if atomic.LoadUint32(&ce.pinCount) == 0 &&
					atomic.SwapUint32(&ce.refBit, 0) == 0 &&
					atomic.CompareAndSwapUint32(&ce.evicting, 0, 1) {
					candidate = &frames[idx]
					candidateIdx = idx
				}
			}
			if candidate != nil {
				bp.atomicHand.Store(int64(recheckHand))
				bp.replacerMu.Unlock()
			} else {
				bp.cond.Wait()
				bp.replacerMu.Unlock()
				continue
			}
		}

		// Verify under the PageLatch that the frame is still unpinned.
		// pinCount could have risen between our clock scan and here.
		candidate.PageLatch.Lock()
		if atomic.LoadUint32(&bp.clock[candidateIdx].pinCount) != 0 {
			candidate.PageLatch.Unlock()
			atomic.StoreUint32(&bp.clock[candidateIdx].evicting, 0)
			continue
		}

		return candidate, candidateIdx, nil
	}
}
