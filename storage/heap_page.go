package storage

import (
	"mit.edu/dsg/godb/common"
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NUM_SLOTS (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	*PageFrame
}

func (hp HeapPage) NumUsed() int {
	return (int(hp.Bytes[12]) << 8) | int(hp.Bytes[13])
}

func (hp HeapPage) setNumUsed(numUsed int) {
	hp.Bytes[13] = byte(numUsed)
	hp.Bytes[12] = byte(numUsed >> 8)
}

func (hp HeapPage) NumSlots() int {
	return (int(hp.Bytes[10]) << 8) | int(hp.Bytes[11])
}

func (hp HeapPage) RowSize() int {
	return (int(hp.Bytes[8]) << 8) | int(hp.Bytes[9])
}

const SIZE_OF_BMAP = 64
const BMAP_ALLOC_OFF = 16
const BMAP_DELETE_OFF = BMAP_ALLOC_OFF + SIZE_OF_BMAP
const ROWS_OFFSET = BMAP_ALLOC_OFF + 2*SIZE_OF_BMAP

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	// We determine the number of slots denoted by x
	// number of bytes consumed by bitmaps is x/8 each
	// other than bitmaps header consumes 16 bytes
	// x  <= 4096 - 16 - x/8 - x/8
	// x  (t_size + 1/4)  <= 4080
	// x \le 4080 / (t_size + 1/4) \le
	// x/8 should be a multiple of 8 too
	// rough calc t_size <= 8 bytes
	// then x \le 4080 / ( 8 + 1/4) = 494 slots at most
	// we round to 64 bytes
	// 64 bytes for each dmap with last 2 bits are redundant

	// rowsize
	frame.Bytes[8] = byte(desc.bytesPerRow >> 8)
	frame.Bytes[9] = byte(desc.bytesPerRow)

	// numslots
	num_slots := (common.PageSize - ROWS_OFFSET) / desc.bytesPerRow
	frame.Bytes[10] = byte(num_slots >> 8)
	frame.Bytes[11] = byte(num_slots)
	// numused:
	frame.Bytes[12] = 0
	frame.Bytes[13] = 0
	// padding bytes
	frame.Bytes[14] = 0
	frame.Bytes[15] = 0

	// initialize the Alloc MAP
	curr_ind := BMAP_ALLOC_OFF
	for curr_ind < BMAP_DELETE_OFF {
		frame.Bytes[curr_ind] = 0
		curr_ind++
	}
	// initialize the Deleted MAP
	for curr_ind < ROWS_OFFSET {
		frame.Bytes[curr_ind] = 0
		curr_ind++
	}
	// rest is the rows.
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	return HeapPage{
		PageFrame: frame,
	}
}

func (hp HeapPage) FindFreeSlot() int {
	BMapAlloc := AsBitmap(hp.Bytes[BMAP_ALLOC_OFF:BMAP_DELETE_OFF], hp.NumSlots())
	return BMapAlloc.FindFirstZero(0)
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	BMapAlloc := AsBitmap(hp.Bytes[BMAP_ALLOC_OFF:BMAP_DELETE_OFF], hp.NumSlots())
	// fmt.Println(hp.RowSize(), "\t slots: ", rid.Slot, "\t num_slots:", hp.NumSlots())
	return BMapAlloc.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {

	BMapAlloc := AsBitmap(hp.Bytes[BMAP_ALLOC_OFF:BMAP_DELETE_OFF], hp.NumSlots())

	isAllocated := BMapAlloc.LoadBit(int(rid.Slot))
	if isAllocated != allocated {
		if allocated {
			hp.setNumUsed(hp.NumUsed() + 1)
		} else {
			hp.setNumUsed(hp.NumUsed() - 1)
			BMapDelete := AsBitmap(hp.Bytes[BMAP_DELETE_OFF:ROWS_OFFSET], hp.NumSlots())
			BMapDelete.SetBit(int(rid.Slot), false)
		}
		BMapAlloc.SetBit(int(rid.Slot), allocated)
	}
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	BMapDelete := AsBitmap(hp.Bytes[BMAP_DELETE_OFF:ROWS_OFFSET], hp.NumSlots())
	return BMapDelete.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	BMapDelete := AsBitmap(hp.Bytes[BMAP_DELETE_OFF:ROWS_OFFSET], hp.NumSlots())
	BMapDelete.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	row_size := int32(hp.RowSize())
	// hp.setNumUsed(hp.NumUsed() + 1)
	// fmt.Println(rid.Slot, "\trowsize:", row_size, "\tRows offset:", ROWS_OFFSET, "\t", hp.NumSlots())
	return hp.PageFrame.Bytes[ROWS_OFFSET+rid.Slot*row_size : ROWS_OFFSET+(rid.Slot+1)*row_size]
}
