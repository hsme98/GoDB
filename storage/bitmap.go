package storage

import (
	"unsafe"

	"mit.edu/dsg/godb/common"
)

// Bitmap provides a convenient interface for manipulating bits in a byte slice.
// It does not own the underlying bytes; instead, it provides a structured view over
// an existing buffer (e.g., a database page).
//
// The implementation should be optimized for performance by performing word-level (uint64)
// operations during scans to skip full blocks of set bits.
type Bitmap struct {
	words   []uint64
	numBits int
}

// AsBitmap creates a Bitmap view over the provided byte slice.
//
// Constraints:
// 1. data must be aligned to 8 bytes to allow safe casting to uint64.
// 2. data must be large enough to contain numBits (rounded up to the nearest 8-byte word).
func AsBitmap(data []byte, numBits int) Bitmap {
	common.Assert(common.AlignedTo8(len(data)), "Bitmap bytes length must be aligned to 8")

	numWords := (numBits + 63) / 64
	common.Assert(len(data) >= numWords*8, "bitmap buffer too small")

	ptr := unsafe.Pointer(&data[0])
	// Slice reference cast to uint64
	words := unsafe.Slice((*uint64)(ptr), numWords)

	return Bitmap{
		words:   words,
		numBits: numBits,
	}
}

// SetBit sets the bit at index i to the given value.
// Returns the previous value of the bit.
func (b *Bitmap) SetBit(i int, on bool) (originalValue bool) {
	prev := (b.words[i/64] >> (i % 64)) & 1
	if on {
		b.words[i/64] = b.words[i/64] | (1 << (i % 64))
	} else {
		b.words[i/64] = b.words[i/64] & (^(1 << (i % 64)))
	}
	return prev != 0
}

// LoadBit returns the value of the bit at index i.
func (b *Bitmap) LoadBit(i int) bool {
	return ((b.words[i/64] >> (i % 64)) & 1) != 0
}

// FindFirstZero searches for the first bit set to 0 (false) in the bitmap.
// It begins the search at startHint and scans to the end of the bitmap.
// If no zero bit is found, it wraps around and scans from the beginning (index 0)
// up to startHint.
//
// Returns the index of the first zero bit found, or -1 if the bitmap is entirely full.
func (b *Bitmap) FindFirstZero(startHint int) int {
	if b.numBits <= 0 {
		return -1
	}
	var cword uint64
	pwind := -1
	count := 0
	for count < b.numBits {
		ind := (startHint + count) % b.numBits
		wind := ind >> 6
		if wind != pwind || ind == 0 {
			// fetch only when necessary
			cword = b.words[wind] >> (ind % 64)
			pwind = wind
			// speed up by skipping cword if it is all 1's
			if (^cword) == 0 {
				count += 64
				continue
			}
		}

		if (cword & 1) == 0 {
			return ind
		}

		cword >>= 1
		// pass this bit
		count += 1
	}

	return -1
}
