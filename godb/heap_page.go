package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type HeapRecordID struct {
	PageID int
	Slot   int
}

// Implement the recordID interface methods for HeapRecordID here.
// For instance, if recordID requires methods like GetPageID and GetSlot, implement them:
func (r *HeapRecordID) GetPageID() int {
	return r.PageID
}

func (r *HeapRecordID) GetSlot() int {
	return r.Slot
}

type heapPage struct {
	pageID    int
	tuples    []*Tuple      // Array to store tuples
	numSlots  int           // Total number of slots
	usedSlots int           // Number of used slots
	is_dirty  bool          // Dirty flag
	tid       TransactionID // Transaction ID for dirty page
	file      *HeapFile     // Reference to the HeapFile
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	// Calculate the number of slots based on the tuple size and page size
	t_size := 0
	for _, field := range desc.Fields {
		switch field.Ftype {
		case IntType:
			t_size += 8
		case StringType:
			t_size += 32
		default:
			// Handle unknown or unsupported types
			panic(fmt.Sprintf("unsupported type: %v", field.Ftype))
		}
	}
	remPageSize := PageSize - 8 // Assume 8 bytes are used for header
	numSlots := remPageSize / t_size

	return &heapPage{
		pageID:    pageNo,
		numSlots:  numSlots,
		usedSlots: 0,
		tuples:    make([]*Tuple, numSlots),
		is_dirty:  false,
		file:      f,
	}, nil
}

func (h *heapPage) getNumSlots() int {
	return h.numSlots
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	for i := 0; i < h.numSlots; i++ {
		if h.tuples[i] == nil { // Found an empty slot
			h.tuples[i] = t
			h.usedSlots++
			t.Rid = &HeapRecordID{PageID: h.pageID, Slot: i}
			return t.Rid, nil // Return as recordID
		}
	}
	return nil, fmt.Errorf("no available slot")
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	heapRid, ok := rid.(*HeapRecordID)
	if !ok || heapRid.Slot >= h.numSlots || h.tuples[heapRid.Slot] == nil {
		return fmt.Errorf("invalid recordID or slot is already empty")
	}
	h.tuples[heapRid.Slot] = nil
	h.usedSlots--
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.is_dirty
}

func (h *heapPage) getTID() TransactionID {
	return (h.tid)
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.is_dirty = dirty // Mark the page as dirty or clean
	if dirty {
		h.tid = tid // Associate this dirty page with the modifying transaction
	} else {
		h.tid = 0 // Clear the transaction ID if the page is not dirty
	}
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	return p.file
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int32(h.numSlots))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, int32(h.usedSlots))
	if err != nil {
		return nil, err
	}
	// Write tuples to buffer (assuming Tuple has WriteTo method)
	for _, tuple := range h.tuples {
		if tuple != nil {
			err := tuple.writeTo(buf)
			if err != nil {
				return nil, err
			}
		}
	}

	// Pad buffer to PageSize if necessary
	remaining := PageSize - buf.Len()
	if remaining > 0 {
		err := binary.Write(buf, binary.LittleEndian, make([]byte, remaining))
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var numSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &numSlots); err != nil {
		fmt.Println("Error1 :", err)
		return fmt.Errorf("error reading number of slots: %w", err)
	}
	var usedSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &usedSlots); err != nil {
		fmt.Println("Error2:", err)
		return fmt.Errorf("error reading number of slots: %w", err)
	}
	h.numSlots = int(numSlots)
	h.usedSlots = int(usedSlots)

	// fmt.Println(numSlots)
	// fmt.Println(usedSlots)
	//fmt.Println("in")
	h.tuples = make([]*Tuple, h.numSlots)
	//fmt.Println("out")
	// Read tuples from buffer
	//fmt.Println(h.file)
	for i := 0; i < h.usedSlots; i++ {
		tuple, err := readTupleFrom(buf, h.file.tupleDesc)
		if err != nil {
			fmt.Println("Error3:", err)
			return fmt.Errorf("error reading number of slots: %w", err)
		}
		h.tuples[i] = tuple
	}
	return nil
}

// func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
// 	// Read header
// 	if err := binary.Read(buf, binary.LittleEndian, &h.numSlots); err != nil {
// 		return fmt.Errorf("error reading number of slots: %w", err)
// 	}
// 	if err := binary.Read(buf, binary.LittleEndian, &h.usedSlots); err != nil {
// 		return fmt.Errorf("error reading used slots: %w", err)
// 	}

// 	// Read tuples
// 	h.tuples = make([]*Tuple, h.numSlots)
// 	for i := 0; i < h.numSlots; i++ {
// 		t := new(Tuple)
// 		if err := t.readFrom(buf, h.desc); err != nil {
// 			return fmt.Errorf("error reading tuple at slot %d: %w", i, err)
// 		}
// 		if t != nil && t.isValid() {
// 			h.tuples[i] = t
// 		}
// 	}

// 	return nil
// }

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (h *heapPage) tupleIter() func() (*Tuple, error) {
	i := 0
	return func() (*Tuple, error) {
		for i < len(h.tuples) {
			tuple := h.tuples[i]
			i++
			if tuple != nil {
				// Set the tuple's recordID to a compatible type
				tuple.Rid = &HeapRecordID{PageID: h.pageID, Slot: i - 1}
				return tuple, nil
			}
		}
		return nil, nil
	}
}
