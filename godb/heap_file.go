package godb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	bufPool   *BufferPool // Reference to the buffer pool (already present)
	filename  string      // Name/path of the file storing the heap data
	numPages  int         // Number of pages in the file
	pageSize  int         // Size of each page in bytes
	file      *os.File    // Handle to the actual file on disk
	tupleDesc *TupleDesc
	//mutex sync.RWMutex    // For concurrent access control
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// Open or create the file
	var file *os.File
	var err error

	if _, err := os.Stat(fromFile); os.IsNotExist(err) {
		// Create a new file if it does not exist
		file, err = os.Create(fromFile)
		if err != nil {
			//fmt.Println("Error creating file:", err)
			return nil, err
		}
	} else {
		// Open the existing file
		file, err = os.OpenFile(fromFile, os.O_RDWR, 0666)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return nil, err
		}
	}

	// Initialize the HeapFile struct
	heapFile := &HeapFile{
		filename:  fromFile,
		bufPool:   bp,
		tupleDesc: td,
		pageSize:  PageSize,
		file:      file,
	}

	// Get the number of pages in the file
	info, err := file.Stat()

	if err != nil {
		file.Close() // ensure file is closed if there's an error
		fmt.Println("Error creating file:", err)
		return nil, err
	}

	heapFile.numPages = int(info.Size() / int64(PageSize))

	//fmt.Println(heapFile.filename)
	return heapFile, nil

}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	return f.filename
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	return f.numPages
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.CommitTransaction(tid)

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	// Open the file for reading
	file, err := os.Open(f.filename)
	if err != nil {
		//fmt.Println("ff")
		return nil, err
	}
	defer file.Close()

	// Calculate the offset to seek to the page number
	offset := int64(pageNo) * int64(PageSize)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	// Read the page data into a buffer
	pageData := make([]byte, PageSize)

	_, err = file.Read(pageData)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Page data: %x\n", pageData[:100])
	//fmt.Println(pageData)
	// Create a new heapPage and initialize it with the buffer
	hp := &heapPage{
		// Assuming you have initialized fields specific to heapPage here, like pageNo, etc.
		pageID: pageNo,
		file:   f,
	}
	buffer := bytes.NewBuffer(pageData)

	err = hp.initFromBuffer(buffer)
	if err != nil {
		return nil, err
	}

	return hp, nil
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {

	pageNo := 0

	for ; pageNo < f.numPages; pageNo++ {
		//fmt.Println(pageNo)
		page, err := f.bufPool.GetPage(f, pageNo, tid, 1)
		//fmt.Println("test1")

		if err != nil {
			return err
		}

		heapPage, ok := page.(*heapPage)
		//fmt.Println("test2")
		if !ok {
			return fmt.Errorf("unexpected page type")
		}
		//fmt.Println(heapPage.usedSlots)
		if heapPage.getNumSlots() > heapPage.usedSlots {
			rid, err := heapPage.insertTuple(t)
			//fmt.Println("test3")
			if err != nil {
				return err
			}
			t.Rid = rid
			heapPage.setDirty(tid, true)
			//fmt.Println("test4")
			return nil
		}

	}
	// If all pages are full, create a new page and insert

	//fmt.Println("hree")
	newPage, err := newHeapPage(f.tupleDesc, pageNo, f)
	//fmt.Println("test5")
	if err != nil {
		return err
	}
	rid, err := newPage.insertTuple(t)
	//fmt.Println("test6")
	if err != nil {
		return err
	}

	t.Rid = rid
	newPage.setDirty(tid, true)
	//fmt.Println("test7")
	f.numPages++

	nPage, err := newHeapPage(&t.Desc, pageNo, f)
	nPage.insertTuple(t)
	nPage.setDirty(tid, true)

	if err != nil {
		return err
	}

	f.bufPool.insertpage(f, tid, pageNo, nPage)

	return nil

}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	rid, ok := t.Rid.(*HeapRecordID)
	if !ok {
		return errors.New("invalid RID type")
	}

	page, err := f.bufPool.GetPage(f, rid.PageID, tid, 1)
	if err != nil {
		return err
	}

	heapPage, ok := page.(*heapPage)
	if !ok {
		return errors.New("invalid page type")
	}

	err = heapPage.deleteTuple(rid)
	if err != nil {
		return err
	}

	heapPage.setDirty(tid, true)

	return nil
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	heapPage, ok := p.(*heapPage)

	if !ok {
		//fmt.Println("test1")
		return errors.New("invalid page type")
	}

	data, err := heapPage.toBuffer()
	if err != nil {
		//fmt.Println("test2")
		return err
	}

	offset := int64(heapPage.pageID) * int64(PageSize)
	_, err = f.file.Seek(offset, 0)
	if err != nil {
		return err
	}

	_, err = f.file.Write(data.Bytes()) // Write data at the correct position
	if err != nil {
		return err
	}

	err = f.file.Sync() // Ensure data is flushed to disk
	if err != nil {
		return err
	}

	heapPage.setDirty(0, true)
	//fmt.Println("CLEAR HP")
	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.tupleDesc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	var currentPageNo int
	var currentPage *heapPage
	var iter func() (*Tuple, error)
	//var currentTupleIndex int

	next := func() (*Tuple, error) {
		// Initialize first page if needed
		if currentPage == nil {
			var err error
			currentPage, err = f.nextPage(tid, currentPageNo)
			if err != nil {
				if errors.Is(err, io.EOF) { // Check if the error is of type EOF
					return nil, nil // Return nil, nil if EOF is reached
				}
				return nil, err // Return other errors as is
			}
			iter = currentPage.tupleIter()
		}

		// Continue searching from current position
		for {
			// Search current page
			tuple, err := iter()
			if err != nil {
				return nil, err
			}
			if tuple != nil {
				tuple.Desc = *f.tupleDesc
				return tuple, nil
			}

			// Move to next page
			currentPageNo++

			currentPage, err = f.nextPage(tid, currentPageNo)
			if err != nil {
				// Return EOF or other errors
				return nil, nil
			}
			iter = currentPage.tupleIter()
		}
	}

	return next, nil
}

func (f *HeapFile) nextPage(tid TransactionID, pageNo int) (*heapPage, error) {
	page, err := f.bufPool.GetPage(f, pageNo, tid, 0)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, err
		}
		return nil, err
	}

	heapPage, ok := page.(*heapPage)
	if !ok {
		return nil, errors.New("invalid page type")
	}

	return heapPage, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{FileName: f.filename, PageNo: pgNo}
}
