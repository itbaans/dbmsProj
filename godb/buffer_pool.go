package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
	"sync"
	// "time"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

// PageLock represents a page's locking state
type PageLock struct {
	sharedLocks    map[TransactionID]bool // Transactions holding shared locks
	exclusiveLock  TransactionID          // Transaction holding exclusive lock (if any)
	waitingReaders []chan struct{}        // Channels for waiting readers
	waitingWriters []chan struct{}        // Channels for waiting writers
	mutex          sync.Mutex             // Mutex to protect lock state
}

// Updated BufferPool to include locking mechanism
type BufferPool struct {
	numPages    int
	pages       map[interface{}]Page
	pageLocks   map[interface{}]*PageLock
	bufferMutex sync.Mutex
	activeTxns  map[TransactionID]bool                   // New field to track active transactions
	waitFor     map[TransactionID]map[TransactionID]bool // Wait-for graph
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		numPages:  numPages,
		pages:     make(map[interface{}]Page),
		pageLocks: make(map[interface{}]*PageLock),
	}, nil
}

// Create or get existing page lock
func (bp *BufferPool) getOrCreatePageLock(pageKey interface{}) *PageLock {
	bp.bufferMutex.Lock()
	defer bp.bufferMutex.Unlock()

	if lock, exists := bp.pageLocks[pageKey]; exists {
		return lock
	}

	newLock := &PageLock{
		sharedLocks:    make(map[TransactionID]bool),
		waitingReaders: make([]chan struct{}, 0),
		waitingWriters: make([]chan struct{}, 0),
	}
	bp.pageLocks[pageKey] = newLock
	return newLock
}

// Attempt to acquire a lock for a page
func (pl *PageLock) acquireLock(tid TransactionID, perm RWPerm) (chan struct{}, bool) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	// Create a wait channel
	waitChan := make(chan struct{})

	// Check for read (shared) lock
	if perm == ReadPerm {
		// Can acquire read lock if no exclusive lock or if exclusive lock is held by same transaction
		if pl.exclusiveLock == 0 || pl.exclusiveLock == tid {
			pl.sharedLocks[tid] = true
			return nil, true
		}
		// Cannot acquire lock, add to waiting readers
		pl.waitingReaders = append(pl.waitingReaders, waitChan)
		return waitChan, false
	}

	// Check for write (exclusive) lock
	if perm == WritePerm {
		// Can acquire write lock only if no other locks exist
		if len(pl.sharedLocks) == 0 && pl.exclusiveLock == 0 {
			pl.exclusiveLock = tid
			return nil, true
		}
		// Can upgrade to exclusive if current transaction already has a shared lock
		if len(pl.sharedLocks) == 1 && pl.sharedLocks[tid] && pl.exclusiveLock == 0 {
			delete(pl.sharedLocks, tid)
			pl.exclusiveLock = tid
			return nil, true
		}
		// Cannot acquire lock, add to waiting writers
		pl.waitingWriters = append(pl.waitingWriters, waitChan)
		return waitChan, false
	}

	return nil, false
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	bp.bufferMutex.Lock()
	defer bp.bufferMutex.Unlock()

	// Track active transactions
	if _, exists := bp.activeTxns[tid]; exists {
		return fmt.Errorf("transaction %v is already running", tid)
	}

	// Initialize transaction tracking
	if bp.activeTxns == nil {
		bp.activeTxns = make(map[TransactionID]bool)
	}

	// Mark transaction as active
	bp.activeTxns[tid] = true
	return nil
}

func (bp *BufferPool) insertpage(file DBFile, tid TransactionID, pageNo int, page Page) error {
	pageKey := file.pageKey(pageNo) // Unique key for the page
	pageLock := bp.getOrCreatePageLock(pageKey)

	for {
		bp.bufferMutex.Lock()

		fmt.Println("Wait-For Graph:")
		for tID, dependencies := range bp.waitFor {
			fmt.Printf("Transaction %d waits for: ", tID)
			if len(dependencies) == 0 {
				fmt.Println("None")
			} else {
				for dep, isWaiting := range dependencies {
					if isWaiting {
						fmt.Printf("%d ", dep)
					}
				}
				fmt.Println()
			}
		}

		// Check for deadlock
		if bp.detectDeadlock(tid) {

			bp.bufferMutex.Unlock()
			return fmt.Errorf("deadlock detected for transaction %v", tid)
		}

		// Check if page already exists
		if _, exists := bp.pages[pageKey]; exists {
			bp.bufferMutex.Unlock()
			return fmt.Errorf("page already exists")
		}

		// Attempt to acquire lock
		waitChan, lockAcquired := pageLock.acquireLock(tid, WritePerm)

		if lockAcquired {
			// Check buffer pool capacity before inserting
			if len(bp.pages) >= bp.numPages {
				// Implement eviction logic
				evicted := false
				for key, existingPage := range bp.pages {
					if !existingPage.isDirty() {
						delete(bp.pages, key)
						delete(bp.pageLocks, key)
						evicted = true
						break
					}
				}

				if !evicted {
					bp.bufferMutex.Unlock()
					return fmt.Errorf("buffer pool full, cannot insert page")
				}
			}

			// Insert the page
			bp.pages[pageKey] = page
			bp.bufferMutex.Unlock()
			return nil
		}

		// Track wait-for relationship if lock couldn't be acquired
		blockerTid := pageLock.exclusiveLock
		if blockerTid != 0 {
			bp.addWaitFor(blockerTid, tid)
		}

		bp.bufferMutex.Unlock()

		// Wait on the channel
		<-waitChan
	}
}

// GetPage with page-level locking
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	pageKey := file.pageKey(pageNo)
	pageLock := bp.getOrCreatePageLock(pageKey)

	for {
		bp.bufferMutex.Lock()

		// Check if page is already in buffer pool
		page, pageExists := bp.pages[pageKey]

		// Attempt to acquire lock
		waitChan, lockAcquired := pageLock.acquireLock(tid, perm)
		var blockerTid TransactionID

		if lockAcquired {
			// If page doesn't exist, load from disk
			if !pageExists {
				// Page not in buffer pool, check if we have space to add it
				if len(bp.pages) >= bp.numPages {
					// Handle eviction if buffer pool is full
					evicted := false
					for key, page := range bp.pages {
						if !page.isDirty() {
							delete(bp.pages, key) // Remove a clean page
							delete(bp.pageLocks, key)
							evicted = true
							break
						}
					}
					if !evicted {
						bp.bufferMutex.Unlock()
						return nil, fmt.Errorf("all pages are dirty, cannot evict any page")
					}
				}

				// Load the page from disk
				var err error
				page, err = file.readPage(pageNo)
				if err != nil {
					bp.bufferMutex.Unlock()
					return nil, err
				}
				bp.pages[pageKey] = page
			}

			if blockerTid != 0 {
				bp.removeWaitFor(blockerTid, tid)
			}

			bp.bufferMutex.Unlock()
			return page, nil
		}

		// Track wait-for relationship if lock couldn't be acquired
		if pageExists {
			blockerTid = pageLock.exclusiveLock
			if blockerTid != 0 {
				bp.addWaitFor(blockerTid, tid)
			}
			for readLock := range pageLock.sharedLocks {
				if tid != readLock {
					bp.addWaitFor(readLock, tid)
				}
			}

			fmt.Println("Wait-For Graph:")
			for tID, dependencies := range bp.waitFor {
				fmt.Printf("Transaction %d waits for: ", tID)
				if len(dependencies) == 0 {
					fmt.Println("None")
				} else {
					for dep, isWaiting := range dependencies {
						if isWaiting {
							fmt.Printf("%d ", dep)
						}
					}
					fmt.Println()
				}
			}

			// Check for deadlock
			if bp.detectDeadlock(tid) {
				bp.bufferMutex.Unlock()
				return nil, fmt.Errorf("deadlock detected for transaction %v", tid)
			}
		}

		bp.bufferMutex.Unlock()

		// Wait on the channel
		<-waitChan
	}
}

func (bp *BufferPool) detectDeadlock(tid TransactionID) bool {

	// Perform depth-first search to detect cycles in the wait-for graph
	visited := make(map[TransactionID]bool)
	stack := []TransactionID{tid}

	for len(stack) > 0 {
		curr := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if visited[curr] {
			return true // Cycle detected
		}

		visited[curr] = true

		for waiter := range bp.waitFor[curr] {
			stack = append(stack, waiter)
		}
	}

	return false // No cycle detected
}

func (bp *BufferPool) addWaitFor(blocker, waiter TransactionID) {
	// bp.bufferMutex.Lock()
	// defer bp.bufferMutex.Unlock()

	if bp.waitFor == nil {
		bp.waitFor = make(map[TransactionID]map[TransactionID]bool)
	}

	if _, exists := bp.waitFor[blocker]; !exists {
		bp.waitFor[blocker] = make(map[TransactionID]bool)
	}

	bp.waitFor[blocker][waiter] = true
}

func (bp *BufferPool) removeWaitFor(blocker, waiter TransactionID) {

	// bp.bufferMutex.Lock()
	// defer bp.bufferMutex.Unlock()

	if waiters, exists := bp.waitFor[blocker]; exists {
		delete(waiters, waiter)
		if len(waiters) == 0 {
			delete(bp.waitFor, blocker)
		}
	}
}

func (pl *PageLock) releaseLock(tid TransactionID) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	// Remove shared locks
	delete(pl.sharedLocks, tid)

	// Clear exclusive lock if held by this transaction
	if pl.exclusiveLock == tid {
		pl.exclusiveLock = 0

		// Attempt to unblock waiting writers first
		if len(pl.waitingWriters) > 0 {
			// Signal the first waiting writer
			close(pl.waitingWriters[0])
			pl.waitingWriters = pl.waitingWriters[1:]
			return
		}

		// If no writers, try to unblock waiting readers
		for _, readerChan := range pl.waitingReaders {
			close(readerChan)
		}
		pl.waitingReaders = nil
	}
}
func (bp *BufferPool) CommitTransaction(tid TransactionID) error {
	// bp.bufferMutex.Lock()
	// defer bp.bufferMutex.Unlock()

	// Flush dirty pages for this specific transaction
	for _, page := range bp.pages {
		if page.isDirty() && page.(*heapPage).tid == tid {
			f := page.getFile()
			err := f.flushPage(page)
			if err != nil {
				return err
			}

			page.setDirty(0, false)
		}
	}

	// Release locks
	bp.AbortTransaction(tid)
	return nil
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() error {
	for _, page := range bp.pages {
		if page.isDirty() {
			f := page.getFile()
			err := f.flushPage(page) // Assuming flushPage writes to disk
			if err != nil {
				return err
			}
			page.setDirty(0, false) // Mark page as not dirty after flushing

		}
	}
	return nil
}

func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// bp.bufferMutex.Lock()
	// defer bp.bufferMutex.Unlock()

	// Remove all wait-for entries for this transaction
	for blocker, waiters := range bp.waitFor {
		if _, exists := waiters[tid]; exists {
			bp.removeWaitFor(blocker, tid)
		}
	}

	// Revert changes by removing dirty pages associated with this transaction
	for pageKey, page := range bp.pages {
		if page.(*heapPage).tid == tid {
			// Remove the page from the buffer pool
			delete(bp.pages, pageKey)
			//delete(bp.pageLocks, pageKey)
		}
	}

	// Release locks for this transaction
	for key, pageLock := range bp.pageLocks {
		pageLock.releaseLock(tid)
		delete(bp.pageLocks, key)
	}

	// Remove transaction from active transactions
	delete(bp.activeTxns, tid)
}
