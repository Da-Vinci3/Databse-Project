package main

import (
	"fmt"
	"os"
	"path"
	"syscall"

	"golang.org/x/sys/unix"
)

// We will be building an append only KV store, for now we will ignore concurrency and focus on sequrntial access

type KV struct {
	Path  string
	Fsync func(int) error // overriadeable for testing

	//internals hidden from the public
	fd   int
	tree BTree

	// mmap Memory Map essentially means the OS maps directly into memory
	// avoids needing read or write

	// Some benefits of this structure are:
	// Reading a page = just accessing memory (super fast)
	//The OS handles caching and flushing to disk
	//Multiple processes can share the same mapped memor
	mmap struct {
		total  int      // mmap size can be bigger than the file size
		chunks [][]byte // multiple mmaps can be continous
	}

	page struct {
		flushed uint64   // database size in page numbers (how many pages are on the disk)
		temp    [][]byte // newly allocated pages
	}
	failed bool // Did the last update fail
}

// We need to ensure atomicity and durability, so atomicity is the idea that
// data is updated or not and durability is the idea that data persists but firstly we need to
// deal with our B+ tree implementations

// BTree.new this deals with page allocation
func (db *KV) pageAppend(node []byte) uint64 {
	assert(len(node) == B_TREE_PAGE_SIZE, "THe size is not the same as a standard OS Page")
	ptr := db.page.flushed + uint64(len(db.page.temp)) // just append (next availabble page number)
	db.page.temp = append(db.page.temp, node)          // Add to temp storage
	return ptr
}

// BTree.get this deals with dat retrieval
// We convert a page number into an actual memory location
func (db *KV) pageRead(ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		// Loop through mapped chunks and find target
		//
		end := start + uint64(len(chunk))/B_TREE_PAGE_SIZE
		if ptr < end {
			offset := B_TREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+B_TREE_PAGE_SIZE] // return slice of 4KB pointing to that page
		}
		start = end
	}
	panic("Bad ptr")
}

//KV Interfaces

// Delegation pattern with dependency injection.
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key) // This is from our bplustree
}

func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db)
	if err := db.tree.Insert(key, val); err != nil {
		return err
		// Insert is also from our bplustree
	}
	return updateorRevert(db, meta) // Commit or rollback
}

func (db *KV) Del(key []byte) (bool, error) {
	meta := saveMeta(db)
	if deleted, err := db.tree.Delete(key); !deleted {
		return false, err
	}

	err := updateorRevert(db, meta) // Commit or rollback
	return err == nil, err
}

// Cleanups

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := unix.Munmap(chunk) //Unmap memory
		assert(err == nil, "We got an error")
	}

	_ = syscall.Close(syscall.Handle(db.fd)) // CLose file Descriptor
}

// The fun part our actual functions

// open or create a file and fsync the directory
// We need to fsync here for a couple of reasons we need two thing to be persistent incase of power loss
// the file data and the directory that refrences it, so we will do this preemptively when potentially creating a new file

// open or create a file and fsync the directory
func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)
	// open or create the file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	// fsync the directory
	err = syscall.Fsync(dirfd)
	if err != nil { // may leave an empty file
		_ = syscall.Close(fd)
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	// done
	return fd, nil
}
