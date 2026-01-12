package main

import (
	"bytes"
	"encoding/binary"
	"errors"
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

	_ = syscall.Close(db.fd) // close file descriptor
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

// Here we open or create our database
func (db *KV) Open() error {
	if db.Fsync == nil {
		// To ensure that we have a way to write to disk, if a custom method isn't defined
		// we default to the system
		db.Fsync = syscall.Fsync
	}
	var err error
	// Our B+ tree callbacks, essentially we map our functions defined in our b+ tree to our kv append
	db.tree.get = db.pageRead
	db.tree.newPage = db.pageAppend
	db.tree.delPage = func(u uint64) {}

	// Now we open or if it doesn't exist create the file
	if db.fd, err = createFileSync(db.Path); err != nil {
		return err
	}

	// get the file size, we need to know how big the files are
	finfo := syscall.Stat_t{}
	// this is a struct populated by data from Fstat
	if err = syscall.Fstat(db.fd, &finfo); err != nil {
		goto fail
	}

	// we need to create our mmap, this is super important
	if err = extendMmap(db, int(finfo.Size)); err != nil {
		goto fail
	}

	// we need to read the meta page
	if err = readRoot(db, finfo.Size); err != nil {
		goto fail
	}
	// if we make it this far we can return nil
	return nil

fail: // this is our fail func of sorts, basically rather than declaring this multiple times we do it once
	// save some typing
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)

}

const DB_SIG = "RANDOMWORDSHERE"

// Our first page is to store the root and other auxilary data, our meta page
// | sig | root_ptr | page_used |
// | 16B |    8B    |     8B    |
func loadMeta(db *KV, data []byte) {
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
}

// we need to save our meta page

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[:16], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

// We have loaded the meta page and now we should read the root
// the root is essentially a pointer to our data because theres nothing else on the root
func readRoot(db *KV, filesize int64) error {
	if filesize%B_TREE_PAGE_SIZE != 0 {
		return errors.New("Filesize is not a multiple of pages")
	}
	if filesize == 0 {
		//we do not have a file so we need to instanciate it
		db.page.flushed = 1 // meta page initialised on first write
		return nil
	}
	// if we made it this far we have data to read so lets read it
	data := db.mmap.chunks[0]
	loadMeta(db, data)

	// page verification
	// This is why we have our DBSig we need to know if we are reading the correct databse

	bad := !bytes.Equal([]byte(DB_SIG), data[:16])

	// pointers
	maxpages := uint64(filesize / B_TREE_PAGE_SIZE) // How many pages do we have
	// this simply checks the pages that a flushed and the root to make sure we have them and they are valid

	bad = bad || !(0 < db.page.flushed && db.page.flushed <= maxpages)
	bad = bad || !(0 < db.tree.root && db.tree.root <= maxpages)

	if bad {
		// if bad is true our meta page is wrong and without it well our database is an expensive paper weight
		return errors.New("bad meta page")
	}

	//other wise what we have is valid
	return nil
}

// we need our root to be atomic so we have to be able to update it
func updateRoot(db *KV) error {
	// we use pwrite which is like write but avoids offsetting the file, hence why we have 0 at the end
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("Write meta page: %w", err)
	}

	return nil
}

// extend Mmap we use mmap to avoid memory overhead and so we may need to make it bigger from time to time to mathc the filesize as it grows
func extendMmap(db *KV, size int) error {

	if size < db.mmap.total {
		// we are fine no need to add more space
		return nil
	}

	alloc := max(db.mmap.total, 64<<20) // double the current size
	for db.mmap.total+alloc < size {
		alloc *= 2 // keep doubling either we run out of space or we get mmap big enough
	}
	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	// this is essentially a read only mmap
	if err != nil {
		return fmt.Errorf("mmap err: %w", err)
	}

	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk) //append our chunk onto the chunks
	return nil
}

// we need to ensure atomicity and durability always, remeber in relation to what? Atomic in relation to our file so far
func updateFile(db *KV) error {
	// Write new nodes
	if err := writePages(db); err != nil {
		return err
	}
	// Fsync after writing before we point to it it must be durable
	if err := db.Fsync(db.fd); err != nil {
		return err
	}
	// Update the root pointer atomically
	if err := updateRoot(db); err != nil {
		return err
	}
	// fsync everything again to ensure durability, long winded but essential to ensure our data is never half written
	if err := db.Fsync(db.fd); err != nil {
		return err
	}

	return nil
}

func updateorRevert(db *KV, meta []byte) error {
	// We need to ensure what we have on disk matches in memory if we get any error
	if db.failed {
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("rewrite meta page: %w", err)
		}

		if err := db.Fsync(db.fd); err != nil {
			return err
		}

		db.failed = false
	}

	// 2 stage update here
	err := updateFile(db)
	//revert to last known safe state
	if err != nil {
		// on-disk meta page is in an unknown state
		db.failed = true
		// in-memory states arereverted immediately to allow reads
		loadMeta(db, meta)
		// discard temporaries
		db.page.temp = db.page.temp[:0]

	}
	return err
}

// lastly our writePages, its function is evident in the name it writes pages
func writePages(db *KV) error {
	// firstly we check if we need to make our mmap bigger
	size := (int(db.page.flushed) + len(db.page.temp)) * B_TREE_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}

	// write data to pages
	offset := int64(db.page.flushed * B_TREE_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}

	// discard in-memory data
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}
