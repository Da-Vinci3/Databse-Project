# B+ Tree Database Engine

A high-performance B+ tree implementation in Go, built from scratch to understand database storage internals. This project implements the core data structures and algorithms used in modern database systems for efficient disk-based storage and retrieval.

## 🚀 Features Implemented

### Core B+ Tree Operations
- **Node Structure**: Complete page-based node implementation with headers, pointers, offsets, and key-value storage
- **Insertion**: Automatic node splitting with 2-way and 3-way split algorithms to handle overflow
- **Deletion**: Node merging and underflow handling with sibling redistribution
- **Search**: Efficient key lookup using binary search within nodes
- **Variable-length Data**: Support for keys and values of different sizes with proper offset management

### Storage Management
- **Page-based Architecture**: 4KB pages optimized for disk I/O performance
- **Copy-on-Write Semantics**: Ensures data integrity during concurrent operations
- **Memory-efficient Design**: Minimal copying with slice-based data access

### Key Technical Features
- **Configurable Limits**: Maximum key size (1KB) and value size (3KB) with validation
- **Proper Error Handling**: Comprehensive bounds checking and error propagation
- **Test Coverage**: Full test suite for all major operations
- **Clean Architecture**: Modular design separating node operations from tree logic

## 🏗️ Architecture
```go
// Core interfaces
type BTree struct {
    root    uint64
    get     func(uint64) []byte    // Page retrieval callback  
    newPage func([]byte) uint64    // Page allocation callback
    delPage func(uint64)           // Page deallocation callback
}

type BNode []byte // Page-aligned byte array for disk compatibility
```

### Node Layout
```
| type | nkeys | pointers | offsets |     key-values     | unused |
|  2B  |  2B   |   n×8B   |  n×2B   | variable length    |        |
```

## 🔧 Current Status

**✅ Completed:**
- Complete B+ tree node operations (insert, delete, search, split, merge)
- Memory management with proper allocation/deallocation callbacks
- Comprehensive test suite with edge case coverage
- Variable-length key-value storage with offset management
- Node splitting algorithms (2-way and 3-way splits)

**🚧 In Progress:**
- Full database engine implementation following "Build Your Own Database From Scratch"
- Disk-based persistence layer with Unix file I/O
- Free list management for deleted pages
- Transaction support and crash recovery

**📋 Planned Features:**
- B+ tree to table mapping
- SQL query parser and execution engine  
- Indexing and query optimization
- Concurrent access with proper locking
- Write-ahead logging (WAL) for durability

## 🛠️ Technical Implementation

Built using **Go 1.21** on **WSL2 Ubuntu** to leverage Unix system calls for optimal I/O performance.

### Key Algorithms
- **Node Lookup**: Binary search for O(log n) key location within nodes
- **Split Strategy**: Dynamic splitting based on page size constraints
- **Merge Logic**: Intelligent sibling merging with size validation
- **Memory Management**: Zero-copy operations where possible using Go slices

### Performance Characteristics
- **Page Size**: 4KB (standard database page size)
- **Node Capacity**: Variable based on key/value sizes
- **Search Complexity**: O(log n) for tree traversal + O(log k) within nodes
- **Space Efficiency**: Minimal overhead with packed node layout

## 📚 Learning Journey

This project represents a deep dive into database internals, implementing concepts typically hidden behind abstractions:

- **Storage Engine Design**: Understanding how databases organize data on disk
- **Memory Management**: Page-based allocation similar to PostgreSQL/MySQL  
- **Algorithm Implementation**: Hand-coding complex tree algorithms from academic papers
- **Systems Programming**: Low-level Go programming with careful memory management

## 🔗 Usage Example
```go
// Create a new B+ tree with custom page management
tree := &BTree{
    get: func(ptr uint64) []byte {
        // Load page from disk/memory
    },
    newPage: func(data []byte) uint64 {
        // Allocate new page, return pointer
    },
    delPage: func(ptr uint64) {
        // Deallocate page
    },
}

// Insert key-value pairs
err := tree.Insert([]byte("user:123"), []byte(`{"name": "Alice"}`))

// Retrieve values
value, found := tree.Get([]byte("user:123"))

// Delete keys
deleted, err := tree.Delete([]byte("user:123"))
```

## 🎯 Why B+ Trees?

B+ trees are the foundation of virtually every modern database system:
- **PostgreSQL**: Uses B+ trees for indexes and table storage
- **MySQL InnoDB**: Clustered indexes implemented as B+ trees  
- **SQLite**: B+ tree storage engine
- **MongoDB**: WiredTiger storage engine uses B+ trees

Understanding B+ trees means understanding how databases actually work under the hood.

## 🚀 Next Steps

1. **Complete Database Engine**: Finish implementing full database with SQL support
2. **Performance Benchmarking**: Compare against embedded databases like SQLite
3. **Concurrency**: Add proper locking mechanisms for multi-threaded access
4. **Optimization**: Implement advanced techniques like prefix compression

## 📖 References

- "Build Your Own Database From Scratch" - James Smith
- "Database System Concepts" - Silberschatz, Galvin, Gagne
- "Designing Data-Intensive Applications" - Martin Kleppmann

---

**Note**: This is an educational implementation focusing on understanding database internals. For production use, consider established solutions like SQLite, PostgreSQL, or specialized embedded databases.
