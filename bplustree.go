package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// database type const, it simply tells us wether the node is an internal or a leaf

/*
Sample table view
	| type | nkeys | pointers | offsets |            key-values           | unused |
	|   2  |   2   | nil nil  |  8 19   | 2 2 "k1" "hi"  2 5 "k3" "hello" |        |
	|  2B  |  2B   |   2×8B   |  2×2B   | 4B + 2B + 2B + 4B + 2B + 5B     |        |
*/

const (
	BNODE_NODE = 1 // INTERNAL NODE WITH POINTER
	BNODE_LEAF = 2 // LEAF NODE WITH VALUES
)

// constants to keep our files to a reasonable size as dealing with overflow is complex and not essential for B-Trees
const B_TREE_PAGE_SIZE = 4096 // size of one page roughly 4KB
const B_TREE_MAX_KEY_SIZE = 1000
const B_TREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := 4 + 1*8 + 1*2 + 4 + B_TREE_MAX_KEY_SIZE + B_TREE_MAX_VAL_SIZE
	// 4 = Header size (type + nkeys), 1*8 = One Pointer, 1*2 = one offset entry, 4 = Key Length + Value Length + B_TREE_MAX_KEY_SIZE + B_TREE_MAX_VAL_SIZE
	assert(node1max <= B_TREE_PAGE_SIZE, "Value is larger than maximum KV allowed") // maximum KV
}

// Decode, due to simplicity we do not need to use an in language data type for storage

type BNode []byte // This can be dumped onto the disk also unlike a struct a byte array can be of variable length

type BTree struct {
	//pointer to a non zero page number
	root uint64
	// callbacks for managing on-disk pages
	get     func(uint64) []byte // derefrence a pointer
	newPage func([]byte) uint64 // allocate a newpage
	delPage func(uint64)        //de-allocate page number

	/* Disk IO can be simulated with these callbacks. So a disk-based B+tree can easily be used as
	   an in-memory B+tree, while the reverse is not true. */
}

// getters
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2]) // readint+g our type leaf or interanl
}

func (node BNode) nkeys() uint16 {
	return binary.BigEndian.Uint16(node[2:4])
}

// Integers are stored in little endian, this is the default for most machines
// But I decided to be explicit incase of difference in machines

// setter
func (node BNode) setHeader(btype, nkeys uint16) {
	//a setter function for our header essentially sets the first two columns of our function

	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// read and write the child pointers array
func (node BNode) getPtr(idx uint16) uint64 {
	// essentially we return our pointer here along withthe rest of the data such as offsets and key-values
	assert(idx < node.nkeys(), "No such index")
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func assert(cond bool, text string) {
	if !cond {
		panic(text)
	}
}

// setPtr this essentially allows us to set our pointer
func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys(), "No such index")
	pos := 4 + 8*idx                               //essentially skip the header and set based on the idx
	binary.LittleEndian.PutUint64(node[pos:], val) // node[pos:] when used with Little Endian returns exactly 8 bytes so no need to specify an end
}

// Read KV pairs

//Read offsets array

// Using a node is just loading integers or returning sliced data. A slice is a reference, so the KV data can be passed around without copying.
// This is efficient because less copying mean less RAM used and multiple functions can acess the same key data simultaneously.

func offSetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys(), "Index of offset not in range")
	pos := 4 + 8*node.nkeys() + 2*(idx-1) // This calculation returns from offset onwards only 8 bytes though.
	//  2*(idx-1) = Find the right offset in the offsets array
	return pos
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[offSetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offSetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys(), "no such index")
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx) // this takes us to the start of our kv pairs
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys(), "no such index")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys(), "No such index")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen] // Skip lengths + key, take vlen bytes
}

// Creating Nodes

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key, val []byte) {
	// idx is the position of the item (a key, a value or a pointer)
	// ptr is the nth child pointer, which is unused for leaf nodes
	// key and val is the KV pair which is empty for internal nodes

	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx) // uses offset value of the previous key
	// 4-bytes KV sizes
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	// KV data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	// update the offset value for the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, oldSrc uint16, n uint16) {
	// Similar to the append function just for a range of values
	assert(oldSrc+n <= old.nkeys() && dstNew+n <= new.nkeys(), "Assertion failed key out of bounds")
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, oldSrc+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}

}

// node size in bytes, we can cheat here by simply using the offset value of the last key
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// find the last position that is less than or equal to the key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16
	for i = 0; i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key) //  0 if a == b, -1 if a < b ,  1 if a > b

		if cmp == 0 {
			return i
		} // This is when we find an exact match,

		if cmp > 0 {
			return i - 1
		} // node.getKey(i) > key, so previous position was our answer
	}
	return i - 1 // If we get through the whole loop without finding a larger key, we return i-1 which is the last position - meaning our search key is larger than everything in the node.
}

// insert functions
// Insert a new key-value
func leafInsert(newNode, oldNode BNode, idx uint16, key, val []byte) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys()+1)                   // one more key than before
	nodeAppendRange(newNode, oldNode, 0, 0, idx)                       // copy keys from 0 to idx - 1
	nodeAppendKV(newNode, idx, 0, key, val)                            // insert key at position
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nkeys()-idx) // copy reamining keys
}

// Update functions
// Update a key-value
func leafUpdate(newNode, oldNode BNode, idx uint16, key, val []byte) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys())                           // same number of keys
	nodeAppendRange(newNode, oldNode, 0, 0, idx)                             // copy to target idx
	nodeAppendKV(newNode, idx, 0, key, val)                                  // replace at idx
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nkeys()-(idx-1)) // copy everything remaining after
}

// Internal node pointer manipulation
// Simple pointer update
func nodeRepalceKid1Ptr(newNode, oldNode BNode, idx uint16, ptr uint64) {
	copy(newNode, oldNode[:oldNode.nbytes()]) // copy everything in the old  node
	newNode.setPtr(idx, ptr)                  // Update pointer
}

// Replace one kid with multiple
func nodeReplaceKidN(tree *BTree, newNode, oldNode BNode, idx uint16, kids ...BNode) {
	include := uint16(len(kids))
	if include == 1 && bytes.Equal(kids[0].getKey(0), oldNode.getKey(idx)) {
		// Really this happens a lot, thats why we have the simple case
		nodeRepalceKid1Ptr(newNode, oldNode, idx, tree.newPage(kids[0]))
	}

	newNode.setHeader(BNODE_NODE, oldNode.nkeys()+include-1) // add the kids and remove the replaced link
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(newNode, idx+uint16(i), tree.newPage(node), node.getKey(0), nil)
	}
	nodeAppendRange(newNode, oldNode, idx+include, idx+1, oldNode.nkeys()-(idx+1))
}

func nodeReplace2Kid(newNode, oldNode BNode, idx uint16, ptr uint64, key []byte) {
	newNode.setHeader(BNODE_NODE, oldNode.nkeys()-1)                         // take away the old key to merge
	nodeAppendRange(newNode, oldNode, 0, 0, idx)                             // Copy before
	nodeAppendKV(newNode, idx, ptr, key, nil)                                // merge new key
	nodeAppendRange(newNode, oldNode, idx+1, idx+2, oldNode.nkeys()-(idx+2)) // skip both old keys

}

// Node splitting

// When nodes grow too large we have to split them, splitting in two will guarantee that they always fit
// There is a small issue that when splitting sometimes two isn't enough for a page, it's still bigger than a page

// Node splitting in two is done with the half position as a guess then we can work left and right if one side is too big
func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2, "We don't have enough to split")
	// initial guess
	nleft := old.nkeys() / 2
	// Attempt to fit it into the left half
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > B_TREE_PAGE_SIZE {
		nleft--
	}

	assert(nleft >= 1, "the left side is not bigger") // we will try to fit it towards the right side

	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4
	}

	for right_bytes() > B_TREE_PAGE_SIZE {
		nleft++
	} // If right side is too big Move on key back to the left

	// Now we actually create the two nodes, what we just did is we split the node in half and kept bouncing
	// keys until we got a good split that works

	assert(nleft < old.nkeys(), "Left side is bigger than old tree")
	nright := old.nkeys() - nleft
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	// Appending the key ranges during our split
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	assert(right.nbytes() <= B_TREE_PAGE_SIZE, "Right side is still bigger")

	// this guarantees the right side is always fine but the left can be bigger
	// so lets split it into 3
}

// Why is this possible we have what is known as big-key-in-middle case
// because we allow a KV ro take up an entire page if needed, so what if we split it again
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= B_TREE_PAGE_SIZE {
		old = old[:B_TREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode(make([]byte, 2*B_TREE_PAGE_SIZE)) // This might get split later
	// essentially we want a really big left node for now, if we need the space
	right := BNode(make([]byte, B_TREE_PAGE_SIZE)) // This is the normal page size
	nodeSplit2(left, right, old)
	if left.nbytes() <= B_TREE_PAGE_SIZE {
		left = left[:B_TREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	} // This is if our work fits we can stop here, but
	leftleft := BNode(make([]byte, B_TREE_PAGE_SIZE))
	middle := BNode(make([]byte, B_TREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left) // If our left is still two big we split it here
	assert(leftleft.nbytes() <= B_TREE_PAGE_SIZE, "left side is still too big")
	return 3, [3]BNode{leftleft, middle, right}
	// the number tells us how many splits 1, 2 or 3 and the resulting number of nodes
	// This case could be eliminated by lowering KV size
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// We need it bigger just temporarily
	newNode := BNode(make([]byte, 2*B_TREE_PAGE_SIZE))
	// where do we insert the key
	idx := nodeLookupLE(node, key) // this gets us the position
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newNode, node, idx, key, val) // found it so we update
		} else {
			leafInsert(newNode, node, idx, key, val) // not found, insert
		}
	case BNODE_NODE: // internal node, walk into child mode
		nodeInsert(tree, newNode, node, idx, key, val)
	default:
		panic("Bad node")
	}

	return newNode
}

// Insertion and Deletion functions
func nodeInsert(tree *BTree, newNode, node BNode, idx uint16, key []byte, val []byte) {
	// recursive insertion into the kid node
	kptr := node.getPtr(idx)
	knode := treeInsert(tree, tree.get(kptr), key, val)

	//after insertion split the result
	nssplit, split := nodeSplit3(knode)
	// de-allocate  the old knode
	tree.delPage(kptr)
	// update with kid links
	nodeReplaceKidN(tree, newNode, node, idx, split[:nssplit]...)
}

// Remove the key from a leaf node
func leafDelete(newNode, oldNode BNode, idx uint16) {
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys()-1) // Because we are deleting it
	nodeAppendRange(newNode, oldNode, 0, 0, idx)     // we always copy first
	nodeAppendRange(newNode, oldNode, idx, idx+1, oldNode.nkeys()-(idx+1))
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // we didn't find anything
	}
	tree.delPage(kptr)

	new := BNode(make([]byte, B_TREE_PAGE_SIZE))
	// check if we should merge
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, B_TREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.delPage(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.newPage(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, B_TREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.delPage(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.newPage(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0, "No empty child") // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)                            // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > B_TREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - 4 // HEADER = 4
		if merged <= B_TREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}
	if idx+1 > node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - 4 // HEADER = 4
		if merged <= B_TREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}
	return 0, BNode{} // incase none of this is true we just return an empty as we do not need to merge
}

func checkLimit(key []byte, val []byte) error {
	if len(key) == 0 {
		return errors.New("empty key") // used as a dummy key
	}
	if len(key) > B_TREE_MAX_KEY_SIZE {
		return errors.New("key too long")
	}
	if len(val) > B_TREE_MAX_VAL_SIZE {
		return errors.New("value too long")
	}
	return nil
}

func nodeMerge(newNode, leftNode, rightNode BNode) {
	newNode.setHeader(leftNode.btype(), leftNode.nkeys()+rightNode.nkeys())
	nodeAppendRange(newNode, leftNode, 0, 0, leftNode.nkeys())
	nodeAppendRange(newNode, rightNode, leftNode.nkeys(), 0, leftNode.nkeys())
	assert(newNode.nbytes() <= B_TREE_PAGE_SIZE, "The data is too big for a page")
}

// Tree Deletion

// Deleting a key
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// Find the key
	idx := nodeLookupLE(node, key)
	// what we do depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // we didn't find anything
		}
		// Delete key in the leaf
		new := BNode(make([]byte, B_TREE_PAGE_SIZE)) // we always copy, ensures data integrity
		leafDelete(new, node, idx)                   // copy everything except the leaf in question
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("Bad Node!")
	}
}

// The interface
// We have build a couple of helper functions, now the main functions

func (tree *BTree) Insert(key, val []byte) error {
	if err := checkLimit(key, val); err != nil {
		return err // the check limit ensure we do not exceed our limits, its the only way this fails
	}

	if tree.root == 0 {
		// Our tree is empty sop lets make it
		root := BNode(make([]byte, B_TREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// This is a dummy key that allows the tree to cover the whole key space
		// thus lookup can always find a containing node
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.newPage(root)
		return nil
	}

	// Normal case

	node := treeInsert(tree, tree.get(tree.root), key, val) // Do the insertion
	nsplit, split := nodeSplit3(node)                       // Handle the splitting
	tree.delPage(tree.root)                                 //Delete the old root
	if nsplit > 1 {
		//root was split, so we add a new level
		root := BNode(make([]byte, B_TREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.newPage(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.newPage(root)
	} else {
		tree.root = tree.newPage(split[0]) // No split so just update the root
	}
	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	// Insert and delete are alomst mirrors of each other
	if err := checkLimit(key, nil); err != nil {
		return false, err // Same reason as above
	}

	if tree.root == 0 {
		return false, nil // The tree is empty nothing to delete
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated) == 0 {
		return false, nil //we didn't find the key requested
	}

	tree.delPage(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove a level
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.newPage(updated)
	}
	return true, nil
}

// Retrival functions

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		// Handles the empty tree case
		return nil, false
	}

	return nodeGetKey(tree, tree.get(tree.root), key)
}

func nodeGetKey(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		} else {
			return nil, false
		}
	case BNODE_NODE:
		return nodeGetKey(tree, tree.get(node.getPtr(idx)), key)
	default:
		panic("bad node!")
	}
}
