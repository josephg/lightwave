package lightwavefed

import (
  "crypto/sha256"
  "encoding/hex"
  "os"
  "sort"
  "bytes"
)

const (
  hashTreeDepth = 32 * 2 // 32 byte hash in hex-encoding is 64 characters
  hashTreeNodeDegree = 16
)

// These constants are used in the reply of the HashTree.Children function.
// They detail the kind of children being returned.
const (
  // The hash tree node with the desired prefix does not exist.
  // Therefore, the list of children is empty
  HashTree_NIL = iota
  // The returned children are IDs that have been added to the hash tree
  // via HashTree.Add, i.e. these are leaves in the tree.
  HashTree_IDs
  // The returned children are the hashes of inner child nodes.
  // The list of children has the length 16.
  // At position x in the list you find the hash of the inner node which has the
  // same prefix as its parent followed by 4-bit equal to x.
  // If a list entry is empty, such an inner node does not exist currnelty.
  HashTree_InnerNodes
)

const hextable = "0123456789abcdef"

type HashTree interface {
  // The toplevel hash of the tree in hex encoding.
  // The hash is a SHA256 hash
  Hash() (hash string)
  // Adds a BLOB id to the tree. The id is a hex encoded SHA256 hash.
  Add(id string) os.Error
  // Returns the children of some inner node.
  // The kind return value determines whether the children are in turn
  // inner nodes or rather IDs added via Add().
  // The strings used here are hex encodings of SHA256 hashes.
  Children(prefix string) (kind int, children []string, err os.Error)
}

// An implementation of the HashTree interface.
// SimpleHashTree holds the entire tree in RAM.
type SimpleHashTree struct {
  hashTreeNode
}

type hashTreeNode struct {
  hash []byte
  childIDs [][]byte
  childNodes []*hashTreeNode
}

func NewSimpleHashTree() *SimpleHashTree {
  return &SimpleHashTree{}
}

func (self *SimpleHashTree) Hash() (hash string) {
  return hex.EncodeToString(self.binaryHash())
}

func (self *SimpleHashTree) Add(id string) os.Error {
  if len(id) != hashTreeDepth {
    return os.NewError("ID has the wrong length.")
  }
  bin_id, e := hex.DecodeString(id)
  if e != nil {
    return os.NewError("Malformed ID")
  }
  self.add(bin_id, 0)
  return nil
}

func (self *SimpleHashTree) Children(prefix string) (kind int, children []string, err os.Error) {
  depth := len(prefix)
  if depth >= hashTreeDepth {
    return HashTree_NIL, nil, os.NewError("Prefix is too long")
  }
  if len(prefix) % 2 == 1 {
    prefix = prefix + "0"
  }
  bin_prefix, e := hex.DecodeString(prefix)
  if e != nil {
    return HashTree_NIL, nil, os.NewError("Prefix must be a hex encoding")
  }
  kind, bin_children, err := self.children(bin_prefix, 0, depth)
  for _, bin_child := range bin_children {
    children = append(children, hex.EncodeToString(bin_child))
  }
  return
}

func (self *hashTreeNode) children(prefix []byte, level int, depth int) (kind int, children [][]byte, err os.Error) {
  // Recursion ?
  if depth > 0 {
    if self.childNodes == nil {
      return HashTree_NIL, nil, nil
    }
    index := prefix[level / 2]
    if level % 2 == 0 {
      index = index >> 4
    } else {
      index = index & 0xf
    }
    ch := self.childNodes[index]
    if ch == nil {
      return HashTree_NIL, nil, nil
    }
    return ch.children(prefix, level + 1, depth - 1)
  }
    
  if self.childNodes == nil {
    return HashTree_IDs, self.childIDs, nil
  }
  children = make([][]byte, hashTreeNodeDegree)
  for i, ch := range self.childNodes {
    if ch == nil {
      children[i] = []byte{}
    } else {
      children[i] = ch.binaryHash()
    }
  }
  kind = HashTree_InnerNodes
  return
}

func (self *hashTreeNode) add(id []byte, level int) {
  self.hash = nil
  index := id[level / 2]
  if level % 2 == 0 {
    index = index >> 4
  } else {
    index = index & 0xf
  }
  if self.childNodes != nil {
    ch := self.childNodes[index]
    if ch == nil {
      ch = &hashTreeNode{}
      self.childNodes[index] = ch
    }
    ch.add(id, level + 1)
  } else {
    self.childIDs = append(self.childIDs, id)
    if len(self.childIDs) <= hashTreeNodeDegree {
      return
    }
    self.childNodes = make([]*hashTreeNode, hashTreeNodeDegree)
    for _, hash := range self.childIDs {
      i := hash[level / 2]
      if level % 2 == 0 {
	i = i >> 4
      } else {
	i = i & 0xf
      }
      ch := self.childNodes[i]
      if ch == nil {
	ch = &hashTreeNode{}
	self.childNodes[i] = ch
      }
      ch.add(hash, level + 1)
    }
    self.childIDs = nil
  }
}

func (self *hashTreeNode) binaryHash() []byte {
  if len(self.hash) != 0 {
    return self.hash
  }
  h := sha256.New()
  if len(self.childNodes) > 0 {
    for _, child := range self.childNodes {
      if child != nil {
	h.Write(child.binaryHash())
      }
    }
  } else {
    sortBytesArray(self.childIDs)
    for _, hash := range self.childIDs {
      h.Write([]byte(hash))
    }
  }
  self.hash = h.Sum()
  return self.hash
}

// ------------------------------------------
// Helpers


type bytesArray [][]byte

func (p bytesArray) Len() int {
  return len(p)
}

func (p bytesArray) Less(i, j int) bool {
    return bytes.Compare(p[i], p[j]) == -1
}

func (p bytesArray) Swap(i, j int) {
  p[i], p[j] = p[j], p[i]
}

func sortBytesArray(arr [][]byte) {
  sort.Sort(bytesArray(arr))
}
  
// Helper function
func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}
