package lightwavefed

import (
  "crypto/sha256"
  "encoding/hex"
  "os"
//  "log"
  "sort"
  "bytes"
//  "json"
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
  Hash() (hash string, err os.Error)
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

/*
func NewHashTree(hashes [][]byte) *HashTree{
  ht := &HashTree{}
  for _, hash := range hashes {
    ht.Add(hash)
  }
  return ht
}
*/

func (self *SimpleHashTree) Hash() (hash string, err os.Error) {
  return hex.EncodeToString(self.binaryHash()), nil
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

// ---------------------------------------------
// Compare two hash trees

// Compares two hash trees and sends all BLOB ids that are not common to both trees
// on the respective channels. The strings returned are hex encoded SHA256 hashes or
// prefixes thereof. In case of a prefix, all BLOBS who's IDs match the prefix are not
// in both trees.
func CompareHashTrees(tree1, tree2 HashTree) (onlyIn1, onlyIn2 <-chan string) {
  ch1 := make(chan string)
  ch2 := make(chan string)
  go compareHashTrees(tree1, tree2, "", ch1, ch2)
  return ch1, ch2
}

func compareHashTrees(tree1, tree2 HashTree, prefix string, onlyIn1, onlyIn2 chan<- string) {
  if len(prefix) == 0 {
    defer close(onlyIn1)
    defer close(onlyIn2)
    // The trees are equal?
    h1, err1 := tree1.Hash()
    h2, err2 := tree2.Hash()
    if err1 != nil || err2 != nil {
      return
    }
    if h1 == h2 {
      return
    }
  }
  
  kind1, children1, err1 := tree1.Children(prefix)
  kind2, children2, err2 := tree2.Children(prefix)
  if kind1 == HashTree_NIL || kind2 == HashTree_NIL || err1 != nil || err2 != nil {
    return
  }
  
  // Turn a list of strings into a map of strings for further efficient processing
  map1 := map[string]bool{}
  for _, ch := range children1 {
    map1[ch] = true
  }
  map2 := map[string]bool{}
  for _, ch := range children2 {
    map2[ch] = true
  }
  
  if kind1 == HashTree_IDs && kind2 == HashTree_IDs {
    // Both returned hashes. Compare the two sets of hashes
    for key, _ := range map1 {
      if _, ok := map2[key]; !ok {
	onlyIn1 <- key
      }
    }
    for key, _ := range map2 {
      if _, ok := map1[key]; !ok {
	onlyIn2 <- key
      }
    }
  } else if kind1 == HashTree_InnerNodes && kind2 == HashTree_InnerNodes {
    // Both returned subtree nodes? Recursion into the sub tree nodes
    for i := 0; i < hashTreeNodeDegree; i++ {
      if children1[i] == children2[i] {
	continue
      }
      if children1[i] == "" {
	onlyIn2 <- prefix + string(hextable[i])
      } else if children2[i] == "" {
	onlyIn1 <- prefix + string(hextable[i])
      } else {
	compareHashTrees(tree1, tree2, prefix + string(hextable[i]), onlyIn1, onlyIn2)
      }
    }
  } else if kind1 == HashTree_InnerNodes && kind2 == HashTree_IDs {
    for i := 0; i < hashTreeNodeDegree; i++ {
      compareHashTreeWithList(tree1, map2, prefix + string(hextable[i]), onlyIn1, onlyIn2)
      for id, _ := range map2 {
	onlyIn2 <- id
      }
    }
  } else {
    for i := 0; i < hashTreeNodeDegree; i++ {
      compareHashTreeWithList(tree2, map1, prefix + string(hextable[i]), onlyIn2, onlyIn1)
      for id, _ := range map1 {
	onlyIn1 <- id
      }
    }  
  }
}

func compareHashTreeWithList(tree1 HashTree, list map[string]bool, prefix string, onlyIn1, onlyIn2 chan<- string) {
  kind1, children1, err := tree1.Children(prefix)
  if len(children1) == 0 || kind1 == HashTree_NIL || err != nil {
    return
  }
  
  // Turn a list of strings into a map of strings for further efficient processing
  map1 := map[string]bool{}
  for _, ch := range children1 {
    map1[ch] = true
  }

  if kind1 == HashTree_IDs {
    // Both returned hashes. Compare the two sets of hashes
    for key, _ := range map1 {
      if _, ok := list[key]; !ok {
	onlyIn1 <- key
      } else {
	list[key] = false, false
      }
    }
  } else {
    // Both returned subtree nodes? Recursion into the sub tree nodes
    for i := 0; i < hashTreeNodeDegree; i++ {
      compareHashTreeWithList(tree1, list, prefix + string(hextable[i]), onlyIn1, onlyIn2)
    }
  }
}

// ------------------------------------------
// Talk to a hash tree on another computer

func HashTreeHandler(tree HashTree) RequestHandler {
  return func(req *Message) (status int, data interface{}) {
    switch req.Cmd {
    case "THASH":
      hash, _ := tree.Hash()
      return 200, &struct{Hash string "hash"}{hash}
    case "TCHLD":
      query := struct{Prefix string "prefix"}{}
      if req.DecodePayload(query) != nil {
	return 400, nil
      }
      reply := &struct{Kind int "kind"; Children []string "chld"}{}
      reply.Kind, reply.Children, _ = tree.Children(query.Prefix)
      return 200, reply
    }
    return 400, nil
  }
}

// Provides remote access to a HashTree on another computer.
// It uses a Connection to talk to the remote HashTree.
// The purpose of RemoteHashTree is to compare the content of
// two blob servers in order of synchronizing their content.
type RemoteHashTree struct {
  conn *Connection
}

func NewRemoteHashTree(conn *Connection) *RemoteHashTree {
  return &RemoteHashTree{conn: conn}
}

func (self *RemoteHashTree) Hash() (hash string, err os.Error) {
  resp := &struct{Hash string "hash"}{}
  status, err := self.conn.SendRequest("THASH", nil, resp)
  if status != 200 {
    return "", os.NewError("Unexpected status code")
  }
  return resp.Hash, nil
}

func (self *RemoteHashTree) Add(id string) os.Error {
  return os.NewError("Adding is not allowed on a remote hash tree")
}

func (self *RemoteHashTree) Children(prefix string) (kind int, children []string, err os.Error) {
  resp := &struct{Kind int "kind"; Children []string "chld"}{}
  status, err := self.conn.SendRequest("TCHLD", map[string]interface{}{"prefix": prefix}, resp)
  if status != 200 {
    err = os.NewError("Wrong status code")
    return
  }
  return resp.Kind, resp.Children, nil
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