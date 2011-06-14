package lightwavefed

import (
  "crypto/sha256"
  "encoding/hex"
//  "log"
  "sort"
  "bytes"
)

const (
  HashTreeDepth = 32 * 2 // 32 byte hash in hex-encoding is 64 characters
  HashTreeNodeDegree = 16
)

const (
  HashTree_NIL = iota
  HashTree_IDs
  HashTree_InnerNodes
)

const hextable = "0123456789abcdef"

type HashTree struct {
  hashTreeNode
}

type hashTreeNode struct {
  hash []byte
  childIDs [][]byte
  childNodes []*hashTreeNode
}

func Build(hashes [][]byte) *HashTree{
  ht := &HashTree{}
  for _, hash := range hashes {
    ht.Add(hash)
  }
  return ht
}

func (self *HashTree) Add(id []byte) {
  self.add(id, 0)
}

func (self *HashTree) Children(prefix string) (kind int, children []string) {
  depth := len(prefix)
  if len(prefix) % 2 == 1 {
    prefix = prefix + "0"
  }
  bin_prefix, e := hex.DecodeString(prefix)
  if e != nil {
    return HashTree_NIL, nil
  }
  kind, bin_children := self.children(bin_prefix, 0, depth)
  for _, bin_child := range bin_children {
    children = append(children, hex.EncodeToString(bin_child))
  }
  return
}

func (self *hashTreeNode) children(prefix []byte, level int, depth int) (kind int, children [][]byte) {
  // Recursion ?
  if depth > 0 {
    if self.childNodes == nil {
      return HashTree_NIL, nil
    }
    index := prefix[level / 2]
    if level % 2 == 0 {
      index = index >> 4
    } else {
      index = index & 0xf
    }
    ch := self.childNodes[index]
    if ch == nil {
      return HashTree_NIL, nil
    }
    return ch.children(prefix, level + 1, depth - 1)
  }
    
  if self.childNodes == nil {
    return HashTree_IDs, self.childIDs
  }
  children = make([][]byte, HashTreeNodeDegree)
  for i, ch := range self.childNodes {
    if ch == nil {
      children[i] = []byte{}
    } else {
      children[i] = ch.Hash()
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
    if len(self.childIDs) <= HashTreeNodeDegree {
      return
    }
    self.childNodes = make([]*hashTreeNode, HashTreeNodeDegree)
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

func (self *hashTreeNode) Hash() []byte {
  if len(self.hash) != 0 {
    return self.hash
  }
  h := sha256.New()
  if len(self.childNodes) > 0 {
    for _, child := range self.childNodes {
      if child != nil {
	h.Write(child.Hash())
      }
    }
  } else {
    SortBytesArray(self.childIDs)
    for _, hash := range self.childIDs {
      h.Write([]byte(hash))
    }
  }
  self.hash = h.Sum()
  return self.hash
}

// ---------------------------------------------
// Compare two hash trees

func CompareHashTrees(tree1 *HashTree, tree2 *HashTree) (onlyIn1, onlyIn2 <-chan string) {
  ch1 := make(chan string)
  ch2 := make(chan string)
  go compareHashTrees(tree1, tree2, "", ch1, ch2)
  return ch1, ch2
}

func compareHashTrees(tree1, tree2 *HashTree, prefix string, onlyIn1, onlyIn2 chan<- string) {
  if len(prefix) == 0 {
    defer close(onlyIn1)
    defer close(onlyIn2)
    // The trees are equal? 
    if bytes.Compare(tree1.Hash(), tree2.Hash()) == 0 {
      return
    }
  }
  
  kind1, children1 := tree1.Children(prefix)
  kind2, children2 := tree2.Children(prefix)
  
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
    for i := 0; i < HashTreeNodeDegree; i++ {
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
    for i := 0; i < HashTreeNodeDegree; i++ {
      compareHashTreeWithList(tree1, map2, prefix + string(hextable[i]), onlyIn1, onlyIn2)
      for id, _ := range map2 {
	onlyIn2 <- id
      }
    }
  } else {
    for i := 0; i < HashTreeNodeDegree; i++ {
      compareHashTreeWithList(tree2, map1, prefix + string(hextable[i]), onlyIn2, onlyIn1)
      for id, _ := range map1 {
	onlyIn1 <- id
      }
    }  
  }
}

func compareHashTreeWithList(tree1 *HashTree, list map[string]bool, prefix string, onlyIn1, onlyIn2 chan<- string) {
  kind1, children1 := tree1.Children(prefix)
  if len(children1) == 0 {
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
    for i := 0; i < HashTreeNodeDegree; i++ {
      compareHashTreeWithList(tree1, list, prefix + string(hextable[i]), onlyIn1, onlyIn2)
    }
  }
}

// ------------------------------------------
// Helpers

type BytesArray [][]byte

func (p BytesArray) Len() int {
  return len(p)
}

func (p BytesArray) Less(i, j int) bool {
    return bytes.Compare(p[i], p[j]) == -1
}

func (p BytesArray) Swap(i, j int) {
  p[i], p[j] = p[j], p[i]
}

func SortBytesArray(arr [][]byte) {
  sort.Sort(BytesArray(arr))
}
  
// Helper function
func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}