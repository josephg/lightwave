package lightwavefed

import (
  "os"
  "crypto/sha256"
  "encoding/hex"
  "log"
  "sort"
)

const (
  HashTreeDepth = 32 * 2 // 32 byte hash in hex-encoding is 64 characters
  HashTreeNodeDegree = 16
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

func (self *HashTree) PrefixHash(prefix string) (hash string, childIDs []string, err os.Error) {
  depth := len(prefix)
  if len(prefix) % 2 == 1 {
    prefix = prefix + "0"
  }
  bin_prefix, e := hex.DecodeString(prefix)
  if e != nil {
    err = os.NewError("Malformed prefix. Must be a hex encoding.")
    return
  }
  h, children := self.prefixHash(bin_prefix, 0, depth)
  hash = hex.EncodeToString(h)
  if children_hashes, ok := children.([][]byte); ok {
    for _, ch := range children_hashes {
      childIDs = append(childIDs, hex.EncodeToString(ch))
    }
  } else if children_indexes, ok := children.([]int); ok {
    for _, ch := range children_indexes {
      childIDs = append(childIDs, string(hextable[ch]))
    }
  }
  return
}

func (self *hashTreeNode) prefixHash(prefix []byte, level int, depth int) (hash []byte, children interface{}) {
  if depth > 0 {
    // Recursion
    index := prefix[level / 2]
    if level % 2 == 0 {
      index = index >> 4
    } else {
      index = index & 0xf
    }
    if self.childNodes == nil {
      return self.Hash(), nil
    }
    ch := self.childNodes[index]
    if ch == nil {
      return self.Hash(), nil
    }
    return ch.prefixHash(prefix, level + 1, depth - 1)
  }
  
  if self.childNodes != nil {
    indexes := []int{}
    for i, ch := range self.childNodes {
      if ch != nil {
	indexes = append(indexes, i)
      }
    }
    return self.Hash(), indexes
  }
  return self.Hash(), self.childIDs
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
  }
  
  h1, children1, _ := tree1.PrefixHash(prefix)
  h2, children2, _ := tree2.PrefixHash(prefix)
  if h1 == h2 {
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
  
  if (len(children1) == 0 || len(children1[0]) == 64) && (len(children2) == 0 || len(children2[0]) == 64) {
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
  } else if (len(children1) == 0 || len(children1[0]) == 1) && (len(children2) == 0 || len(children2[0]) == 1) {
    // Both returned subtree nodes? Recursion into the sub tree nodes
    for key, _ := range map1 {
      if _, ok := map2[key]; !ok {
	onlyIn1 <- prefix + key
      } else {
	compareHashTrees(tree1, tree2, prefix + key, onlyIn1, onlyIn2)
      }
    }
    for key, _ := range map2 {
      if _, ok := map1[key]; !ok {
	onlyIn2 <- prefix + key
      }
    }
  } else if (len(children1) > 0 || len(children1[0]) == 1) && (len(children2) > 0 || len(children2[0]) == 64) {
    for key, _ := range map1 {
      compareHashTreeWithList(tree1, map2, prefix + key, onlyIn1, onlyIn2)
    }
  } else if (len(children1) > 0 || len(children1[0]) == 64) && (len(children2) > 0 || len(children2[0]) == 1) {
    for key, _ := range map2 {
      compareHashTreeWithList(tree2, map1, prefix + key, onlyIn2, onlyIn1)
    }
  } else {
    log.Printf("One tree returned a malformed hash")
  }
}

func compareHashTreeWithList(tree1 *HashTree, list map[string]bool, prefix string, onlyIn1, onlyIn2 chan<- string) {
  _, children1, _ := tree1.PrefixHash(prefix)
  if len(children1) == 0 {
    return
  }
  
  // Turn a list of strings into a map of strings for further efficient processing
  map1 := map[string]bool{}
  for _, ch := range children1 {
    map1[ch] = true
  }

  if len(children1[0]) == 64 {
    // Both returned hashes. Compare the two sets of hashes
    for key, _ := range map1 {
      if _, ok := list[key]; !ok {
	onlyIn1 <- key
      }
    }
    for key, _ := range list {
      if _, ok := map1[key]; !ok {
	onlyIn2 <- key
      }
    }
  } else if len(children1[0]) == 1 {
    // Both returned subtree nodes? Recursion into the sub tree nodes
    for key, _ := range map1 {
      compareHashTreeWithList(tree1, list, prefix + key, onlyIn1, onlyIn2)
    }
  } else {
    log.Printf("One tree returned a malformed hash")
  }
}

// ------------------------------------------
// Helpers

type BytesArray [][]byte

func (p BytesArray) Len() int {
  return len(p)
}

func (p BytesArray) Less(i, j int) bool {
  l := min(len(p[i]), len(p[j]))
  for pos := 0; pos < l; pos++ {
    if p[i][pos] < p[j][pos] {
      return true
    }
    if p[i][pos] > p[j][pos] {
      return false
    }
  }
  if len(p[i]) < len(p[j]) {
    return true
  }
  return false
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