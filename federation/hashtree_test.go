package lightwavefed

import (
  "crypto/sha256"
  "encoding/hex"
  "testing"
  "fmt"
  "rand"
  "log"
  "sort"
)

func TestHashTree1(t *testing.T) {
  set := []string{}
  for i := 0; i < 1000; i++ {
    set = append(set, fmt.Sprintf("m%v", i))
  }
  hash := ""
  for test := 0; test < 1000; test++ {
    tree := NewSimpleHashTree()
    perm := rand.Perm(len(set))
    for i := 0; i < len(set); i++ {
      member := set[perm[i]]
      h := sha256.New()
      h.Write([]byte(member))
      tree.Add(hex.EncodeToString(h.Sum()))
    }
    result := tree.Hash()
    if test == 0 {
      hash = result
    } else {
      if hash != result {
	t.Fatal("Hashes are not the same")
      }
    }
  }
}

func TestHashTree2(t *testing.T) {  
  set := []string{}
  for i := 0; i < 1000; i++ {
    set = append(set, fmt.Sprintf("m%v", i))
  }
  tree1 := NewSimpleHashTree()
  for i := 0; i < len(set); i++ {
    member := set[i]
    h := sha256.New()
    h.Write([]byte(member))
    tree1.Add(hex.EncodeToString(h.Sum()))
  }
  tree2 := NewSimpleHashTree()
  for i := 0; i < len(set); i++ {
    member := set[i]
    h := sha256.New()
    h.Write([]byte(member))
    tree2.Add(hex.EncodeToString(h.Sum()))
  }

  h := sha256.New()
  h.Write([]byte("Torben"))
  diff1 := [][]byte{h.Sum()}
  
  h = sha256.New()
  h.Write([]byte("Claudia"))
  diff2 := [][]byte{h.Sum()}
  h = sha256.New()
  h.Write([]byte("Anna"))
  diff2 = append(diff2, h.Sum())

  for _, d := range diff1 {
    tree1.Add(hex.EncodeToString(d))
  }
  for _, d := range diff2 {
    tree2.Add(hex.EncodeToString(d))
  }

  if tree1.Hash() == tree2.Hash() {
    t.Fatal("Hashes should be different")
  }
  
  only1 := []string{}
  only2 := []string{}
  
  onlyIn1, onlyIn2 := CompareHashTrees(tree1, tree2)
  for {
    select {
    case ch1, ok := <- onlyIn1:
      if !ok {
	return
      }
      log.Printf("Only In 1: %v\n", ch1)
      only1 = append(only1, ch1)
    case ch2, ok := <- onlyIn2:
      if !ok {
	return
      }
      log.Printf("Only In 2: %v\n", ch2)
      only2 = append(only2, ch2)
    }
  }
  if len(only1) != len(diff1) || len(only2) != len(diff2) {
    t.Fatal("Wrong diff size")
  }
  sort.SortStrings(only1)
  sort.SortStrings(only2)
  SortBytesArray(diff1)
  SortBytesArray(diff2)
  for i, s := range only1 {
    if s != hex.EncodeToString(diff1[i]) {
      t.Fatal("Wrong result")
    }
  }
  for i, s := range only2 {
    if s != hex.EncodeToString(diff2[i]) {
      t.Fatal("Wrong result")
    }
  }
}