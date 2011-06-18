package lightwavefed

import (
  "crypto/sha256"
  "encoding/hex"
  "testing"
  "fmt"
  "rand"
  "log"
  "sort"
  "time"
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
    result, _ := tree.Hash()
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

  h1, _ := tree1.Hash()
  h2, _ := tree2.Hash()
  if h1 == h2 {
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
  sortBytesArray(diff1)
  sortBytesArray(diff2)
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

func TestHashTreeRemote(t *testing.T) {  
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

  h1, _ := tree1.Hash()
  h2, _ := tree2.Hash()
  if h1 == h2 {
    t.Fatal("Hashes should be different")
  }

  fed1 := NewFederation(&dummyBlobStore2{})
  go Listen(":8989", fed1)
  time.Sleep(100000)
  
  fed2 := NewFederation(&dummyBlobStore2{})
  conn, err := Dial(":8989", fed2)
  if err != nil {
    t.Fatal("Could not connect")
  }
  
  rtree1 := NewRemoteHashTree(conn)
  handler := HashTreeHandler(tree1)
  fed1.Handle( "THASH", handler )
  fed1.Handle( "TCHLD", handler )
  
  x, err := rtree1.Hash()
  if err != nil {
    t.Fatal(err.String())
  }
  log.Printf("REMOTE HASH: %v\n", x)
  
  only1 := []string{}
  only2 := []string{}

  onlyIn1, onlyIn2 := CompareHashTrees(rtree1, tree2)
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
  sortBytesArray(diff1)
  sortBytesArray(diff2)
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

type dummyBlobStore2 struct {
}

func (self *dummyBlobStore2) StoreBlob(blob []byte, blobref string) {
}

func (self *dummyBlobStore2) AddListener(listener BlobStoreListener) {
}

func (self *dummyBlobStore2) HashTree() HashTree {
  return nil
}
