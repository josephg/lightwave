package store

import (
  "crypto/sha256"
  "encoding/hex"
  "testing"
  "fmt"
  "rand"
)

// This test checks that the hash tree is always the same
// no matter in which order the IDs are being inserted
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
      tree.Add(hex.EncodeToString(h.Sum([]byte{})))
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
