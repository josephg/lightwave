package store

import (
  "testing"
  "time"
  "bytes"
  "fmt"
)

func TestReplication(t *testing.T) {
  store1 := NewSimpleBlobStore()
  store2 := NewSimpleBlobStore()
  rep1 := NewReplication("a@alice", store1, ":8181", "")
  rep2 := NewReplication("a@alice", store2, ":8282", ":8181")

  // Add some keys to both stores
  for i := 0; i < 1000; i++ {
    blob := []byte(fmt.Sprintf("{\"z\":\"m%v\"}", i))
    store1.StoreBlob(blob, "")
    store2.StoreBlob(blob, "")
  }

  // Add some keys which are different
  d1 := 10
  for i := 0; i < d1; i++ {
    store1.StoreBlob([]byte(fmt.Sprintf("{\"x\":%v}", i)), "")
  }
  // Add some keys which are different
  d2 := 17
  for i := 0; i < d2; i++ {
    store1.StoreBlob([]byte(fmt.Sprintf("{\"y\":%v}", i)), "")
  }

  go rep1.Listen()
  go rep2.Listen()
//  time.Sleep(100000)
//  
//  err := fed2.Dial("fed1.com")
//  if err != nil {
//    t.Fatal("Could not connect")
//  }

  // Wait for synchronization to happen
  time.Sleep(3000000000)
  
  // Now both stores should have the same stuff
  m1 := store1.Enumerate()
  m2 := store2.Enumerate()
  if len(m1) != len(m2) || len(m1) != 1000 + d1 + d2 {
    t.Fatalf("Wrong number of entries: %v %v", len(m1), len(m2))
  }
   
  for key, blob := range m1 {
    blob2, ok := m2[key]
    if !ok {
      t.Fatal("Missing blob")
    }
    if bytes.Compare(blob, blob2) != 0 {
      t.Fatal("Blobs are different")
    }
  }
}
