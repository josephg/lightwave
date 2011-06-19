package lightwavefed

import (
  "testing"
  "time"
  "bytes"
  "os"
  "fmt"
  "strings"
  "crypto/sha256"
  "encoding/hex"
)

func newBlobRef(blob []byte) string {
  h := sha256.New()
  h.Write(blob)
  return string(hex.EncodeToString(h.Sum()))
}

type dummyBlobStore struct {
  listeners []BlobStoreListener
  blobs map[string][]byte
  hashTree *SimpleHashTree
}

func newDummyBlobStore() *dummyBlobStore {
  s := &dummyBlobStore{blobs:make(map[string][]byte), hashTree: NewSimpleHashTree()}
  return s
}

func (self *dummyBlobStore) Enumerate() (result map[string][]byte) {
  return self.blobs
}

func (self *dummyBlobStore) StoreBlob(blob []byte, blobref string) {
  // Empty blob reference?
  if len(blobref) == 0 {
    blobref = newBlobRef(blob)
  }
  // The blob is already known?
  if _, ok := self.blobs[blobref]; ok {
    return
  }
  self.hashTree.Add(blobref)
  // Store the blob and allow for its further processing
  self.blobs[blobref] = blob
  for _, l := range self.listeners {
    l.HandleBlob(blob, blobref)
  }
}

func (self *dummyBlobStore) HashTree() HashTree {
  return self.hashTree
}

func (self *dummyBlobStore) GetBlob(blobref string) (blob []byte, err os.Error) {
  var ok bool
  if blob, ok = self.blobs[blobref]; ok {
    return
  }
  err = os.NewError("Unknown Blob ID")
  return
}

func (self *dummyBlobStore) GetBlobs(prefix string) (channel <-chan Blob, err os.Error) {
  ch := make(chan Blob)
  go self.getBlobs(prefix, ch)
  return ch, nil
}

func (self *dummyBlobStore) getBlobs(prefix string, channel chan Blob) {
  // TODO: The sending on the channel might fail if the underlying
  // connection is broken
  for blobref, blob := range self.blobs {
    if strings.HasPrefix(blobref, prefix) {
      channel <- Blob{Data:blob, BlobRef: blobref}
    }
  }
  close(channel)
}

func (self *dummyBlobStore) AddListener(l BlobStoreListener) {
  self.listeners = append(self.listeners, l)
}

type dummyNameService struct {
}

func (self *dummyNameService) Lookup(identity string) (addr string, err os.Error) {
  switch identity {
  case "fed1.com":
    return ":8787", nil
  case "fed2.com":
    return ":8686", nil
  }
  return "", os.NewError("Unknown host")
}
      

func TestFed(t *testing.T) {
  ns := &dummyNameService{}
  store1 := newDummyBlobStore()
  store2 := newDummyBlobStore()
  fed1 := NewFederation("fed1.com", ns, store1)
  fed2 := NewFederation("fed2.com", ns, store2)

  // Add some keys to both stores
  for i := 0; i < 1000; i++ {
    blob := []byte(fmt.Sprintf("{\"x\"=\"m%v\"}", i))
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

  go fed1.Listen()
  time.Sleep(100000)
  
  err := fed2.Dial("fed1.com")
  if err != nil {
    t.Fatal("Could not connect")
  }

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