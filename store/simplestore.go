package lightwavestore

import (
  "os"
  "strings"
  "crypto/sha256"
  "encoding/hex"
  "log"
)

func NewBlobRef(blob []byte) string {
  h := sha256.New()
  h.Write(blob)
  return string(hex.EncodeToString(h.Sum()))
}

type SimpleBlobStore struct {
  listeners []BlobStoreListener
  blobs map[string][]byte
  hashTree *SimpleHashTree
}

func NewSimpleBlobStore() *SimpleBlobStore {
  s := &SimpleBlobStore{blobs:make(map[string][]byte), hashTree: NewSimpleHashTree()}
  return s
}

func (self *SimpleBlobStore) Enumerate() (result map[string][]byte) {
  return self.blobs
}

func (self *SimpleBlobStore) StoreBlob(blob []byte, blobref string) {
  // Empty blob reference?
  if len(blobref) == 0 {
    blobref = NewBlobRef(blob)
  }
  // The blob is already known?
  if _, ok := self.blobs[blobref]; ok {
    log.Printf("Blob is already known\n")
    return
  }
  self.hashTree.Add(blobref)
  // Store the blob and allow for its further processing
  self.blobs[blobref] = blob
  for _, l := range self.listeners {
    l.HandleBlob(blob, blobref)
  }
}

func (self *SimpleBlobStore) HashTree() HashTree {
  return self.hashTree
}

func (self *SimpleBlobStore) GetBlob(blobref string) (blob []byte, err os.Error) {
  var ok bool
  if blob, ok = self.blobs[blobref]; ok {
    return
  }
  err = os.NewError("Unknown Blob ID")
  return
}

func (self *SimpleBlobStore) GetBlobs(prefix string) (channel <-chan Blob, err os.Error) {
  ch := make(chan Blob)
  go self.getBlobs(prefix, ch)
  return ch, nil
}

func (self *SimpleBlobStore) getBlobs(prefix string, channel chan Blob) {
  // TODO: The sending on the channel might fail if the underlying
  // connection is broken
  for blobref, blob := range self.blobs {
    if strings.HasPrefix(blobref, prefix) {
      channel <- Blob{Data:blob, BlobRef: blobref}
    }
  }
  close(channel)
}

func (self *SimpleBlobStore) AddListener(l BlobStoreListener) {
  self.listeners = append(self.listeners, l)
}
