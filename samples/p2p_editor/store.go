package main

import (
  "os"
  "crypto/sha256"
  "encoding/hex"
)

type StoreListener interface {
  HandleBlob(blob []byte, blobref string)
}

type Store struct {
  listeners []StoreListener
  blobs map[string][]byte
}

func NewBlobRef(blob []byte) string {
  h := sha256.New()
  h.Write(blob)
  return string(hex.EncodeToString(h.Sum()))
}

func NewStore() *Store {
  return &Store{blobs:make(map[string][]byte)}
}

func (self *Store) Enumerate() (result map[string][]byte) {
  return self.blobs
}

// The BlobRef is optional. If it is zero then the blobref will be computed
func (self *Store) StoreBlob(blob []byte, blobref string) {
  // Empty blob reference?
  if len(blobref) == 0 {
    blobref = NewBlobRef(blob)
  }
  // The blob is already known?
  if _, ok := self.blobs[blobref]; ok {
    return
  }
  // Store the blob and allow for its further processing
  self.blobs[blobref] = blob
  for _, l := range self.listeners {
    l.HandleBlob(blob, blobref)
  }
}

func (self *Store) GetBlob(blobref string) (blob []byte, err os.Error) {
  var ok bool
  if blob, ok = self.blobs[blobref]; ok {
    return
  }
  err = os.NewError("Unknown Blob ID")
  return
}

func (self *Store) AddListener(l StoreListener) {
  self.listeners = append(self.listeners, l)
}

