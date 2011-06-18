package main

import (
  "os"
  "strings"
  "crypto/sha256"
  "encoding/hex"
  fed "lightwavefed"
)

// Implements the BlobStore interface
type Store struct {
  listeners []fed.BlobStoreListener
  blobs map[string][]byte
  hashTree *fed.SimpleHashTree
}

func NewBlobRef(blob []byte) string {
  h := sha256.New()
  h.Write(blob)
  return string(hex.EncodeToString(h.Sum()))
}

func NewStore() *Store {
  s := &Store{blobs:make(map[string][]byte), hashTree: fed.NewSimpleHashTree()}
  return s
}

func (self *Store) HashTree() fed.HashTree {
  return self.hashTree
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
  self.hashTree.Add(blobref)
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

func (self *Store) GetBlobs(prefix string) (channel <-chan fed.Blob, err os.Error) {
  ch := make(chan fed.Blob)
  go self.getBlobs(prefix, ch)
  return ch, nil
}

func (self *Store) getBlobs(prefix string, channel chan fed.Blob) {
  // TODO: The sending on the channel might fail if the underlying
  // connection is broken
  for blobref, blob := range self.blobs {
    if strings.HasPrefix(blobref, prefix) {
      channel <- fed.Blob{Data:blob, BlobRef: blobref}
    }
  }
  close(channel)
}

func (self *Store) AddListener(l fed.BlobStoreListener) {
  self.listeners = append(self.listeners, l)
}

