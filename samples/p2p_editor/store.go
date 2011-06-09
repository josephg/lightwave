package main

import (
  "os"
  "json"
  . "lightwaveot"
  "log"
  "crypto/sha256"
  "encoding/base64"
)

type BlobRef []byte

type StoreListener interface {
  HandleBlob(blob []byte, ref BlobRef)
}

type Store struct {
  listeners []StoreListener
  blobs map[string][]byte
}

func EncodeMutation(mut Mutation) (blob []byte, ref BlobRef, err os.Error) {
  // HACK: Exclude the ID from marshalling
  mut.ID = ""
  blob, err = json.Marshal(&mut)
  if err != nil {
    log.Printf("ERR: %v", err)
  }
  h := sha256.New()
  h.Write(blob)
  ref = BlobRef(h.Sum())
  return
}

func DecodeMutation(blob []byte, ref BlobRef) (mut Mutation, err os.Error) {
  err = json.Unmarshal(blob, &mut)
  if err != nil {
    return
  }
  mut.ID = ref.String()
  return
}

func NewBlobRef(blob []byte) BlobRef {
  h := sha256.New()
  h.Write(blob)
  return BlobRef(h.Sum())
}

func DecodeBlobRef(ref string) (bref BlobRef, err os.Error) {
  result := make([]byte,32)[:]
  var n int
  n, err = base64.StdEncoding.Decode( result, []byte(ref) )
  if n != 32 {
    err = os.NewError("ID is too short")
    return
  }
  bref = result
  return
}

func (self *BlobRef) String() string {
  result := make([]byte, base64.StdEncoding.EncodedLen(len(*self)))[:]
  base64.StdEncoding.Encode(result, *self)
  return string(result)
}

func NewStore() *Store {
  return &Store{blobs:make(map[string][]byte)}
}

func (self *Store) Enumerate() (result map[string][]byte) {
  return self.blobs
}

// The BlobRef is optional. If it is zero then the blobref will be computed
func (self *Store) StoreBlob(blob []byte, ref BlobRef) {
  // Empty blob reference?
  if len(ref) == 0 {
    ref = NewBlobRef(blob)
  }
  str := ref.String()
  // The blob is already known?
  if _, ok := self.blobs[str]; ok {
    return
  }
  // Store the blob and allow for its further processing
  self.blobs[str] = blob
  for _, l := range self.listeners {
    l.HandleBlob(blob, ref)
  }
}

func (self *Store) GetBlob(ref BlobRef) (blob []byte, err os.Error) {
  str := ref.String()
  var ok bool
  if blob, ok = self.blobs[str]; ok {
    return
  }
  err = os.NewError("Unknown Blob ID")
  return
}

func (self *Store) AddListener(l StoreListener) {
  self.listeners = append(self.listeners, l)
}

