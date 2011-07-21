package lightwavestore

import (
  "os"
)

type BlobStore interface {
  StoreBlob(blob []byte, blobref string) (finalBlobRef string, err os.Error)
  AddListener(listener BlobStoreListener)
  HashTree() HashTree
  GetBlob(blobref string) (blob []byte, err os.Error)
  GetBlobs(prefix string) (channel <-chan Blob, err os.Error)
}

type BlobStoreListener interface {
  HandleBlob(blob []byte, blobref string)
}

type Blob struct {
  Data []byte
  BlobRef string
}

