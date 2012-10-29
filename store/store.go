package store

type BlobStore interface {
  StoreBlob(blob []byte, blobref string) (finalBlobRef string, err error)
  AddListener(listener BlobStoreListener)
  HashTree() HashTree
  GetBlob(blobref string) (blob []byte, err error)
  GetBlobs(prefix string) (channel <-chan Blob, err error)
}

type BlobStoreListener interface {
  HandleBlob(blob []byte, blobref string) error
}

type Blob struct {
  Data []byte
  BlobRef string
}

