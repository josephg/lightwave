package lightwavefed

import (
  "testing"
  "time"
)

type replyStruct struct {
  Value string "val"
}

type dummyBlobStore struct {
}

func (self *dummyBlobStore) StoreBlob(blob []byte, blobref string) {
}

func (self *dummyBlobStore) AddListener(listener BlobStoreListener) {
}

func (self *dummyBlobStore) HashTree() HashTree {
  return nil
}

func produce(ch ChunkChannel) {
  ch <- "Hello"
  ch <- "shiny"
  ch <- "new"
  ch <- "world"
  close(ch)
}

func myHandler(req *Message) (status int, data interface{}) {
  ch := make(ChunkChannel)
  go produce(ch)
  return 0, ch
}

func TestChunks(t *testing.T) {
  fed1 := NewFederation(&dummyBlobStore{})
  fed2 := NewFederation(&dummyBlobStore{})

  fed1.Handle( "ALL", myHandler)
  go Listen(":8787", fed1)
  time.Sleep(100000)
  
  conn, err := Dial(":8787", fed2)
  if err != nil {
    t.Fatal("Could not connect")
  }
  
  rd := make(MessageChannel)
  conn.SendRequest("ALL", nil, rd)
  
  all := ""
  for msg := range rd {
    if msg.Status == 200 {
      break
    } else if msg.Status == 201 {
      var str string
      err := msg.DecodePayload(&str)
      if err != nil {
	t.Fatal(err.String())
      }
      all += str
    } else {
      t.Fatal("Unexpected status code")
    }
  }
  if all != "Helloshinynewworld" {
    t.Fatal("Got wrong data")
  }
}