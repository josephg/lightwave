package lightwavefed

import (
  "testing"
  "time"
)

type replyStruct struct {
  Value string "val"
}

func produce(ch ChunkSender) {
  ch <- ResponseChunk{201, "Hello"}
  ch <- ResponseChunk{201, "shiny"}
  ch <- ResponseChunk{201, "new"}
  ch <- ResponseChunk{200, "world"}
  close(ch)
}

func myHandler(req *Message) (status int, data interface{}) {
  ch := make(ChunkSender)
  go produce(ch)
  return 201, ch
}

func TestChunks(t *testing.T) {
  fed := NewFederation()
  fed.Handle( "ALL", myHandler)
  go Listen(":8787", fed)
  time.Sleep(100000)
  
  conn, err := Dial(":8787", fed)
  if err != nil {
    t.Fatal("Could not connect")
  }
  
  rd := make(MessageReceiver)
  conn.SendRequest("ALL", nil, rd)
  
  all := ""
  for msg := range rd {
    var str string
    err := msg.DecodePayload(&str)
    if err != nil {
      t.Fatal(err.String())
    }
    all += str
  }
  if all != "Helloshinynewworld" {
    t.Fatal("Got wrong data")
  }
}