package lightwavetransformer

import (
  ot "lightwaveot"
  store "lightwavestore"
  grapher "lightwavegrapher"
  "testing"
  "log"
)

type dummyAPI struct {
  t *testing.T
  text *ot.SimpleText
}

func (self *dummyAPI) Signal_ReceivedInvitation(permission *grapher.Permission) {
}

func (self *dummyAPI) Signal_AcceptedInvitation(keep *grapher.Keep) {
}

func (self *dummyAPI) Blob_Keep(blob *grapher.Keep, seqNumber int) {
}

func (self *dummyAPI) Blob_Permission(blob *grapher.Permission, seqNumber int) {
}

func (self *dummyAPI) Blob_Mutation(mutation *grapher.Mutation, seqNumber int) {
  log.Printf("Executing %v\n", mutation.Operation)
  var op ot.Operation
  switch mutation.Operation.(type) {
  case ot.Operation:
    op = mutation.Operation.(ot.Operation)
  case []byte:
    err := op.UnmarshalJSON(mutation.Operation.([]byte))
    if err != nil {
      self.t.Fatal(err.String())
    }
  default:
    self.t.Fatal("Unknown OT operation")
  }
  result, err := ot.ExecuteOperation(self.text, op)
  if err != nil {
    self.t.Fatal(err.String())
  }
  var ok bool
  self.text, ok = result.(*ot.SimpleText)
  if !ok {
    self.t.Fatal("Casting failed")
  }
  log.Printf("Text %v\n", self.text.String())
}

func TestTransformer(t *testing.T) {
  s := store.NewSimpleBlobStore()
  grapher := grapher.NewGrapher("a@b", s, nil)
  NewTransformer(grapher)
  api := &dummyAPI{t: t, text: ot.NewSimpleText("")}
  grapher.SetAPI(api)
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc", "t":"2007-01-02T15:04:05+07:00"}`)
  blobref1 := store.NewBlobRef(blob1)
  blob1b := []byte(`{"type":"keep", "signer":"a@b", "perma":"` + blobref1 + `", "t":"2007-01-02T15:04:05+07:00", "dep":[]}`)
  blobref1b := store.NewBlobRef(blob1b)
  blob2 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "dep":["` + blobref1b + `"], "op":{"$t":["Hello World"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref2 := store.NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "signer":"x@b", "perma":"` + blobref1 + `", "dep":["` + blobref1b + `"], "op":{"$t":["Olla!!"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref3 := store.NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref4 := store.NewBlobRef(blob4)

  s.StoreBlob(blob1, blobref1)
  s.StoreBlob(blob1b, blobref1b)
  s.StoreBlob(blob2, blobref2)  
  s.StoreBlob(blob3, blobref3)  
  s.StoreBlob(blob4, blobref4)  
  
  if api.text.String() != "Hello World??Olla!!" {
    t.Fatal("Wrong resulting text:" + api.text.String())
  }
}