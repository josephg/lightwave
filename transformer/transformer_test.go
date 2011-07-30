package lightwavetransformer

import (
  ot "lightwaveot"
  store "lightwavestore"
  grapher "lightwavegrapher"
  "testing"
  "log"
  "time"
)

type dummyAPI struct {
  t *testing.T
  text *ot.SimpleText
}

func (self *dummyAPI) Signal_ReceivedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode) {
}

func (self *dummyAPI) Signal_AcceptedInvitation(perma grapher.PermaNode, perm grapher.PermissionNode, keep grapher.KeepNode) {
}

func (self *dummyAPI) Blob_Keep(perma grapher.PermaNode, perm grapher.PermissionNode, keep grapher.KeepNode) {
}

func (self *dummyAPI) Blob_Permission(perma grapher.PermaNode, perm grapher.PermissionNode) {
}

func (self *dummyAPI) Blob_Entity(perma grapher.PermaNode, entity grapher.EntityNode) {
}

func (self *dummyAPI) Blob_Mutation(perma grapher.PermaNode, mutation grapher.MutationNode) {
  var op ot.Operation
  switch mutation.Operation().(type) {
  case ot.Operation:
    op = mutation.Operation().(ot.Operation)
  case []byte:
    err := op.UnmarshalJSON(mutation.Operation().([]byte))
    if err != nil {
      self.t.Fatal(err.String())
    }
  default:
    self.t.Fatal("Unknown OT operation")
  }
  log.Printf("Executing %v\n", op)
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
  sg := grapher.NewSimpleGraphStore()
  grapher := grapher.NewGrapher("a@b", s, sg, nil)
  s.AddListener(grapher)
  NewTransformer(grapher)
  api := &dummyAPI{t: t, text: ot.NewSimpleText("")}
  grapher.SetAPI(api)
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc"}`)
  blobref1 := store.NewBlobRef(blob1)
  blob1b := []byte(`{"type":"keep", "signer":"a@b", "perma":"` + blobref1 + `", "dep":[]}`)
  blobref1b := store.NewBlobRef(blob1b)
  blob1c := []byte(`{"type":"entity", "signer":"a@b", "perma":"` + blobref1 + `", "content":"", "dep":["` + blobref1b + `"]}`)
  blobref1c := store.NewBlobRef(blob1c)
  blob2 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "dep":["` + blobref1c + `"], "op":{"$t":["Hello World"]}, "entity":"` + blobref1c + `"}`)
  blobref2 := store.NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "signer":"x@b", "perma":"` + blobref1 + `", "dep":["` + blobref1c + `"], "op":{"$t":["Olla!!"]}, "entity":"` + blobref1c + `"}`)
  blobref3 := store.NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}, "entity":"` + blobref1c + `"}`)
  blobref4 := store.NewBlobRef(blob4)

  s.StoreBlob(blob1, blobref1)
  s.StoreBlob(blob1b, blobref1b)
  s.StoreBlob(blob1c, blobref1c)
  s.StoreBlob(blob2, blobref2)  
  s.StoreBlob(blob3, blobref3)  
  s.StoreBlob(blob4, blobref4)  
  
  time.Sleep(1000000000 * 2)

  if api.text.String() != "Hello World??Olla!!" {
    t.Fatal("Wrong resulting text:" + api.text.String())
  }
}