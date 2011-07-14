package lightwavetransformer

import (
  ot "lightwaveot"
  store "lightwavestore"
  grapher "lightwavegrapher"
  "testing"
  "log"
)

type dummyApp struct {
  api UniAPI
  t *testing.T
  text *ot.SimpleText
}

func (self *dummyApp) Invitation(permanode_blobref, invitation_blobref string) {
}

func (self *dummyApp) AcceptedInvitation(permanode_blobref, invitation_blobref string, keep_blobref string) {
}

func (self *dummyApp) NewFollower(permanode_blobref string, invitation_blobref, keep_blobref, userid string) {
}

func (self *dummyApp) PermaNode(permanode_blobref string, invitation_blobref, keep_blobref string) {
  self.api.Open(permanode_blobref)
}

func (self *dummyApp) Mutation(permanode_blobref string, mut interface{}) {
  mutation, ok := mut.(ot.Mutation)
  if !ok {
    panic("Wrong kind of mutation")
  }
  result, err := ot.Execute(self.text, mutation)
  if err != nil {
    self.t.Fatal(err.String())
  }
  self.text, ok = result.(*ot.SimpleText)
  if !ok {
    self.t.Fatal("Casting failed")
  }
  log.Printf("Executed %v\n", mutation.ID)
}

func (self *dummyApp)  Permission(permanode_blobref string, action int, permission ot.Permission) {
}

func TestTransformer(t *testing.T) {
  s := store.NewSimpleBlobStore()
  grapher := grapher.NewGrapher("a@b", s, nil)
  transformer := NewTransformer("a@b", s, nil, grapher)
  api := NewUniAPI()
  transformer.SetAPI(api)
  app := &dummyApp{api: api, t: t, text: ot.NewSimpleText("")}
  api.SetApplication(app)
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc", "t":"2007-01-02T15:04:05+07:00"}`)
  blobref1 := store.NewBlobRef(blob1)
  blob1b := []byte(`{"type":"keep", "signer":"a@b", "perma":"` + blobref1 + `", "t":"2007-01-02T15:04:05+07:00", "dep":[]}`)
  blobref1b := store.NewBlobRef(blob1b)
  blob2 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "site":"site1", "dep":["` + blobref1b + `"], "op":{"$t":["Hello World"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref2 := store.NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "site":"site2", "dep":["` + blobref1b + `"], "op":{"$t":["Olla!!"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref3 := store.NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "site":"site1", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref4 := store.NewBlobRef(blob4)

  s.StoreBlob(blob1, blobref1)
  s.StoreBlob(blob1b, blobref1b)
  s.StoreBlob(blob2, blobref2)  
  s.StoreBlob(blob3, blobref3)  
  s.StoreBlob(blob4, blobref4)  
  
  if app.text.String() != "Hello World??Olla!!" {
    t.Fatal("Wrong resulting text")
  }
}