package lightwavefed

import (
  ot "lightwaveot"
  . "lightwavestore"
  grapher "lightwavegrapher"
  "testing"
  "time"
  "fmt"
  "log"
  "os"
)

type dummyTransformer struct {
  userID string
  store BlobStore
  fed *Federation
  grapher *grapher.Grapher
  t *testing.T
}

func (self *dummyTransformer) Invitation(permanode_blobref, invitation_blobref, userid string) {
  _, err := self.grapher.CreateKeepBlob(permanode_blobref, invitation_blobref)
  if err != nil {
    self.t.Fatal(err.String())
  }
}

func (self *dummyTransformer) AcceptedInvitation(permanode_blobref, invitation_blobref string, keep_blobref string) {
}

func (self *dummyTransformer) NewFollower(permanode_blobref, invitation_blobref, keep_blobref, userid string) {
  log.Printf("APP: New user: %v\n", userid)
}

func (self *dummyTransformer) PermaNode(blobref, invitation_blobref, keep_blobref string) {
  log.Printf("APP: New permanode")
}

func (self *dummyTransformer) Mutation(blobref, mut_blobref string, mutation []byte, rollback int, concurrent []string) {
  log.Printf("APP: Mutation")
}

func (self *dummyTransformer) Permission(blobref string, action int, permission ot.Permission) {
  log.Printf("App: Permission")
}

type dummyNameService struct {
}

func (self *dummyNameService) Lookup(identity string) (url string, err os.Error) {
  switch identity {
  case "a@alice":
    return "http://localhost:8181/fed", nil
  case "b@bob":
    return "http://localhost:8282/fed", nil
  case "c@charly":
    return "http://localhost:8383/fed", nil
  case "d@daisy":
    return "http://localhost:8484/fed", nil
  }
  return "", os.NewError("Unknown identity")
}

func runFed(t *testing.T, fed *Federation) {
  err := fed.Listen()
  if err != nil {
    t.Fatal(err.String())
  }
}

func TestFederation(t *testing.T) {
  ns := &dummyNameService{}
  store1 := NewSimpleBlobStore()
  store2 := NewSimpleBlobStore()
  store3 := NewSimpleBlobStore()
  store4 := NewSimpleBlobStore()
  fed1 := NewFederation("a@alice", "127.0.0.1:8181", ns, store1)
  fed2 := NewFederation("b@bob", "127.0.0.1:8282", ns, store2)
  fed3 := NewFederation("c@charly", "127.0.0.1:8383", ns, store3)
  fed4 := NewFederation("d@daisy", "127.0.0.1:8484", ns, store4)
  grapher1 := grapher.NewGrapher("a@alice", store1, fed1)
  grapher2 := grapher.NewGrapher("b@bob", store2, fed2)
  grapher3 := grapher.NewGrapher("c@charly", store3, fed3)
  grapher4 := grapher.NewGrapher("d@daisy", store4, fed4)
  app1 := &dummyTransformer{"a@alice", store1, fed1, grapher1, t}
  grapher1.AddListener(app1)
  app2 := &dummyTransformer{"b@bob", store2, fed2, grapher2, t}
  grapher2.AddListener(app2)
  app3 := &dummyTransformer{"c@charly", store3, fed3, grapher3, t}
  grapher3.AddListener(app3)
  app4 := &dummyTransformer{"d@daisy", store4, fed4, grapher4, t}
  grapher4.AddListener(app4)
  go runFed(t, fed1)
  go runFed(t, fed2)
  go runFed(t, fed3)
  go runFed(t, fed4)
  
  time.Sleep(1000000000 * 2)

  blob1 := []byte(`{"type":"permanode", "signer":"a@alice", "random":"perma1abc", "t":"2006-01-02T15:04:05+07:00"}`)
  blobref1 := NewBlobRef(blob1)
  blob1b := []byte(`{"type":"keep", "signer":"a@alice", "perma":"` + blobref1 + `", "t":"2006-01-02T15:04:05+07:00"}`)
  blobref1b := NewBlobRef(blob1b)  
  blob2 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "site":"site1", "dep":["` + blobref1b + `"], "op":{"$t":["Hello World"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref2 := NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "site":"site2", "dep":[], "op":{"$t":["Olla!!"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref3 := NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "site":"site1", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref4 := NewBlobRef(blob4)
  // Grant user foo@bar read access
  blob5 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"b@bob", "allow":` + fmt.Sprintf("%v", grapher.Perm_Read) + `, "deny":0, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref5 := NewBlobRef(blob5)
  blob7 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "site":"site3", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "!!"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref7 := NewBlobRef(blob7)
  blob8 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"c@charly", "allow":` + fmt.Sprintf("%v", grapher.Perm_Read) + `, "deny":0, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref8 := NewBlobRef(blob8)
  blob9 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"d@daisy", "allow":` + fmt.Sprintf("%v", grapher.Perm_Read) + `, "deny":0, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref9 := NewBlobRef(blob9)

  log.Printf("Hashes:\n1: %v\n1b: %v\n2: %v\n3: %v\n4: %v\n5: %v\n7: %v\n8: %v\n", blobref1, blobref1b, blobref2, blobref3, blobref4, blobref5, blobref7, blobref8)
  
  // Insert them in the wrong order
  store1.StoreBlob(blob1, blobref1)
  store1.StoreBlob(blob1b, blobref1b)
  store1.StoreBlob(blob2, blobref2)  
  store1.StoreBlob(blob3, blobref3)  
  store1.StoreBlob(blob4, blobref4)  
  store1.StoreBlob(blob5, blobref5)
  store1.StoreBlob(blob8, blobref8)

  time.Sleep(1000000000 * 2)

  store1.StoreBlob(blob7, blobref7)
  store1.StoreBlob(blob9, blobref9)

  time.Sleep(1000000000 * 2)
  
  followers, err := grapher1.Followers(blobref1)
  if err != nil {
    t.Fatal(err.String())
  }
  if len(followers) != 4 {
    t.Fatal("Indexer1 has wrong number of followers")
  }
  followers, err = grapher2.Followers(blobref1)
  if err != nil {
    t.Fatal(err.String())
  }
  if len(followers) != 4 {
    t.Fatal("Indexer2 has wrong number of followers")
  }
  followers, err = grapher3.Followers(blobref1)
  if err != nil {
    t.Fatal(err.String())
  }
  if len(followers) != 4 {
    t.Fatal("Indexer3 has wrong number of followers")
  }
  followers, err = grapher4.Followers(blobref1)
  if err != nil {
    t.Fatal(err.String())
  }
  if len(followers) != 4 {
    t.Fatal("Indexer4 has wrong number of followers")
  }
}