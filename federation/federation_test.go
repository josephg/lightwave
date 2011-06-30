package lightwavefed

import (
  . "lightwavestore"
  idx "lightwaveidx"
  "testing"
  "time"
  "fmt"
  "log"
  "os"
)

type dummyAppIndexer struct {
  userID string
  store BlobStore
  fed *Federation
  t *testing.T
}

func (self *dummyAppIndexer) Invitation(invitation_blobref string) {
  go self.fed.AcceptInvitation(invitation_blobref)
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
  indexer1 := idx.NewIndexer("a@alice", store1, fed1)
  indexer2 := idx.NewIndexer("b@bob", store2, fed2)
  indexer3 := idx.NewIndexer("c@charly", store3, fed3)
  indexer4 := idx.NewIndexer("d@daisy", store4, fed4)
  app1 := &dummyAppIndexer{"a@alice", store1, fed1, t}
  indexer1.AddListener(app1)
  app2 := &dummyAppIndexer{"b@bob", store2, fed2, t}
  indexer2.AddListener(app2)
  app3 := &dummyAppIndexer{"c@charly", store3, fed3, t}
  indexer3.AddListener(app3)
  app4 := &dummyAppIndexer{"d@daisy", store4, fed4, t}
  indexer4.AddListener(app4)
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
  blob5 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"b@bob", "allow":` + fmt.Sprintf("%v", idx.Perm_Read) + `, "deny":0, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref5 := NewBlobRef(blob5)
  blob7 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "site":"site3", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "!!"]}, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref7 := NewBlobRef(blob7)
  blob8 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"c@charly", "allow":` + fmt.Sprintf("%v", idx.Perm_Read) + `, "deny":0, "t":"2006-01-02T15:04:05+07:00"}`)
  blobref8 := NewBlobRef(blob8)
  blob9 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"d@daisy", "allow":` + fmt.Sprintf("%v", idx.Perm_Read) + `, "deny":0, "t":"2006-01-02T15:04:05+07:00"}`)
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
  
  p, err := indexer1.PermaNode(blobref1)
  if err != nil {
    t.Fatal("Not a perma node")
  }
  if len(p.Followers()) != 4 {
    t.Fatal("Indexer1 has wrong number of followers")
  }
  log.Printf("Content: %v\n", p.OT().Content()) 
  if fmt.Sprintf("%v", p.OT().Content()) != "Hello World??Olla!!!!" {
    t.Fatal("Wrong document content")
  }
  p, err = indexer2.PermaNode(blobref1)
  if err != nil {
    t.Fatal("Not a perma node")
  }
  if len(p.Followers()) != 4 {
    t.Fatal("Indexer2 has wrong number of followers", p.Followers())
  }
  if fmt.Sprintf("%v", p.OT().Content()) != "Hello World??Olla!!!!" {
    t.Fatal("Wrong document content")
  }
  p, err = indexer3.PermaNode(blobref1)
  if err != nil {
    t.Fatal("Not a perma node")
  }
  if len(p.Followers()) != 4 {
    t.Fatal("Indexer3 has wrong number of followers", p.Followers())
  }
  if fmt.Sprintf("%v", p.OT().Content()) != "Hello World??Olla!!!!" {
    t.Fatal("Wrong document content")
  }
  p, err = indexer4.PermaNode(blobref1)
  if err != nil {
    t.Fatal("Not a perma node")
  }
  if len(p.Followers()) != 4 {
    t.Fatal("Indexer3 has wrong number of followers", p.Followers())
  }  
  if fmt.Sprintf("%v", p.OT().Content()) != "Hello World??Olla!!!!" {
    t.Fatal("Wrong document content")
  }
}