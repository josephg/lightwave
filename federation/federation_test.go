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
}

func (self *dummyAppIndexer) Invitation(invitation_blobref, permanode_blobref, inviter string) {
  log.Printf("%v is automatically accepting the invitation\n", self.userID)
  keepBlob := []byte(`{"type":"keep", "signer":"` + self.userID + `", "invitation":"` + invitation_blobref + `", "perma":"` + permanode_blobref + `"}`)
  keepBlobRef := NewBlobRef(keepBlob)
  self.store.StoreBlob(keepBlob, keepBlobRef)
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
  fed1 := NewFederation("127.0.0.1:8181", ns, store1, nil)
  fed2 := NewFederation("127.0.0.1:8282", ns, store2, nil)
  fed3 := NewFederation("127.0.0.1:8383", ns, store3, nil)
  indexer1 := idx.NewIndexer("a@alice", store1, fed1)
  indexer2 := idx.NewIndexer("b@bob", store2, fed2)
  indexer3 := idx.NewIndexer("c@charly", store3, fed3)
  app1 := &dummyAppIndexer{"a@alice", store1}
  indexer1.AddListener(app1)
  app2 := &dummyAppIndexer{"b@bob", store2}
  indexer2.AddListener(app2)
  app3 := &dummyAppIndexer{"c@charly", store3}
  indexer3.AddListener(app3)
  go runFed(t, fed1)
  go runFed(t, fed2)
  go runFed(t, fed3)
  
  time.Sleep(1000000000 * 2)

  blob1 := []byte(`{"type":"permanode", "signer":"a@alice", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob1b := []byte(`{"type":"keep", "signer":"a@alice", "perma":"` + blobref1 + `"}`)
  blobref1b := NewBlobRef(blob1b)  
  blob2 := []byte(`{"type":"mutation", "perma":"` + blobref1 + `", "site":"site1", "dep":[], "op":{"$t":["Hello World"]}}`)
  blobref2 := NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "perma":"` + blobref1 + `", "site":"site2", "dep":[], "op":{"$t":["Olla!!"]}}`)
  blobref3 := NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "perma":"` + blobref1 + `", "site":"site1", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}}`)
  blobref4 := NewBlobRef(blob4)
  // Grant user foo@bar read access
  blob5 := []byte(`{"type":"permission", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"foo@bar", "allow":` + fmt.Sprintf("%v", idx.Perm_Read) + `, "deny":0}`)
  blobref5 := NewBlobRef(blob5)
  // Invite 
  blob6 := []byte(`{"type":"invitation", "signer":"a@alice", "perma":"` + blobref1 + `", "user":"b@bob"}`)
  blobref6 := NewBlobRef(blob6)
  blob7 := []byte(`{"type":"mutation", "perma":"` + blobref1 + `", "site":"site3", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "!!"]}}`)
  blobref7 := NewBlobRef(blob7)
  // Invite 
  blob8 := []byte(`{"type":"invitation", "signer":"a@alice", "perma":"` + blobref1 + `", "user":"c@charly"}`)
  blobref8 := NewBlobRef(blob8)

  log.Printf("Hashes:\n1: %v\n2: %v\n3: %v\n4: %v\n5: %v\n6: %v\n7: %v\n", blobref1, blobref2, blobref3, blobref4, blobref5, blobref6, blobref7)
  
  // Insert them in the wrong order
  store1.StoreBlob(blob1, blobref1)
  store1.StoreBlob(blob1b, blobref1b)
  store1.StoreBlob(blob2, blobref2)  
  store1.StoreBlob(blob3, blobref3)  
  store1.StoreBlob(blob4, blobref4)  
  store1.StoreBlob(blob5, blobref5)
  store1.StoreBlob(blob6, blobref6)

  time.Sleep(1000000000 * 2)

  store1.StoreBlob(blob7, blobref7)
  store1.StoreBlob(blob8, blobref8)

  time.Sleep(1000000000 * 2)
  
  p, err := indexer1.PermaNode(blobref1)
  if err != nil {
    t.Fatal("Not a perma node")
  }
  if len(p.Followers()) != 3 {
    t.Fatal("Indexer1 has wrong number of followers")
  }
  p, err = indexer2.PermaNode(blobref1)
  if err != nil {
    t.Fatal("Not a perma node")
  }
  if len(p.Followers()) != 3 {
    t.Fatal("Indexer2 has wrong number of followers", p.Followers())
  }
  p, err = indexer3.PermaNode(blobref1)
  if err != nil {
    t.Fatal("Not a perma node")
  }
  if len(p.Followers()) != 3 {
    t.Fatal("Indexer3 has wrong number of followers", p.Followers())
  }
}