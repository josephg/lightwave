package lightwaveidx

import (
  . "lightwavestore"
  "testing"
  "fmt"
  "log"
)

type dummyFederation struct {
}

func (self *dummyFederation) Forward(blobref string, users []string) {
  log.Printf("Forwarding %v to %v\n", blobref, users) 
}

func (self *dummyFederation) SetIndexer(idx *Indexer) {
}

func TestPermanode(t *testing.T) {
  store := NewSimpleBlobStore()
  indexer := NewIndexer("a@b", store, &dummyFederation{})
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob2 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma2xyz", "perma":"` + blobref1 + `"}`)
  blobref2 := NewBlobRef(blob2)
  
  store.StoreBlob(blob1, blobref1)
  store.StoreBlob(blob2, blobref2)
  
  perma, err := indexer.PermaNode(blobref1)
  if perma == nil || err != nil {
    t.Fatal("Did not find perma node")
  }
  perma, err = indexer.PermaNode(blobref2)
  if perma == nil || err != nil {
    t.Fatal("Did not find perma node")
  }
}

func TestPermanode2(t *testing.T) {
  store := NewSimpleBlobStore()
  indexer := NewIndexer("a@b", store, &dummyFederation{})
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob2 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma2xyz", "perma":"` + blobref1 + `"}`)
  blobref2 := NewBlobRef(blob2)

  // Insert them in the wrong order
  store.StoreBlob(blob2, blobref2)  
  store.StoreBlob(blob1, blobref1)

  perma, err := indexer.PermaNode(blobref1)
  if perma == nil || err != nil {
    t.Fatal("Did not find perma node")
  }
  perma, err = indexer.PermaNode(blobref2)
  if perma == nil || err != nil {
    t.Fatal("Did not find perma node")
  }
}

func TestPermanode3(t *testing.T) {
  fed := &dummyFederation{}
  store := NewSimpleBlobStore()
  indexer := NewIndexer("a@b", store, fed)
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob1b := []byte(`{"type":"keep", "signer":"a@b", "perma":"` + blobref1 + `"}`)
  blobref1b := NewBlobRef(blob1b)
  blob2 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "site":"site1", "dep":[], "op":{"$t":["Hello World"]}}`)
  blobref2 := NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "site":"site2", "dep":[], "op":{"$t":["Olla!!"]}}`)
  blobref3 := NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "signer":"a@b", "perma":"` + blobref1 + `", "site":"site1", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}}`)
  blobref4 := NewBlobRef(blob4)
  // Grant user foo@bar read access. At the same time this serves as an invitation
  blob5 := []byte(`{"type":"permission", "perma":"` + blobref1 + `", "signer":"a@b", "action":"invite", "dep":["` + blobref4 + `"], "user":"foo@bar", "allow":` + fmt.Sprintf("%v", Perm_Read) + `, "deny":0}`)
  blobref5 := NewBlobRef(blob5)
  // Fake a keep
  blob7 := []byte(`{"type":"keep", "signer":"foo@bar", "permission":"` + blobref5 + `", "perma":"` + blobref1 + `"}`)
  blobref7 := NewBlobRef(blob7)

  store.StoreBlob(blob1, blobref1)
  store.StoreBlob(blob1b, blobref1b)
  store.StoreBlob(blob2, blobref2)  
  store.StoreBlob(blob3, blobref3)  
  store.StoreBlob(blob4, blobref4)  
  store.StoreBlob(blob5, blobref5)
  store.StoreBlob(blob7, blobref7)
  
  perma, err := indexer.PermaNode(blobref1)
  if perma == nil || err != nil {
    t.Fatal("Did not find perma node")
  }
  if !perma.HasKeep("a@b") {
    t.Fatal("Missing a keep")
  }
  if !perma.HasKeep("foo@bar") {
    t.Fatal("Missing a keep")
  }

  allow, err := indexer.HasPermission("a@b", blobref1, Perm_Read)
  if err != nil {
    t.Fatal(err.String())
  }
  if !allow {
    t.Fatal("Expected an allow for a@b")
  }

  allow, err = indexer.HasPermission("x@y", blobref1, Perm_Read)
  if err != nil {
    t.Fatal(err.String())
  }
  if allow {
    t.Fatal("Expected a deny")
  }

  allow, err = indexer.HasPermission("foo@bar", blobref1, Perm_Read)
  if err != nil {
    t.Fatal(err.String())
  }
  if !allow {
    t.Fatal("Expected an allow for foo@bar")
  }

  allow, err = indexer.HasPermission("a@b", blobref1, Perm_Invite | Perm_Expel)
  if err != nil {
    t.Fatal(err.String())
  }
  if !allow {
    t.Fatal("Expected an allow for Invite a@b")
  }

  users, err := indexer.Followers(blobref1)
  if err != nil {
    t.Fatal(err.String())
  }
  if len(users) != 2 {
    t.Fatalf("Wrong number of users: %v\n", users)
  }
  if (users[0] != "a@b" || users[1] != "foo@bar") && (users[1] != "a@b" || users[0] != "foo@bar") {
    t.Fatal("Wrong users")
  }

  users = perma.FollowersWithPermission(Perm_Read, false)
  if len(users) != 2 {
    t.Fatalf("Wrong number of users: %v\n", users)
  }
  if (users[0] != "a@b" || users[1] != "foo@bar") && (users[1] != "a@b" || users[0] != "foo@bar") {
    t.Fatal("Wrong users")
  }

  users = perma.FollowersWithPermission(Perm_Read | Perm_Invite, false)
  if len(users) != 1 {
    t.Fatalf("Wrong number of users: %v\n", users)
  }
  if users[0] != "a@b" {
    t.Fatal("Wrong users")
  }
}
