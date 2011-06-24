package lightwaveidx

import (
  . "lightwavestore"
  "testing"
  "fmt"
)

func TestPermanode(t *testing.T) {
  store := NewSimpleBlobStore()
  _ = NewIndexer("a@b", store)
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob2 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma2xyz", "perma":"` + blobref1 + `"}`)
  blobref2 := NewBlobRef(blob2)
  
  store.StoreBlob(blob1, blobref1)
  store.StoreBlob(blob2, blobref2)  
}

func TestPermanode2(t *testing.T) {
  store := NewSimpleBlobStore()
  _ = NewIndexer("a@b", store)
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob2 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma2xyz", "perma":"` + blobref1 + `"}`)
  blobref2 := NewBlobRef(blob2)

  // Insert them in the wrong order
  store.StoreBlob(blob2, blobref2)  
  store.StoreBlob(blob1, blobref1)
}

func TestPermanode3(t *testing.T) {
  store := NewSimpleBlobStore()
  indexer := NewIndexer("a@b", store)
  
  blob1 := []byte(`{"type":"permanode", "signer":"a@b", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob2 := []byte(`{"type":"mutation", "perma":"` + blobref1 + `", "site":"site1", "dep":[], "op":{"$t":["Hello World"]}}`)
  blobref2 := NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "perma":"` + blobref1 + `", "site":"site2", "dep":[], "op":{"$t":["Olla!!"]}}`)
  blobref3 := NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "perma":"` + blobref1 + `", "site":"site1", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}}`)
  blobref4 := NewBlobRef(blob4)
  // Grant user foo@bar read access
  blob5 := []byte(`{"type":"permission", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"foo", "domain":"bar", "allow":` + fmt.Sprintf("%v", Perm_Read) + `, "deny":0}`)
  blobref5 := NewBlobRef(blob5)
  // Invite 
  blob6 := []byte(`{"type":"invitation", "signer":"a@b", "perma":"` + blobref1 + `", "user":"foo", "domain":"bar"}`)
  blobref6 := NewBlobRef(blob6)
  // Fake a keep
  blob7 := []byte(`{"type":"keep", "signer":"foo@bar", "invitation":"` + blobref6 + `", "perma":"` + blobref1 + `", "user":"foo", "domain":"bar"}`)
  blobref7 := NewBlobRef(blob7)

  // Insert them in the wrong order
  store.StoreBlob(blob1, blobref1)
  store.StoreBlob(blob2, blobref2)  
  store.StoreBlob(blob3, blobref3)  
  store.StoreBlob(blob4, blobref4)  
  store.StoreBlob(blob5, blobref5)
  store.StoreBlob(blob6, blobref6)
  store.StoreBlob(blob7, blobref7)
  
  allow, err := indexer.HasPermission("a@b", blobref1, Perm_Read)
  if err != nil {
    t.Fatal(err.String())
  }
  if !allow {
    t.Fatal("Expected an allow")
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
    t.Fatal("Expected an allow")
  }

  allow, err = indexer.HasPermission("a@b", blobref1, Perm_Invite | Perm_Expel)
  if err != nil {
    t.Fatal(err.String())
  }
  if !allow {
    t.Fatal("Expected an allow")
  }

  users, err := indexer.Users(blobref1)
  if err != nil {
    t.Fatal(err.String())
  }
  if len(users) != 2 {
    t.Fatal("Wrong number of users")
  }
  if (users[0] != "a@b" || users[1] != "foo@bar") && (users[1] != "a@b" || users[0] != "foo@bar") {
    t.Fatal("Wrong users")
  }
}
