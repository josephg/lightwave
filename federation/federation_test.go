package lightwavefed

import (
  . "lightwavestore"
  grapher "lightwavegrapher"
  "testing"
  "time"
  "fmt"
  "log"
  "os"
  "http"
  "strings"
)

type dummyAPI struct {
  userID string
  grapher *grapher.Grapher
  t *testing.T
}

func (self *dummyAPI) Signal_ReceivedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode) {
  log.Printf("APP %v: Received Invitation", self.userID)
  // Automatically accept the invitation
  _, err := self.grapher.CreateKeepBlob(perma.BlobRef(), permission.BlobRef())
  if err != nil {
    self.t.Fatal(err.String())
  }
}

func (self *dummyAPI) Signal_AcceptedInvitation(perma grapher.PermaNode, perm grapher.PermissionNode, keep grapher.KeepNode) {
  log.Printf("APP %v: Accepted Invitation", self.userID)
}

func (self *dummyAPI) Blob_Keep(perma grapher.PermaNode, perm grapher.PermissionNode, keep grapher.KeepNode) {
  log.Printf("APP %v: Keep", self.userID)
}

func (self *dummyAPI) Blob_Permission(perma grapher.PermaNode, perm grapher.PermissionNode) {
  log.Printf("APP %v: Permission", self.userID)
}

func (self *dummyAPI) Blob_Mutation(perma grapher.PermaNode, mutation grapher.MutationNode) {
  log.Printf("APP %v: Mutation", self.userID)
}

func (self *dummyAPI) Blob_Entity(perma grapher.PermaNode, entity grapher.EntityNode) {
  log.Printf("APP %v: Entity", self.userID)
}

type dummyNameService struct {
}

func (self *dummyNameService) Lookup(identity string) (url string, err os.Error) {
  i := strings.Index(identity, "@")
  if i == -1 {
    panic("Malformed user ID")
  }
  return fmt.Sprintf("http://%v:8181/fed", identity[i+1:]), nil
//  switch identity {
//  case "a@alice":
//    return "http://localhost:8181/fed", nil
//  case "b@bob":
//    return "http://localhost:8282/fed", nil
//  case "c@charly":
//    return "http://localhost:8383/fed", nil
 // case "d@daisy":
//    return "http://localhost:8484/fed", nil
//  }
//  return "", os.NewError("Unknown identity")
}

func listen(t *testing.T) {
  err := http.ListenAndServe(":8181", nil)
  if err != nil {
    t.Fatal("ListenAndServe: ", err.String())
  }
}

func TestFederation(t *testing.T) {
  ns := &dummyNameService{}
  store1 := NewSimpleBlobStore()
  store2 := NewSimpleBlobStore()
  store3 := NewSimpleBlobStore()
  store4 := NewSimpleBlobStore()
  sg1 := grapher.NewSimpleGraphStore()
  sg2 := grapher.NewSimpleGraphStore()
  sg3 := grapher.NewSimpleGraphStore()
  sg4 := grapher.NewSimpleGraphStore()
  fed1 := NewFederation("a@alice", "alice", 8181, http.DefaultServeMux, ns, store1)
  fed2 := NewFederation("b@bob", "bob", 8181, http.DefaultServeMux, ns, store2)
  fed3 := NewFederation("c@charly", "charly", 8181, http.DefaultServeMux, ns, store3)
  fed4 := NewFederation("d@daisy", "daisy", 8181, http.DefaultServeMux, ns, store4)
  grapher1 := grapher.NewGrapher("a@alice", store1, sg1, fed1)
  grapher2 := grapher.NewGrapher("b@bob", store2, sg2, fed2)
  grapher3 := grapher.NewGrapher("c@charly", store3, sg3, fed3)
  grapher4 := grapher.NewGrapher("d@daisy", store4, sg4, fed4)
  store1.AddListener(grapher1)
  store2.AddListener(grapher2)
  store3.AddListener(grapher3)
  store4.AddListener(grapher4)
  app1 := &dummyAPI{"a@alice", grapher1, t}
  grapher1.SetAPI(app1)
  app2 := &dummyAPI{"b@bob", grapher2, t}
  grapher2.SetAPI(app2)
  app3 := &dummyAPI{"c@charly", grapher3, t}
  grapher3.SetAPI(app3)
  app4 := &dummyAPI{"d@daisy", grapher4, t}
  grapher4.SetAPI(app4)
  go listen(t)
  
  time.Sleep(1000000000 * 2)

  blob1 := []byte(`{"type":"permanode", "signer":"a@alice", "random":"perma1abc"}`)
  blobref1 := NewBlobRef(blob1)
  blob1b := []byte(`{"type":"keep", "signer":"a@alice", "perma":"` + blobref1 + `"}`)
  blobref1b := NewBlobRef(blob1b)  
  blob1c := []byte(`{"type":"entity", "signer":"a@b", "perma":"` + blobref1 + `", "content":"", "dep":["` + blobref1b + `"]}`)
  blobref1c := NewBlobRef(blob1c)
  blob2 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "dep":["` + blobref1c + `"], "op":{"$t":["Hello World"]}, "entity":"` + blobref1c + `"}`)
  blobref2 := NewBlobRef(blob2)
  blob3 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "dep":["` + blobref1c + `"], "op":{"$t":["Olla!!"]}, "entity":"` + blobref1c + `"}`)
  blobref3 := NewBlobRef(blob3)
  blob4 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "??"]}, "entity":"` + blobref1c + `"}`)
  blobref4 := NewBlobRef(blob4)
  // Grant user foo@bar read access
  blob5 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"b@bob", "allow":` + fmt.Sprintf("%v", grapher.Perm_Read) + `, "deny":0}`)
  blobref5 := NewBlobRef(blob5)
  blob7 := []byte(`{"type":"mutation", "signer":"a@alice", "perma":"` + blobref1 + `", "dep":["` + blobref2 + `"], "op":{"$t":[{"$s":11}, "!!"]}, "entity":"` + blobref1c + `"}`)
  blobref7 := NewBlobRef(blob7)
  blob8 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"c@charly", "allow":` + fmt.Sprintf("%v", grapher.Perm_Read) + `, "deny":0}`)
  blobref8 := NewBlobRef(blob8)
  blob9 := []byte(`{"type":"permission", "signer":"a@alice", "action":"invite", "perma":"` + blobref1 + `", "dep":["` + blobref4 + `"], "user":"d@daisy", "allow":` + fmt.Sprintf("%v", grapher.Perm_Read) + `, "deny":0}`)
  blobref9 := NewBlobRef(blob9)

  log.Printf("Hashes:\n1: %v\n1b: %v\n2: %v\n3: %v\n4: %v\n5: %v\n7: %v\n8: %v\n", blobref1, blobref1b, blobref2, blobref3, blobref4, blobref5, blobref7, blobref8)
  
  // Insert them in the wrong order
  store1.StoreBlob(blob1, blobref1)
  store1.StoreBlob(blob1b, blobref1b)
  store1.StoreBlob(blob1c, blobref1c)
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