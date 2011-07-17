package lightwavetransformer

import (
  ot "lightwaveot"
  fed "lightwavefed"
  grapher "lightwavegrapher"
  store "lightwavestore"
  "log"
  "os"
  "sync"
)

type Transformer interface {
  SetAPI(api API)
  Repeat(perma_blobref string, startWithSeqNumber int) (err os.Error)
}

type transformer struct {
  userID string
  store store.BlobStore
  fed *fed.Federation
  grapher *grapher.Grapher
  api API
  // The interface{} can contain a ot.Mutation, Keep or Permission struct
  appliedBlobs map[string][]interface{}
  mutex sync.Mutex
}

func NewTransformer(userid string, store store.BlobStore, fed *fed.Federation, grapher *grapher.Grapher) (api_interface Transformer) {
  t := &transformer{userID: userid, store: store, fed: fed, grapher: grapher, appliedBlobs: make(map[string][]interface{})}
  grapher.AddListener(t)
  return t
}

// Interface towards the API
func (self *transformer) SetAPI(api API) {
  self.api = api
}

// Interface towards the API
func (self *transformer) Repeat(perma_blobref string, startWithSeqNumber int) (err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  blobs, ok := self.appliedBlobs[perma_blobref]
  if !ok {
    return os.NewError("Sequence number out of range")
  }
  if startWithSeqNumber > len(blobs) {
    return os.NewError("Sequence number out of range")
  }
  if startWithSeqNumber == len(blobs) {
    return nil
  }
  send_blobs := blobs[startWithSeqNumber:]
  
  f := func() {
    for i, m := range send_blobs {
      switch m.(type) {
      case *Mutation:
	self.api.Blob_Mutation(self, m.(*Mutation), startWithSeqNumber + i)
      case *Keep:
	self.api.Blob_Keep(self, m.(*Keep), startWithSeqNumber + i)
      case *Permission:
	self.api.Blob_Permission(self, m.(*Permission), startWithSeqNumber + i)
      default:
	log.Printf("ERR: %v", m)
	panic("Unknown blob type")
      }
    }
  }
  go f()
  return
}

func (self *transformer) Transform(mutation *grapher.Mutation) (err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  
  var mut ot.Mutation
  switch mutation.Operation.(type) {
  case ot.Operation:
    mut.Operation = mutation.Operation.(ot.Operation)
  case []byte:
    err = mut.Operation.UnmarshalJSON(mutation.Operation.([]byte))
    if err != nil {
      return err
    }
  default:
    panic("Unknown OT operation")
  }
  mut.ID = mutation.MutationBlobRef
  mut.Site = mutation.MutationSigner
  
  // Get the list of mutations applied so far for the perma blob
  app_blobs, ok := self.appliedBlobs[mutation.PermaBlobRef]
  if !ok {
    app_blobs = []interface{}{}
  }
  // Determine which blobs must be rolled back. Skip those which are not mutations
  muts := []ot.Mutation{}
  for _, m := range app_blobs[len(app_blobs) - mutation.Rollback:] {
    if mm, ok := m.(*Mutation); ok {
      muts = append(muts, ot.Mutation{ID:mm.MutationBlobRef, Site:mm.MutationSigner, Operation:mm.Operation.(ot.Operation)})
    }
  }
    
  // Transform 'mut' to apply it locally
  _, pmut, err := ot.TransformSeq(muts, mut)
  if err != nil {
    log.Printf("TRANSFORM ERR: %v", err)
    return err
  }
  
  mutation.Operation = pmut.Operation
  return nil
}

// Interface towards the Grapher
func (self *transformer) Signal_ReceivedInvitation(permission *grapher.Permission) {
  self.api.Signal_ReceivedInvitation(self, (*Permission)(permission))
}

// Interface towards the Grapher
func (self *transformer) Signal_AcceptedInvitation(keep *grapher.Keep) {
  self.api.Signal_AcceptedInvitation(self, (*Keep)(keep))
}

// Interface towards the Grapher
func (self *transformer) Blob_Keep(keep *grapher.Keep, keep_deps []string) {
  app_blobs, ok := self.appliedBlobs[keep.PermaBlobRef]
  if !ok {
    app_blobs = []interface{}{}
  }
  self.appliedBlobs[keep.PermaBlobRef] = append(app_blobs, (*Keep)(keep))
  self.api.Blob_Keep(self, (*Keep)(keep), len(app_blobs))
}

// Interface towards the Grapher
func (self *transformer) Blob_Permission(permission *grapher.Permission, perm_deps []string) {
  app_blobs, ok := self.appliedBlobs[permission.PermaBlobRef]
  if !ok {
    app_blobs = []interface{}{}
  }
  self.appliedBlobs[permission.PermaBlobRef] = append(app_blobs, (*Permission)(permission))
  self.api.Blob_Permission(self, (*Permission)(permission), len(app_blobs))
}

// Interface towards the Grapher
func (self *transformer) Blob_Mutation(mutation *grapher.Mutation) (err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  
  log.Printf("Rollback %v, concurrent %v", mutation.Rollback, mutation.Concurrent)
  var mut ot.Mutation
  switch mutation.Operation.(type) {
  case ot.Operation:
    mut.Operation = mutation.Operation.(ot.Operation)
  case []byte:
    err = mut.Operation.UnmarshalJSON(mutation.Operation.([]byte))
    if err != nil {
      return err
    }
  default:
    panic("Unknown OT operation")
  }
  mut.ID = mutation.MutationBlobRef
  mut.Site = mutation.MutationSigner
  
  // Get the list of mutations applied so far for the perma blob
  app_blobs, ok := self.appliedBlobs[mutation.PermaBlobRef]
  if !ok {
    app_blobs = []interface{}{}
  }
  // Determine which mutations must be rolled back. Skip those which are not valid
  muts := []ot.Mutation{}
  for _, m := range app_blobs[len(app_blobs) - mutation.Rollback:] {
    if mm, ok := m.(*Mutation); ok {
      muts = append(muts, ot.Mutation{ID:mm.MutationBlobRef, Site:mm.MutationSigner, Operation:mm.Operation.(ot.Operation)})
    }
  }

  // Prune all mutations that have been applied locally but do not belong to the history of the new mutation
  prune := map[string]bool{}
  for _, p := range mutation.Concurrent {
    prune[p] = true
  }
  pmuts, e := ot.PruneMutationSeq(muts, prune)
  if e != nil {
    log.Printf("Prune Error: %v\n", e)
    return e
  }
    
  // Transform 'mut' to apply it locally
  pmuts = append(pmuts, mut)
  for _, m := range muts {
    if m.ID != pmuts[0].ID {
      pmuts, _, err = ot.TransformSeq(pmuts, m)
      if err != nil {
	log.Printf("TRANSFORM ERR: %v", err)
	return
      }
    } else {
      pmuts = pmuts[1:]
    }
  }
  
  result := &Mutation{}
  result.PermaBlobRef = mutation.PermaBlobRef
  result.PermaSigner = mutation.PermaSigner
  result.MutationBlobRef = mutation.MutationBlobRef
  result.MutationSigner = mutation.MutationSigner
  result.Operation = pmuts[0].Operation
  app_blobs = append(app_blobs, result)
  self.appliedBlobs[mutation.PermaBlobRef] = app_blobs
  seq := len(app_blobs) - 1
  self.api.Blob_Mutation(self, result, seq)
  
  return nil
  
}
