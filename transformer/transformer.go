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
  RepeatMutations(perma_blobref string, startWithSeqNumber int) (err os.Error)
  Frontier(perma_blobref string) (blobrefs []string, err os.Error)
}

type transformer struct {
  userID string
  store store.BlobStore
  fed *fed.Federation
  grapher *grapher.Grapher
  api API
  appliedMutations map[string][]ot.Mutation
  seqNumbers map[string]int
  mutex sync.Mutex
}

func NewTransformer(userid string, store store.BlobStore, fed *fed.Federation, grapher *grapher.Grapher) (api_interface Transformer) {
  t := &transformer{userID: userid, store: store, fed: fed, grapher: grapher, appliedMutations: make(map[string][]ot.Mutation), seqNumbers: make(map[string]int)}
  grapher.AddListener(t)
  return t
}

// Interface towards the API
func (self *transformer) SetAPI(api API) {
  self.api = api
}

func (self *transformer) Frontier(perma_blobref string) (blobrefs []string, err os.Error) {
  return self.grapher.Frontier(perma_blobref)
}

// Interface towards the API
func (self *transformer) RepeatMutations(perma_blobref string, startWithSeqNumber int) (err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  muts, ok := self.appliedMutations[perma_blobref]
  if !ok {
    return os.NewError("Sequence number out of range")
  }
  max_seq, ok := self.seqNumbers[perma_blobref]
  if !ok {
    max_seq = 0
  }
  if startWithSeqNumber > max_seq {
    return os.NewError("Sequence number out of range")
  }
  if startWithSeqNumber == max_seq {
    return nil
  }
  seq_count := 0
  send_muts := make([]interface{}, max_seq - startWithSeqNumber)
  for i := len(muts) - 1; seq_count < max_seq - startWithSeqNumber; i-- {
    mut := muts[i]
    if mut.ID != "" {
      seq_count++
      send_muts[len(send_muts) - seq_count] = mut
    }
  }
  
  f := func() {
    for i, m := range send_muts {
      self.api.Mutation(self, perma_blobref, m, startWithSeqNumber + i)
    }
  }
  go f()
  return
}

// Interface towards the Grapher
func (self *transformer) Invitation(permanode_blobref, invitation_blobref string, userid string) {
  self.api.Invitation(self, permanode_blobref, invitation_blobref, userid)
}

// Interface towards the Grapher
func (self *transformer) AcceptedInvitation(permanode_blobref, invitation_blobref string, keep_blobref string) {
  self.api.AcceptedInvitation(self, permanode_blobref, invitation_blobref, keep_blobref)
}

// Interface towards the Grapher
func (self *transformer) NewFollower(permanode_blobref, invitation_blobref, keep_blobref, userid string) {
  self.api.NewFollower(self, permanode_blobref, invitation_blobref, keep_blobref, userid)
}

// Interface towards the Grapher
func (self *transformer) PermaNode(blobref, invitation_blobref, keep_blobref string) {
  self.api.PermaNode(self, blobref, invitation_blobref, keep_blobref)
}

// Interface towards the Grapher
func (self *transformer) Permission(blobref string, action int, permission ot.Permission) {
  self.api.Permission(self, blobref, action, permission)
}

// Interface towards the Grapher
func (self *transformer) Mutation(blobref, mut_blobref string, mutation []byte, rollback int, concurrent []string) {
  mut, err := self.handleMutation(blobref, mut_blobref, mutation, rollback, concurrent)
  if err != nil {
    self.mutex.Lock()
    defer self.mutex.Unlock()
    log.Printf("ERR Transformer: %v\n", err)
    app_muts, ok := self.appliedMutations[blobref]
    if !ok {
      app_muts = []ot.Mutation{}
    }
    self.appliedMutations[blobref] = append(app_muts, ot.Mutation{})
    return
  } else {
    seqNumber := 0
    if i, ok := self.seqNumbers[blobref]; ok {
      seqNumber = i
    }
    self.api.Mutation(self, blobref, mut, seqNumber)
    self.seqNumbers[blobref] = seqNumber + 1
  }
}

func (self *transformer) handleMutation(blobref, mut_blobref string, mutation []byte, rollback int, concurrent []string) (result ot.Mutation, err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  
  var mut ot.Mutation
  err = mut.UnmarshalJSON(mutation)
  if err != nil {
    return ot.Mutation{}, err
  }
  mut.ID = mut_blobref

  // Get the list of mutations applied so far for the perma blob
  app_muts, ok := self.appliedMutations[blobref]
  if !ok {
    app_muts = []ot.Mutation{}
  }
  // Determine which mutations must be rolled back. Skip those which are not valid
  muts := []ot.Mutation{}
  for _, m := range app_muts[len(app_muts) - rollback:] {
    if m.ID != "" {
      muts = append(muts, m)
    }
  }

  // Prune all mutations that have been applied locally but do not belong to the history of the new mutation
  prune := map[string]bool{}
  for _, p := range concurrent {
    prune[p] = true
  }
  pmuts, e := ot.PruneMutationSeq(muts, prune)
  if e != nil {
    log.Printf("Prune Error: %v\n", e)
    return ot.Mutation{}, e
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
  
  app_muts = append(app_muts, pmuts[0])
  self.appliedMutations[blobref] = app_muts
  return pmuts[0], nil
}
