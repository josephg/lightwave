package main

import (
  . "lightwaveot"
  "log"
  lst "container/list"
)

type IndexerListener interface {
  HandleMutation(mut Mutation)
}

type Indexer struct {
  frontier Frontier
  store *Store
  federation *Federation
  listeners []IndexerListener
  // An ordered list of applied mutations.
  // The most recent mutation is at the end of the list
  mutations []Mutation
  // The set of mutation IDs that has already been applied
  mutationsByID map[string]bool
  // The IDs of mutations which are missing. The value is a list of strings
  // which are the IDs of pending mutations.
  waitingLists map[string]*lst.List
  // The IDs of mutations that are in the store but not yet applied
  // because they depend on mutations which are not yet applied.
  // The value is the number of unsatisfied dependencies.
  pendingMutations map[string]int
}

func NewIndexer(store *Store, federation *Federation) *Indexer {
  idx := &Indexer{store: store, federation:federation, mutationsByID: make(map[string]bool), waitingLists: make(map[string]*lst.List), pendingMutations: make(map[string]int), frontier: make(Frontier)}
  store.AddListener(idx)
  return idx
}

func (self *Indexer) HandleBlob(blob []byte, ref BlobRef) {
  mut, err := DecodeMutation(blob, ref)
  if err != nil {
    log.Printf("JSON: %v\n", err)
    return
  }
  // Send the blob to call other network participants
  self.federation.ForwardBlob(blob, ref)
  // Try to apply it
  self.handleMutation(mut)
}

func (self *Indexer) handleMutation(mut Mutation) {
  // Are all dependencies satisfied?
  unsatisfied := 0
  for _, dep := range mut.Dependencies {
    if _, ok := self.mutationsByID[dep]; !ok {
      unsatisfied++
      l, ok := self.waitingLists[dep]
      if !ok {
	l = lst.New()
	self.waitingLists[dep] = l
      }
      l.PushBack(mut.ID)
    }
  }
  if unsatisfied > 0 {
    self.pendingMutations[mut.ID] = unsatisfied
    return
  }
  
  log.Printf("APPLYING %v\n", mut.ID)
  
  // Find out how far back we have to go to find a common anchor point for transformation
  h := NewHistoryGraph(self.frontier, mut.Dependencies)
  rollback := 0
  prune := map[string]bool{}
  for i := len(self.mutations) - 1; i >= 0 && !h.Test(); i-- {
    history_mut := self.mutations[i]
//    ref, _ := DecodeBlobRef(self.mutations[i])
//    blob, _ := self.store.GetBlob(ref)
//    history_mut, _ := DecodeMutation(blob, ref)
    if !h.Substitute(history_mut) {
      prune[history_mut.ID] = true
    }
//    muts = append(muts, history_mut)
    rollback++
  }

  log.Printf("Have to rollback %v and prune %v\n", rollback, prune)

  // Prune all mutations that have been applied locally but do not belong to the history of 'mut'
  muts := self.mutations[len(self.mutations) - rollback:]
  pmuts, err := PruneSeq(muts, prune)
  if err != nil {
    log.Printf("PRUNE ERROR %v\n", err)
    return
  }
  
  // Transform 'mut' to apply it locally
  pmuts = append(pmuts, mut)
  for _, m := range muts {
    if m.ID != pmuts[0].ID {
      pmuts, err = TransformSeq(pmuts, m)
      if err != nil {
	log.Printf("TRANSFORM ERR: %v", err)
	return
      }
    } else {
      pmuts = pmuts[1:]
    }
  }
  mut = pmuts[0]
  
  self.mutations = append(self.mutations, mut)
  self.mutationsByID[mut.ID] = true
  self.frontier.Add(mut)
  for _, l := range self.listeners {
    l.HandleMutation(mut)
  }
  
  // Did other mutations wait for this one?
  if l, ok := self.waitingLists[mut.ID]; ok {
    self.waitingLists[mut.ID] = nil, false
    for l.Len() > 0 {
      waiting_id := l.Remove(l.Front()).(string)
      self.pendingMutations[waiting_id]--
      if self.pendingMutations[waiting_id] == 0 {
	self.pendingMutations[waiting_id] = 0, false
	ref, _ := DecodeBlobRef(waiting_id)
	blob, _ := self.store.GetBlob(ref)
	waiting_mut, _ := DecodeMutation(blob, ref)
	self.handleMutation(waiting_mut)
      }
    }
  }
}

func (self *Indexer) AddListener(l IndexerListener) {
  self.listeners = append(self.listeners, l)
}
