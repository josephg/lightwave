package main

import (
  . "lightwave/ot"
  . "lightwave/store"
  "log"
  "errors"
)

type IndexerListener interface {
  HandleMutation(mut Mutation)
}

type Indexer struct {
  *SimpleBuilder
  store BlobStore
  listeners []IndexerListener
}

func NewIndexer(store BlobStore) *Indexer {
  idx := &Indexer{SimpleBuilder: NewSimpleBuilder(), store: store}
  store.AddListener(idx)
  return idx
}

func (self *Indexer) HandleClientMutation(mut Mutation) (err error) {
  appliedAt := 0
  // Fetch all mutations which are newer than mut.AppliedAt
  history_muts := []Mutation{}
  for history_mut := range self.History(true) {
    appliedAt = max(history_mut.AppliedAt + 1, appliedAt)
    if history_mut.AppliedAt + 1 == mut.AppliedAt {
      break
    }
    history_muts = append(history_muts, history_mut)
  }
  // Transform
  _, tmut, err := TransformSeq(history_muts, mut)
  // Fix the Dependencies field
  tmut.Dependencies = self.Frontier().IDs()
  blob, blobref, err := EncodeMutation(tmut, EncNormal)
  if err != nil {
    panic("FAILED encoding a mutation")
  }
  tmut.ID = blobref
  // Apply it
  self.Apply(&tmut)
  // Store the blob. This will call back into the indexer, but since the mutation
  // has already been allplied, nothing bad will happen
  self.store.StoreBlob(blob, blobref)
  return
}

func (self *Indexer) HandleBlob(blob []byte, blobref string) error {
  mut, err := DecodeMutation(blob)
  if err != nil {
    log.Printf("JSON ERR: %v\n", err)
    return nil
  }
  if mut.ID != blobref {
    return errors.New("Something is wrong with the blobref")
  }
  // Try to apply it
  Build(self, mut)
  return nil
}

func (self *Indexer) AddListener(l IndexerListener) {
  self.listeners = append(self.listeners, l)
}

func (self *Indexer) Apply(mut *Mutation) {
  self.SimpleBuilder.Apply(mut)
  // Inform all listeners
  for _, l := range self.listeners {
    l.HandleMutation(*mut)
  }
}

// Helper function
func max(a, b int) int {
  if a > b {
    return a
  }
  return b
}
