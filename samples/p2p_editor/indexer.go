package main

import (
  . "lightwaveot"
  "log"
)

type IndexerListener interface {
  HandleMutation(mut Mutation)
}

type Indexer struct {
  *SimpleBuilder
  federation *Federation
  listeners []IndexerListener
}

func NewIndexer(store *Store, federation *Federation) *Indexer {
  idx := &Indexer{SimpleBuilder: NewSimpleBuilder(), federation:federation}
  store.AddListener(idx)
  return idx
}

func (self *Indexer) HandleBlob(blob []byte, blobref string) {
//  log.Printf("ID = %v\n", blobref)
  mut, err := DecodeMutation(blob)
  if err != nil {
    log.Printf("JSON: %v\n", err)
    return
  }
  if mut.ID != blobref {
    panic("Something is wrong with the blobref")
  }
  // Send the blob to call other network participants
  self.federation.ForwardBlob(blob, blobref)
//  log.Printf("Frontier: %v\n", self.Frontier())
  // Try to apply it
  Build(self, mut)
//  log.Printf("Frontier: %v\n", self.Frontier())
}

func (self *Indexer) AddListener(l IndexerListener) {
  self.listeners = append(self.listeners, l)
}

func (self *Indexer) Apply(mut Mutation) {
  self.SimpleBuilder.Apply(mut)
  // Inform all listeners
  for _, l := range self.listeners {
    l.HandleMutation(mut)
  }
}