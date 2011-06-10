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

func (self *Indexer) HandleBlob(blob []byte, ref BlobRef) {
  mut, err := DecodeMutation(blob, ref)
  if err != nil {
    log.Printf("JSON: %v\n", err)
    return
  }
  // Send the blob to call other network participants
  self.federation.ForwardBlob(blob, ref)
  // Try to apply it
  Build(self, mut)
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