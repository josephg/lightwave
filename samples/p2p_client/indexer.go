package main

import (
  . "lightwaveot"
  "os"
  "log"
)

type IndexerListener interface {
  HandleMutation(mut Mutation)
}

type Indexer struct {
  serverVersion int
  mutationQueue []Mutation
  mutationInFlight Mutation
  listeners []IndexerListener
  csProto *CSProtocol
  site string
}

func NewIndexer() *Indexer {
  idx := &Indexer{site: uuid()}
  return idx
}

func (self *Indexer) SetCSProtocol(csProto *CSProtocol) {
  self.csProto = csProto
}

func (self *Indexer) HandleClientMutation(mut Mutation) {
  mut.Site = self.site
  self.Apply(mut)
  // Is there a mutation in-flight? -> enqueue any further mutations
  if self.mutationInFlight.Operation.Kind != NoOp {
    self.mutationQueue = append(self.mutationQueue, mut)
  } else {
    self.mutationInFlight = mut
    self.mutationInFlight.AppliedAt = self.serverVersion
    self.csProto.SendMutation(self.mutationInFlight)
  }
}

func (self *Indexer) HandleServerMutation(mut Mutation) (err os.Error) {
  log.Printf("Read from server\n")
  // Is this a server ACK?
  if mut.Site == self.site {
    log.Printf("\tAck\n")
    if self.mutationInFlight.Operation.Kind == NoOp {
      return os.NewError("Did not expect a server ACK")
    }
    self.mutationInFlight = Mutation{}
    self.serverVersion = mut.AppliedAt + 1
    if len(self.mutationQueue) > 0 {
      mut := self.mutationQueue[0]
      mut.AppliedAt = self.serverVersion
      self.csProto.SendMutation(mut)
      // TODO: On the long run this will leak memory.
      self.mutationQueue = self.mutationQueue[1:]
    }
    return
  }
  // This server-sent mutation must be transformed against locally queued mutations
  tmut := mut
  if self.mutationInFlight.Operation.Kind != NoOp {
    tmut, _, err = Transform(mut, self.mutationInFlight)
    if err != nil {
      return os.NewError("Transformation Error")
    }
  }
  self.mutationQueue, tmut, err = TransformSeq(self.mutationQueue, tmut)
  if err != nil {
    return os.NewError("Transformation Error")
  }
  self.serverVersion = mut.AppliedAt + 1  
  self.Apply(tmut)
  return
}

func (self *Indexer) AddListener(l IndexerListener) {
  self.listeners = append(self.listeners, l)
}

func (self *Indexer) Apply(mut Mutation) {
  // Inform all listeners
  for _, l := range self.listeners {
    l.HandleMutation(mut)
  }
}
