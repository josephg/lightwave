package lightwavegrapher

import (
  "os"
  lst "container/list"
)

type graph struct {
  data map[string]interface{}
  nodes []map[string]interface{}
  nodesByBlobRef map[string]int
}

type SimpleGraphStore struct {
  graphs map[string]*graph
  // The blobrefs of blobs that cannot be processed because they depend on
  // another blob that has not yet been indexed.
  waitingBlobs map[string]bool
  // The blobrefs of blobs which are missing. The value is a list of strings
  // which are the blobrefs of pending mutations.
  waitingLists map[string]*lst.List
  // The blobrefs or blobs that are in the store but not yet indexed
  // because they depend on blobs which are not yet indexed.
  // The value is the number of unsatisfied dependencies.
  pendingBlobs map[string]int
}

func NewSimpleGraphStore() *SimpleGraphStore {
  return &SimpleGraphStore{waitingBlobs: make(map[string]bool), waitingLists: make(map[string]*lst.List), pendingBlobs: make(map[string]int), graphs: make(map[string]*graph)}
}

func (self *SimpleGraphStore) StoreNode(perma_blobref string, blobref string, data map[string]interface{}) os.Error {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    return os.NewError("Unknown perma blob")
  }
  g.nodesByBlobRef[blobref] = len(g.nodes)
  g.nodes = append(g.nodes, data)
  return nil
}

func (self *SimpleGraphStore) StorePermaNode(perma_blobref string, data map[string]interface{}) os.Error {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    g = &graph{data: data, nodesByBlobRef: make(map[string]int)}
    self.graphs[perma_blobref] = g
  } else {
    g.data = data
  }
  return nil
}

func (self *SimpleGraphStore) GetPermaNode(perma_blobref string) (data map[string]interface{}, err os.Error) {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    return nil, nil
  }
  return g.data, nil
}

func (self *SimpleGraphStore) HasOTNodes(perma_blobref string, blobrefs []string) (missing_blobrefs []string, err os.Error) {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    return nil, os.NewError("Unknown perma blob")
  }
  for _, b := range blobrefs {
    if _, ok := g.nodesByBlobRef[b]; !ok {
      missing_blobrefs = append(missing_blobrefs, b)
    }
  }
  return
}

/*
func (self *SimpleGraphStore) GetOTNodeBySeqNumber(perma_blobref string, seqNumber int) (data map[string]interface{}, err os.Error) {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    return nil, os.NewError("Unknown perma blob")
  }
  if seqNumber < 0 || seqNumber >= len(g.nodes) {
    return nil, os.NewError("Index out of bounds")
  }
  return g.nodes[seqNumber]
}
*/

func (self *SimpleGraphStore) GetOTNodeByBlobRef(perma_blobref string, blobref string) (data map[string]interface{}, err os.Error) {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    return nil, os.NewError("Unknown perma blob")
  }
  seq, ok := g.nodesByBlobRef[blobref]
  if !ok {
    return nil, nil
  }
  return g.nodes[seq], nil
}

func (self *SimpleGraphStore) GetOTNodesAscending(perma_blobref string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan map[string]interface{}, err os.Error) {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    return nil, os.NewError("Unknown perma blob")
  }
  
  if startWithSeqNumber < 0 || startWithSeqNumber > int64(len(g.nodes)) {
    return nil, os.NewError("Index out of bounds")
  }
  if endSeqNumber < 0 || endSeqNumber > int64(len(g.nodes)) {
    return nil, os.NewError("Index out of bounds")
  }

  c := make(chan map[string]interface{})
  f := func() {
    for _, data := range g.nodes[startWithSeqNumber: endSeqNumber] {
      c <- data
    }
    close(c)
  }
  go f()
  return c, nil
}

func (self *SimpleGraphStore) GetOTNodesDescending(perma_blobref string) (ch <-chan map[string]interface{}, err os.Error) {
  g, ok := self.graphs[perma_blobref]
  if !ok {
    return nil, os.NewError("Unknown perma blob")
  }

  c := make(chan map[string]interface{})
  f := func() {
    for i := len(g.nodes) - 1; i >= 0; i-- {
      c <- g.nodes[i]
    }
    close(c)
  }
  go f()
  return c, nil
}

func (self *SimpleGraphStore) Enqueue(perma_blobref string, blobref string, dependencies []string) os.Error {
  // Remember the blob
  self.waitingBlobs[blobref] = true
  // For which other blob is 'blobref' waiting?
  for _, dep := range dependencies {
    // Remember that someone is waiting on 'dep'
    l, ok := self.waitingLists[dep]
    if !ok {
      l = lst.New()
      self.waitingLists[dep] = l
    }
    l.PushBack(blobref)
  }
  self.pendingBlobs[blobref] = len(dependencies)
  return nil
}

func (self *SimpleGraphStore) Dequeue(perma_blobref string, waitFor string) (blobrefs []string, err os.Error) {
 // Is any other mutation waiting for 'waitFor'?
  if l, ok := self.waitingLists[waitFor]; ok {
    self.waitingLists[waitFor] = nil, false
    for l.Len() > 0 {
      waiting_id := l.Remove(l.Front()).(string)
      self.pendingBlobs[waiting_id]--
      // The waiting mutation is no waiting for anything anymore -> return it
      if self.pendingBlobs[waiting_id] == 0 {
        self.pendingBlobs[waiting_id] = 0, false
        blobrefs = append(blobrefs, waiting_id)
        self.waitingBlobs[waiting_id] = false, false
      }
    }
  }
  return
}
