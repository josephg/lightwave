package lightwaveidx

import (
  ot "lightwaveot"
  "log"
  "os"
)

type otHistory struct {
  // An ordered list of applied blob references.
  // The most recent blob is at the end of the list.
  // These blobs may have been transformed before they have been applied  
  // TODO: This is a LARGE data structure. Do not keep it in memory ...
  appliedBlobs []string
  // The applied (and transformed) mutations
  // TODO: This is a LARGE data structure. Do not keep it in memory ...
  members map[string]otNode
  // The current frontier
  frontier ot.Frontier
  // The current content of the document
  // TODO: This is a LARGE data structure. Do not keep it in memory ...
  content interface{}
  permissions map[string]int
}

func newOTHistory() *otHistory {
  return &otHistory{frontier: make(ot.Frontier), members: make(map[string]otNode), permissions:make(map[string]int)}
}

func (self *otHistory) HasApplied(blobref string) bool {
  if _, ok := self.members[blobref]; ok {
    return true
  }
  return false
}

// An ordered list of applied mutation IDs.
// The most recent mutation is at the end of the list.
func (self *otHistory) AppliedBlobs() []string {
  return self.appliedBlobs
}

// Implements the Builder interface
func (self *otHistory) Frontier() ot.Frontier {
  return self.frontier
}

// Implements the Builder interface
func (self *otHistory) History(reverse bool) <-chan interface{} {
  ch := make(chan interface{})
  if reverse {
    f := func() {
      for i := len(self.appliedBlobs) - 1; i >= 0; i-- {
	ch <- self.members[self.appliedBlobs[i]]
      }
      close(ch)
    }
    go f()
    return ch
  }
  f := func() {
    for _, id := range self.appliedBlobs {
      ch <- self.members[id]
    }
    close(ch)
  }
  go f()
  return ch
}

func (self *otHistory) Apply(newnode otNode) (deps []string, err os.Error) {
  // The mutation has already been applied?
  if self.HasApplied(newnode.BlobRef()) {
    return
  }
  // Are all dependencies satisfied, i.e. are all mutations
  // on which mut depends already processed by the builder?
  unsatisfied := false
  for _, dep := range newnode.Dependencies() {
    if !self.HasApplied(dep) {
      unsatisfied = true
      deps = append(deps, dep)
    }
  }
  if unsatisfied {
    return deps, nil
  }

  // Find out how far back we have to go in history to find a common anchor point for transformation
  frontier := self.Frontier()
  h := ot.NewHistoryGraph(frontier, newnode.Dependencies())
  reverse_nodes := []otNode{}
  prune := map[string]bool{}
  // Need to rollback?
  if !h.Test() {
    // Go back in history until our history is equal with that of 'mut'.
    // On the way remember which mutations of our history do not belong to the
    // history of 'mut' because these must be pruned.
    for x := range self.History(true) {
      history_node := x.(otNode)
      if !h.SubstituteBlob(history_node.BlobRef(), history_node.Dependencies()) {
	prune[history_node.BlobRef()] = true
      }
      reverse_nodes = append(reverse_nodes, history_node)
      if h.Test() {
	break
      }
    }
  }

  // Reverse the mutation history, such that oldest are first in the list.
  // This is ugly but prepending in the above loops is too slow.
  nodes := make([]otNode, len(reverse_nodes))
  for i := 0; i < len(nodes); i++ {
    nodes[i] = reverse_nodes[len(reverse_nodes) - 1 - i]
  }
  
  // Prune all mutations that have been applied locally but do not belong to the history of the new mutation
  pnodes, e := pruneSeq(nodes, prune)
  if e != nil {
    err = e
    return
  }
    
  // Transform 'mut' to apply it locally
  pnodes = append(pnodes, newnode)
  for _, n := range nodes {
    if n.BlobRef() != pnodes[0].BlobRef() {
      pnodes, _, err = transformSeq(pnodes, n)
      if err != nil {
	log.Printf("TRANSFORM ERR: %v", err)
	return
      }
    } else {
      pnodes = pnodes[1:]
    }
  }
  newnode = pnodes[0]
  
  // Apply the mutation
  if mut, ok := newnode.(*mutationNode); ok {
    mut.mutation.AppliedAt = len(self.appliedBlobs)
  }
  self.appliedBlobs = append(self.appliedBlobs, newnode.BlobRef())
  self.members[newnode.BlobRef()] = newnode
  self.frontier.AddBlob(newnode.BlobRef(), newnode.Dependencies())
  log.Printf("Applied blob %v\n", newnode.BlobRef())
  
  if mut, ok := newnode.(*mutationNode); ok {
    self.content, err = ot.Execute(self.content, mut.mutation)
  } else if perm, ok := newnode.(*permissionNode); ok {
    userid := perm.permission.User + "@" + perm.permission.Domain
    bits, ok := self.permissions[userid]
    if !ok {
      bits = 0
    }
    bits, err = ot.ExecutePermission(bits, perm.permission)
    if err == nil {
      self.permissions[userid] = bits
    }
  }
  return
}
