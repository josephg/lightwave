package lightwaveot

import (
  "log"
  "os"
  lst "container/list"
)

// The builder is used by the Build function as a storage backend.
// Real-world applications should store the data in a database.
// SimpleBuilder holds everything in memory
type Builder interface {
  HasApplied(id string) bool
  Enqueue(mut Mutation, waitFor []string)
  Dequeue(waitFor string) []Mutation
  History() <-chan Mutation
  Apply(mut Mutation)
  Frontier() Frontier
}

// -------------------------------------------
// SimpleBuilder

type SimpleBuilder struct {
  // The original mutations indexed by their ID.
  waitingBlobs map[string]Mutation
  // The applied (and transformed) mutations
  appliedBlobs map[string]Mutation
  // The current frontier
  frontier Frontier
  // An ordered list of applied mutation IDs.
  // The most recent mutation is at the end of the list.
  // These mutations are transformed
  mutations []string
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

func NewSimpleBuilder() *SimpleBuilder {
  b := &SimpleBuilder{mutationsByID: make(map[string]bool), waitingLists: make(map[string]*lst.List), pendingMutations: make(map[string]int), frontier: make(Frontier), waitingBlobs: make(map[string]Mutation), appliedBlobs: make(map[string]Mutation)}
  return b
}

// An ordered list of applied mutation IDs.
// The most recent mutation is at the end of the list.
func (self *SimpleBuilder) AppliedMutationIDs() []string {
  return self.mutations
}

func (self *SimpleBuilder) AppliedMutation(id string) Mutation {
  return self.appliedBlobs[id]
}

// Implements the Builder interface
func (self *SimpleBuilder) Frontier() Frontier {
  return self.frontier
}

// Implements the Builder interface
func (self *SimpleBuilder) HasApplied(id string) (has bool) {
  _, has = self.mutationsByID[id]
  return
}

// Implements the Builder interface
func (self *SimpleBuilder) Enqueue(mut Mutation, deps []string) {
  // Remember the mutation
  self.waitingBlobs[mut.ID] = mut
  // For which other mutation is 'mut' waiting?
  for _, dep := range deps {
    // Remember that someone is waiting on 'dep'
    l, ok := self.waitingLists[dep]
    if !ok {
      l = lst.New()
      self.waitingLists[dep] = l
    }
    l.PushBack(mut.ID)
  }
  self.pendingMutations[mut.ID] = len(deps)
}

// Implements the Builder interface
func (self *SimpleBuilder) Dequeue(waitFor string) (muts []Mutation) {
  // Is any other mutation waiting for 'waitFor'?
  if l, ok := self.waitingLists[waitFor]; ok {
    self.waitingLists[waitFor] = nil, false
    for l.Len() > 0 {
      waiting_id := l.Remove(l.Front()).(string)
      self.pendingMutations[waiting_id]--
      // The waiting mutation is no waiting for anything anymore -> return it
      if self.pendingMutations[waiting_id] == 0 {
	self.pendingMutations[waiting_id] = 0, false
	muts = append(muts, self.waitingBlobs[waiting_id])
	self.waitingBlobs[waiting_id] = Mutation{}, false
      }
    }
  }
  return
}
  
// Implements the Builder interface
func (self *SimpleBuilder) History() <-chan Mutation {
  ch := make(chan Mutation)
  f := func() {
    for i := len(self.mutations) - 1; i >= 0; i-- {
      ch <- self.appliedBlobs[self.mutations[i]]
    }
    close(ch)
  }
  go f()
  return ch
}

// Implements the Builder interface
func (self *SimpleBuilder) Apply(mut Mutation) {
  self.appliedBlobs[mut.ID] = mut
  self.mutations = append(self.mutations, mut.ID)
  self.mutationsByID[mut.ID] = true
  self.frontier.Add(mut)
}

// -------------------------------------------
// Build

func Build(builder Builder, mut Mutation) (applied bool, err os.Error) {
  // The mutation has already been applied?
  if builder.HasApplied(mut.ID) {
    return true, nil
  }
  // Are all dependencies satisfied, i.e. are all mutations
  // on which mut depends already processed by the builder?
  unsatisfied := false
  deps := []string{}
  for _, dep := range mut.Dependencies {
    if !builder.HasApplied(dep) {
      unsatisfied = true
      deps = append(deps, dep)
    }
  }
  if unsatisfied {
    builder.Enqueue(mut, deps)
    return false, nil
  }

  // Find out how far back we have to go in history to find a common anchor point for transformation
  frontier := builder.Frontier()
  h := NewHistoryGraph(frontier, mut.Dependencies)
  reverse_muts := []Mutation{}
  prune := map[string]bool{}
  // Need to rollback?
  if !h.Test() {
    // Go back in history until our history is equal with that of 'mut'.
    // On the way remember which mutations of our history do not belong to the
    // history of 'mut' because these must be pruned.
    for history_mut := range builder.History() {
      if !h.Substitute(history_mut) {
	prune[history_mut.ID] = true
      }
      reverse_muts = append(reverse_muts, history_mut)
      if h.Test() {
	break
      }
    }
  }

  // Reverse the mutation history, such that oldest are first in the list.
  // This is ugly but prepending in the above loops is too slow.
  muts := make([]Mutation, len(reverse_muts))
  for i := 0; i < len(muts); i++ {
    muts[i] = reverse_muts[len(reverse_muts) - 1 - i]
  }
  
  // Prune all mutations that have been applied locally but do not belong to the history of 'mut'
  pmuts, e := PruneSeq(muts, prune)
  if e != nil {
    return false, e
  }
  
  // Transform 'mut' to apply it locally
  pmuts = append(pmuts, mut)
  for _, m := range muts {
    if m.ID != pmuts[0].ID {
      pmuts, _, err = TransformSeq(pmuts, m)
      if err != nil {
	log.Printf("TRANSFORM ERR: %v", err)
	return
      }
    } else {
      pmuts = pmuts[1:]
    }
  }
  mut = pmuts[0]
  
  builder.Apply(mut)
  
  // Process all mutations that had have been waiting for 'mut'
  for _, m := range builder.Dequeue(mut.ID) {
    Build(builder, m)
  }
  return
}
