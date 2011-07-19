package lightwavetransformer

import (
  ot "lightwaveot"
  grapher "lightwavegrapher"
  "log"
  "os"
)

type transformer struct {
  grapher *grapher.Grapher
  // The interface{} can contain a ot.Mutation, Keep or Permission struct
//  appliedBlobs map[string][]interface{}
}

func NewTransformer(grapher *grapher.Grapher) grapher.Transformer {
  t := &transformer{grapher: grapher /*, appliedBlobs: make(map[string][]interface{})*/ }
  grapher.SetTransformer(t)
  return t
}

func decodeMutation(mutation *grapher.Mutation) (mut ot.Mutation, err os.Error) {
  switch mutation.Operation.(type) {
  case ot.Operation:
    mut.Operation = mutation.Operation.(ot.Operation)
  case []byte:
    err = mut.Operation.UnmarshalJSON(mutation.Operation.([]byte))
    if err != nil {
      return mut, err
    }
  default:
    panic("Unknown OT operation")
  }
  mut.ID = mutation.MutationBlobRef
  mut.Site = mutation.MutationSigner
  return
}

// Interface towards the Grapher
func (self *transformer) TransformClientMutation(mutation *grapher.Mutation, rollback <-chan interface{}) (err os.Error) {
  mut, e := decodeMutation(mutation)
  if e != nil {
    return e
  }

  muts := make([]ot.Mutation, 0)
  for m := range rollback {
    m2, ok := m.(*grapher.Mutation)
    if !ok {
      continue
    }
    m3, e := decodeMutation(m2)
    if e != nil {
      return e
    }
    muts = append(muts, m3)
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
func (self *transformer) TransformMutation(mutation *grapher.Mutation, rollback <-chan interface{}, concurrent []string) (err os.Error) {
  mut, e := decodeMutation(mutation)
  if e != nil {
    return e
  }

  muts := make([]ot.Mutation, 0)
  for m := range rollback {
    m2, ok := m.(*grapher.Mutation)
    if !ok {
      continue
    }
    m3, e := decodeMutation(m2)
    if e != nil {
      return e
    }
    muts = append(muts, m3)
  }

  // Prune all mutations that have been applied locally but do not belong to the history of the new mutation
  prune := map[string]bool{}
  for _, p := range concurrent {
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
  
  mutation.Operation = pmuts[0].Operation
  return nil  
}
