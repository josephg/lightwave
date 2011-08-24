package lightwavetransformer

import (
  ot "lightwaveot"
  grapher "lightwavegrapher"
  "log"
  "os"
)

type transformer struct {
  grapher *grapher.Grapher
}

func NewTransformer(grapher *grapher.Grapher) grapher.Transformer {
  t := &transformer{grapher: grapher}
  grapher.AddTransformer(t)
  return t
}

func decodeMutation(mutation grapher.MutationNode) (mut ot.Mutation, err os.Error) {
  switch mutation.Operation().(type) {
  case ot.Operation:
    mut.Operation = mutation.Operation().(ot.Operation)
  case []byte:
    err = mut.Operation.UnmarshalJSON(mutation.Operation().([]byte))
    if err != nil {
      return mut, err
    }
  default:
    panic("Unknown OT operation")
  }
  mut.ID = mutation.BlobRef()
  mut.Site = mutation.Signer()
  return
}

func (self *transformer) Kind() int {
  return grapher.TransformationMerge
}

func (self *transformer) DataType() int {
  return grapher.TypeString
}

// Interface towards the Grapher
func (self *transformer) TransformClientMutation(mutation grapher.MutationNode, rollback <-chan grapher.MutationNode) (err os.Error) {
  mut, e := decodeMutation(mutation)
  if e != nil {
    log.Printf("Err: Decoding")
    return e
  }

  muts := make([]ot.Mutation, 0)
  for m := range rollback {
    m3, e := decodeMutation(m)
    if e != nil {
      log.Printf("Err: Decoding 2")
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
  
  bytes, err := pmut.Operation.MarshalJSON()
  if err != nil {
    panic("Cannot serlialize")
  }
  mutation.SetOperation(bytes)
  return nil
}

// Interface towards the Grapher
func (self *transformer) TransformMutation(mutation grapher.MutationNode, rollback <-chan grapher.MutationNode, concurrent []string) (err os.Error) {
  mut, e := decodeMutation(mutation)
  if e != nil {
    return e
  }

  muts := make([]ot.Mutation, 0)
  for m := range rollback {
    m3, e := decodeMutation(m)
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
  
  bytes, err := pmuts[0].Operation.MarshalJSON()
  if err != nil {
    panic("Cannot serlialize")
  }
  mutation.SetOperation(bytes)
  return nil  
}
