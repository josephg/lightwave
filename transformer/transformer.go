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

func decodeMutation(mutation grapher.MutationNode) (mut ot.StringMutation, err os.Error) {
  switch mutation.Operation().(type) {
  case []ot.StringOperation:
    mut.Operations = mutation.Operation().([]ot.StringOperation)
  case []byte:
    mut.Operations, err = ot.UnmarshalStringOperations(mutation.Operation().([]byte))
    if err != nil {
      return mut, err
    }
  default:
    panic("Unknown OT operation")
  }
  mut.Id = mutation.Signer() + "/" + mutation.BlobRef()
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

  muts := make([]ot.StringMutation, 0)
  for m := range rollback {
    m3, e := decodeMutation(m)
    if e != nil {
      log.Printf("Err: Decoding 2")
      return e
    }
    muts = append(muts, m3)
  }
    
  // Transform 'mut' to apply it locally
  _, pmut, err := transformSeq(muts, mut)
  if err != nil {
    log.Printf("TRANSFORM ERR: %v", err)
    return err
  }
  
  bytes, err := ot.MarshalStringOperations(pmut.Operations)
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

  muts := make([]ot.StringMutation, 0)
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
  pmuts, e := ot.PruneStringMutationSeq(muts, prune)
  if e != nil {
    log.Printf("Prune Error: %v\n", e)
    return e
  }
    
  // Transform 'mut' to apply it locally
  pmuts = append(pmuts, mut)
  for _, m := range muts {
    if m.Id != pmuts[0].Id {
      pmuts, _, err = transformSeq(pmuts, m)
      if err != nil {
	log.Printf("TRANSFORM ERR: %v", err)
	return
      }
    } else {
      pmuts = pmuts[1:]
    }
  }
  
  bytes, err := ot.MarshalStringOperations(pmuts[0].Operations)
  if err != nil {
    panic("Cannot serlialize")
  }
  mutation.SetOperation(bytes)
  return nil  
}

// Transform two mutations
func transform(m1 ot.StringMutation, m2 ot.StringMutation) (tm1 ot.StringMutation, tm2 ot.StringMutation, err os.Error) {
  tm1 = m1
  tm2 = m2
  if m1.Id == m2.Id {
  // If the IDs are equal, return empty mutations
  } else if m1.Id < m2.Id {
    tm1.Operations, tm2.Operations, err = ot.TransformStringOperations(m1.Operations, m2.Operations)
  } else {
    tm2.Operations, tm1.Operations, err = ot.TransformStringOperations(m2.Operations, m1.Operations)
  }
  return
}

func transformSeq(muts []ot.StringMutation, mut ot.StringMutation) (tmuts []ot.StringMutation, tmut ot.StringMutation, err os.Error) {
  tmut = mut
  for _, m := range muts {
    m, tmut, err = transform(m, tmut)
    if err != nil {
      return
    }
    tmuts = append(tmuts, m)
  }
  return
}
