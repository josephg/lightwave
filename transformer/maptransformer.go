package lightwavetransformer

import (
  grapher "lightwavegrapher"
  "log"
  "os"
  "json"
)

type mapTransformer struct {
  grapher *grapher.Grapher
}

type mapMutation struct {
  ID string
  Site string
  Operation map[string]interface{}
}

func NewMapTransformer(grapher *grapher.Grapher) grapher.Transformer {
  t := &mapTransformer{grapher: grapher}
  grapher.AddTransformer(t)
  return t
}

func decodeMapMutation(mutation grapher.MutationNode) (mut mapMutation, err os.Error) {
  switch mutation.Operation().(type) {
  case map[string]interface{}:
    mut.Operation = mutation.Operation().(map[string]interface{})
  case []byte:
    mut.Operation = make(map[string]interface{})
    err = json.Unmarshal(mutation.Operation().([]byte), &mut.Operation)
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

func (self *mapTransformer) Kind() int {
  return grapher.TransformationMerge
}

func (self *mapTransformer) DataType() int {
  return grapher.TypeMap
}

// Interface towards the Grapher
func (self *mapTransformer) TransformClientMutation(mutation grapher.MutationNode, rollback <-chan grapher.MutationNode) (err os.Error) {
  mut, e := decodeMapMutation(mutation)
  if e != nil {
    log.Printf("Err: Decoding")
    return e
  }

  muts := make([]mapMutation, 0)
  for m := range rollback {
    m3, e := decodeMapMutation(m)
    if e != nil {
      log.Printf("Err: Decoding 2")
      return e
    }
    muts = append(muts, m3)
  }
    
  // Transform 'mut' to apply it locally
  pmut, err := transformMapSeq(muts, mut)
  if err != nil {
    log.Printf("TRANSFORM ERR: %v", err)
    return err
  }
  
  bytes, err := json.Marshal(pmut.Operation)
  if err != nil {
    panic("Cannot serlialize")
  }
  mutation.SetOperation(bytes)
  return nil
}

// Interface towards the Grapher
func (self *mapTransformer) TransformMutation(mutation grapher.MutationNode, rollback <-chan grapher.MutationNode, concurrent []string) (err os.Error) {
  mut, e := decodeMapMutation(mutation)
  if e != nil {
    return e
  }

  // Get a list of all concurrent mutations
  conc := make(map[string]bool)
  for _, id := range concurrent {
    conc[id] = true
  }  
  muts := make([]mapMutation, 0)
  for m := range rollback {
    m3, e := decodeMapMutation(m)
    if e != nil {
      return e
    }
    // Skip those which are not concurrent
    if _, ok := conc[m3.ID]; !ok {
      continue;
    }
    muts = append(muts, m3)
  }
    
  // Transform 'mut' to apply it locally
  mut, err = transformMapSeq(muts, mut)
  if err != nil {
    log.Printf("TRANSFORM ERR: %v", err)
    return
  }
  
  bytes, err := json.Marshal(mut.Operation)
  if err != nil {
    panic("Cannot serlialize")
  }
  mutation.SetOperation(bytes)
  return nil  
}

// TODO: Check for correct data type

// Transforms one mutation against a sequence of mutations.
func transformMapSeq(muts []mapMutation, mut mapMutation) (tmut mapMutation, err os.Error) {
  tmut = mut
  for _, m := range muts {
    tmut, err = transformMap(m, tmut)
    if err != nil {
      return
    }
  }
  return
}

// Transforms mut2 against mut1 and returns a modified mut2.
func transformMap(mut1 mapMutation, mut2 mapMutation) (tmut mapMutation, err os.Error) {
  if mut1.ID < mut2.ID || (mut1.ID == mut2.ID && mut1.Site < mut2.Site) {
    tmut = mut2;
    return;
  }
  for key, val := range mut2.Operation {
    tmut.Operation[key] = val
  }
  tmut.Operation = make(map[string]interface{})
  for key, _ := range mut1.Operation {
    if _, ok := mut2.Operation[key]; ok {
      tmut.Operation[key] = nil, false
    }
  }
  return
}