package lightwavetransformer

import (
  grapher "lightwavegrapher"
  "log"
  "os"
  "json"
)

type latestTransformer struct {
  grapher *grapher.Grapher
}

type latestMutation struct {
  Time int64
  Operation interface{}
}

type stringDummy struct {
  Value string
}

func NewLatestTransformer(grapher *grapher.Grapher) grapher.Transformer {
  t := &latestTransformer{grapher: grapher}
  grapher.AddTransformer(t)
  return t
}

// TODO: Perform type checking for other data types. This one does only strings
func decodeGenericMutation(mutation grapher.MutationNode) (mut latestMutation, err os.Error) {
  switch mutation.Operation().(type) {
  case []byte:
    buffer := []byte(`{"op":`)
    buffer = append(buffer, mutation.Operation().([]byte)...)
    buffer = append(buffer, []byte("}")...)
    var val stringDummy
    err = json.Unmarshal(buffer, &val)
    if err != nil {
      return mut, err
    }
    mut.Operation = val.Value
  default:
    mut.Operation = mutation.Operation()
  }
  mut.Time = mutation.Time();
  return
}

func (self *latestTransformer) Kind() int {
  return grapher.TransformationLatest
}

func (self *latestTransformer) DataType() int {
  return grapher.TypeString
}

// Interface towards the Grapher
func (self *latestTransformer) TransformClientMutation(mutation grapher.MutationNode, concurrent <-chan grapher.MutationNode) (err os.Error) {
  mut, e := decodeGenericMutation(mutation)
  if e != nil {
    log.Printf("Err: Decoding")
    return e
  }

  // If any of these is later, then the mutation is transformed into the epsilon operation
  for m := range concurrent {
    if m.Time() > mut.Time {
      mutation.SetOperation([]byte("null"));
      return
    }
  }
  return
}

// Interface towards the Grapher
func (self *latestTransformer) TransformMutation(mutation grapher.MutationNode, rollback <-chan grapher.MutationNode, concurrent []string) (err os.Error) {
  mut, e := decodeGenericMutation(mutation)
  if e != nil {
    return e
  }

  // Get a list of all concurrent mutations
  conc := make(map[string]bool)
  for _, id := range concurrent {
    conc[id] = true
  }  
  for m := range rollback {
    // Skip those which are not concurrent
    if _, ok := conc[m.BlobRef()]; !ok {
      continue;
    }
    if m.Time() > mut.Time {
      mutation.SetOperation([]byte("null"));
      return
    }
  }
  return 
}
