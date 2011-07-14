package lightwaveot

import (
  "os"
  "fmt"
)

func PruneMutationSeq(muts []Mutation, prune map[string]bool) (result []Mutation, err os.Error) {
  started := false
  var u Mutation
  for _, mut := range muts {
    if _, isundo := prune[mut.ID]; !isundo {
      if started {
	mut, u, err = PruneMutation(mut, u)
	if err != nil {
	  return
	}
      }
      result = append(result, mut)
      continue
    }
    if !started {
      started = true
      u = mut
    } else {
      u, err = Compose(u, mut)
      if err != nil {
	return
      }
    }
  }
  return
}

func PruneMutation(mut Mutation, prune Mutation) (tmut Mutation, tprune Mutation, err os.Error) {
  tmut = mut
  tprune = prune
  tmut.Operation, tprune.Operation, err = pruneOp(tmut.Operation, prune.Operation)
  return
}

func pruneOp(op Operation, prune Operation) (top Operation, tprune Operation, err os.Error) {
  top = op
  tprune = prune
  if op.Kind != prune.Kind {
    err = os.NewError("Operations of both streams operate on a different data type or they are not allowed in this place")
    return
  }
  switch op.Kind {
  case StringOp:
    top.Operations, tprune.Operations, err = pruneOps(op.Operations, prune.Operations, pruneStringOp)
  case ArrayOp:
    // TODO
  case ObjectOp:
    top.Operations, tprune.Operations, err = pruneObject(op.Operations, prune.Operations)
  case NoOp:
    // Do nothing by intention
  default:
    err = os.NewError("Operation kind not allowed in this place")
  }
  return
}

func pruneOps(ops []Operation, prune []Operation, f transformFunc) (tops []Operation, tprune []Operation, err os.Error) {
  var reader = composeReader{stream1: &stream{ops:ops}, stream2: &stream{ops:prune}}
  for {
    var op1, op2 Operation
    op1, op2, err = reader.Read()
    // Error or EOF?
    if err != nil || (op1.Kind == NoOp && op2.Kind == NoOp) {
      return
    }
    op1, op2, err = f(op1, op2)
    if err != nil {
      return
    }
    if op1.Kind != NoOp {
      tops = append(tops, op1)
    }
    if op2.Kind != NoOp {
      tprune = append(tprune, op2)
    }
  }
  return
}

func pruneStringOp(op Operation, prune Operation) (top Operation, tprune Operation, err os.Error) {
  if op.Kind != InsertOp && op.Kind != SkipOp && op.Kind != DeleteOp && op.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: op:%v", op.Kind))
    return
  }
  if prune.Kind != InsertOp && prune.Kind != SkipOp && prune.Kind != DeleteOp && prune.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: undo:%v", prune.Kind))
    return
  }
  top = op
  tprune = prune
  if op.Kind == InsertOp {
    tprune = Operation{Kind: SkipOp, Len: op.Len}
  } else if  prune.Kind == InsertOp {
    top = Operation{}
  }
  return
}

func pruneObject(ops1 []Operation, ops2 []Operation) (tops1 []Operation, tops2 []Operation, err os.Error) {
  attr1 := make(map[string]int)
  attr2 := make(map[string]int)
  pos := 0
  for _, a := range ops1 {
    if a.Kind != AttributeOp {
      err = os.NewError("Operation not allowed in an object context")
      return
    }
    attr1[a.Value.(string)] = pos
    pos++
  }
  pos = 0
  for _, a := range ops2 {
    if a.Kind != AttributeOp {
      err = os.NewError("Operation not allowed in an object context")
      return
    }
    attr2[a.Value.(string)] = pos
    pos++
  }
  tops1 = make([]Operation, len(ops1))
  tops2 = make([]Operation, len(ops2))
  copy(tops1, ops1)
  copy(tops2, ops2)
  for key, pos1 := range attr1 {
    pos2, ok := attr2[key]
    if !ok {
      continue
    }
    tops1[pos1].Operations, tops2[pos2].Operations, err = pruneOps(tops1[pos1].Operations, tops2[pos2].Operations, pruneAttrOp)
    if err != nil {
      return
    }
  }
  return
}

func pruneAttrOp(op Operation, prune Operation) (top Operation, tprune Operation, err os.Error) {
  if op.Kind != InsertOp && op.Kind != SkipOp && op.Kind != ObjectOp && op.Kind != ArrayOp && op.Kind != StringOp && op.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in an object context: %v", op.Kind))
    return
  }
  if prune.Kind != InsertOp && prune.Kind != SkipOp && prune.Kind != ObjectOp && prune.Kind != ArrayOp && prune.Kind != StringOp && prune.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in an object context: %v", prune.Kind))
    return
  }
  top = op
  tprune = prune
  if op.Kind == InsertOp {  // prune must be NoOp
    tprune = Operation{Kind: SkipOp, Len: op.Len}
  } else if prune.Kind == InsertOp {
    top = Operation{}
  } else if (prune.Kind == ObjectOp || prune.Kind == StringOp || prune.Kind == ArrayOp) && (op.Kind == ObjectOp || op.Kind == StringOp || op.Kind == ArrayOp) {
    top, tprune, err = pruneOp(op, prune)
  }
  return
}
