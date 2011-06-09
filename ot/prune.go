package lightwaveot

import (
  "os"
  "fmt"
)

func PruneSeq(muts []Mutation, prune map[string]bool) (result []Mutation, err os.Error) {
  started := false
  var u Mutation
  for _, mut := range muts {
    if _, isundo := prune[mut.ID]; !isundo {
      if started {
	mut, u, err = Prune(mut, u)
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

func Prune(mut Mutation, prune Mutation) (tmut Mutation, tprune Mutation, err os.Error) {
  tmut = mut
  tprune = prune
  if mut.Operation.Kind == OverwriteOp {
    // TODO
  }
  if prune.Operation.Kind == OverwriteOp {
    // TODO
  }
  if mut.Operation.Kind != prune.Operation.Kind {
    err = os.NewError("Operations of both streams operate on a different data type or they are not allowed in this place")
    return
  }
  switch mut.Operation.Kind {
  case StringOp:
    tmut.Operation.Operations, tprune.Operation.Operations, err = pruneOps(mut.Operation.Operations, prune.Operation.Operations, pruneStringOp)
  case ArrayOp:
    // TODO
  case ObjectOp:
    // TODO
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
