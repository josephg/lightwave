package lightwaveot

import (
  "os"
  "fmt"
)

type composeReader struct {
  stream1, stream2 *stream
}

// Read a tuple of operations from Stream1 and Stream2
func (self *composeReader) Read() (op Operation, undo Operation, err os.Error) {
  // EOF?
  if self.stream1.IsEOF() && self.stream2.IsEOF() {
    return
  }
  // EOF Stream1?
  if self.stream1.IsEOF() {
    if self.stream2.ops[self.stream2.pos].Kind != InsertOp {
      err = os.NewError("Streams have different length (1)")
      return
    }
    undo = self.stream2.Read(-1)
    return
  }
  // EOF Stream2?
  if self.stream2.IsEOF() {
    if self.stream1.ops[self.stream1.pos].Kind != InsertOp {
      err = os.NewError("Streams have different length (2)")
      return
    }
    op = self.stream1.Read(-1)
    return
  }
  // Insert of stream1 goes first
  if self.stream1.ops[self.stream1.pos].Kind == InsertOp {
    op = self.stream1.Read(-1)
    return
  }
  // Skip, Insert (of stream2) and Delete go together
  l := min(self.stream1.ops[self.stream1.pos].Len - self.stream1.inside, self.stream2.ops[self.stream2.pos].Len - self.stream2.inside)
  op = self.stream1.Read( l ) 
  undo = self.stream2.Read( l )
  return
}

func ComposeSeq(mutations []Mutation) (result Mutation, err os.Error) {
  if len(mutations) == 0 {
    return
  }
  result = mutations[0]
  for i := 1; i < len(mutations); i++ {
    result, err = Compose(result, mutations[i])
    if err != nil {
      return
    }
  }
  return
}

func Compose(first Mutation, second Mutation) (result Mutation, err os.Error) {
  if first.Operation.Kind == OverwriteOp {
    // TODO
  }
  if second.Operation.Kind == OverwriteOp {
    // TODO
  }
  if first.Operation.Kind != second.Operation.Kind {
    err = os.NewError("Operations of both streams operate on a different data type or they are not allowed in this place")
    return
  }
  result.Operation.Kind = first.Operation.Kind
  switch first.Operation.Kind {
  case StringOp:
    result.Operation.Operations, err = composeOps(first.Operation.Operations, second.Operation.Operations, composeStringOp)
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

type composeFunc func(fi Operation, undo Operation) (result Operation, err os.Error)

func composeOps(first []Operation, second []Operation, f composeFunc) (result []Operation, err os.Error) {
  var reader = composeReader{stream1: &stream{ops:second}, stream2: &stream{ops:first}}
  for {
    var first_op, second_op Operation
    second_op, first_op, err = reader.Read()
    // Error or EOF?
    if err != nil || (first_op.Kind == NoOp && second_op.Kind == NoOp) {
      return
    }
    var op Operation
    op, err = f(first_op, second_op)
    if err != nil {
      return
    }
    if op.Kind != NoOp {
      result = append(result, op)
    }
  }
  return
}

func composeStringOp(first Operation, second Operation) (result Operation, err os.Error) {
  if first.Kind != InsertOp && first.Kind != SkipOp && first.Kind != DeleteOp && first.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: first:%v", first.Kind))
    return
  }
  if second.Kind != InsertOp && second.Kind != SkipOp && second.Kind != DeleteOp && second.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: second:%v", second.Kind))
    return
  }
  if first.Kind == InsertOp {
    if second.Kind == DeleteOp {
      result = Operation{Kind: InsertOp, Len: first.Len} // Insert a tomb in the composed op
    } else {
      result = first
    }
  } else if first.Kind == DeleteOp {
    result = Operation{Kind: DeleteOp, Len: first.Len}
  } else {
    result = second
  }
  return
}
