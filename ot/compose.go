package lightwaveot

import (
  "os"
  "fmt"
)

type composeReader struct {
  stream1, stream2 *stream
}

// Read a tuple of operations from Stream1 and Stream2
func (self *composeReader) Read() (second Operation, first Operation, err os.Error) {
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
    first = self.stream2.Read(-1)
    return
  }
  // EOF Stream2?
  if self.stream2.IsEOF() {
    if self.stream1.ops[self.stream1.pos].Kind != InsertOp {
      err = os.NewError("Streams have different length (2)")
      return
    }
    second = self.stream1.Read(-1)
    return
  }
  // Insert of stream1 goes first
  if self.stream1.ops[self.stream1.pos].Kind == InsertOp {
    second = self.stream1.Read(-1)
    return
  }
  // Skip, Insert (of stream2) and Delete go together
  l := min(self.stream1.ops[self.stream1.pos].Len - self.stream1.inside, self.stream2.ops[self.stream2.pos].Len - self.stream2.inside)
  second = self.stream1.Read( l ) 
  first = self.stream2.Read( l )
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
  result.Operation, err = composeOp(first.Operation, second.Operation)
  return
}

func composeOp(first Operation, second Operation) (result Operation, err os.Error) {
  if first.Kind != second.Kind {
    err = os.NewError("Operations of both streams operate on a different data type or they are not allowed in this place")
    return
  }
  result.Kind = first.Kind
  switch first.Kind {
  case StringOp:
    result.Operations, err = composeOps(first.Operations, second.Operations, composeStringOp)
  case ArrayOp:
    // TODO
  case ObjectOp:
    result.Operations, err = composeObject(first.Operations, second.Operations)
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

func composeObject(first []Operation, second []Operation) (result []Operation, err os.Error) {
  attr_first := make(map[string]int)
  attr_second := make(map[string]int)
  pos := 0
  for _, a := range first {
    if a.Kind != AttributeOp {
      err = os.NewError("Operation not allowed in an object context")
      return
    }
    attr_first[a.Value.(string)] = pos
    pos++
  }
  pos = 0
  for _, a := range second {
    if a.Kind != AttributeOp {
      err = os.NewError("Operation not allowed in an object context")
      return
    }
    attr_second[a.Value.(string)] = pos
    pos++
  }
  for key, pos1 := range attr_first {
    pos2, ok := attr_second[key]
    if !ok {
      result = append(result, first[pos1])
    } else {
      ops, err := composeOps(first[pos1].Operations, second[pos2].Operations, composeAttrOp)
      if err != nil {
	return
      }
      result = append(result, Operation{Kind:AttributeOp, Value:key, Operations: ops})
    }
  }
  for key, pos2 := range attr_second {
    _, ok := attr_first[key]
    if !ok {
      result = append(result, second[pos2])
    }
  }
  return
}

func composeAttrOp(first Operation, second Operation) (result Operation, err os.Error) {
  if first.Kind != InsertOp && first.Kind != SkipOp && first.Kind != StringOp && first.Kind != ObjectOp && first.Kind != ArrayOp && first.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: first:%v", first.Kind))
    return
  }
  if second.Kind != InsertOp && second.Kind != SkipOp && second.Kind != StringOp && second.Kind != ObjectOp && second.Kind != ArrayOp && second.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: second:%v", second.Kind))
    return
  }
  if second.Kind == InsertOp { // The first op is for sure NoOp
    result = second
  } else if first.Kind == InsertOp {
    if second.Kind == StringOp || second.Kind == ObjectOp || second.Kind == ArrayOp {
      if len(first.Operations) != 1 {
	err = os.NewError("Insert operation must have one child operation when composed with StringOp, ObjectOp or ArrayOp")
      }
      result = first
      result.Operations[0], err = composeOp(first.Operations[0], second)
    } else { // The second opeation is for sure SkipOp
      result = first
    }
  } else if second.Kind == SkipOp {
    result = first
  } else if first.Kind == SkipOp {
    result = second
  } else {
    result, err = composeOp(first, second)
  }
  return
}
