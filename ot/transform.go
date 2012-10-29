package ot

import (
  "errors"
  "fmt"
  "log"
)

// -------------------------------------------------------------------------
// A stream of operations

// Treats a sequence of operations as a stream.
type stream struct {
  ops []Operation
  // An index inside the ops slice
  pos int
  // An index that points inside an operation.
  // For example it tells how many characters of an InsertOp have
  // already been read.
  inside int
}

// Tests for the end of the stream
func (self *stream) IsEOF() bool {
  return self.pos == len(self.ops)
}

// Extract an operation of the required length.
// Length is either -1 or limited by the remaining length of the current operation (i.e. self.ops[self.pos].Len - self.inside).
func (self *stream) Read(length int) (op Operation) {
  op = self.ops[self.pos]
  if length == -1 {
    length = op.Len - self.inside
  }
  if op.Kind == InsertOp {
    if self.inside == 0 && length == op.Len {
      // Do nothing by intention
    } else {
      str := op.Value.(string)
      if len(str) > 0 {
        op.Value = str[self.inside : self.inside+length]
      } else {
        op.Value = ""
      }
    }
  }
  self.inside += length
  if self.inside == op.Len {
    self.inside = 0
    self.pos++
  }
  op.Len = length
  return
}

// -------------------------------------------------------------------------
// Read pairs of operations from two streams

// The reader is used during transformation.
// It reads a pair of operations, from two operation streams.
// If one operation is InsertOp or if the other stream is already finished, then
// the other operation can be NoOp.
// Transformation is implemented by reading a pair operations, transforming the ops, and reading again ...
type reader struct {
  stream1, stream2 *stream
}

// Read a tuple of operations from stream1 and stream2
func (self *reader) Read() (op1 Operation, op2 Operation, err error) {
  // EOF?
  if self.stream1.IsEOF() && self.stream2.IsEOF() {
    return
  }
  // EOF Stream1?
  if self.stream1.IsEOF() {
    if self.stream2.ops[self.stream2.pos].Kind != InsertOp {
      err = errors.New("Streams have different length")
      log.Printf("STREAM1: %v\nSTREAM2: %v\n", self.stream1.ops, self.stream2.ops)
      return
    }
    op2 = self.stream2.Read(-1)
    return
  }
  // EOF Stream2?
  if self.stream2.IsEOF() {
    if self.stream1.ops[self.stream1.pos].Kind != InsertOp {
      err = errors.New("Streams have different length")
      log.Printf("STREAM1: %v\nSTREAM2: %v\n", self.stream1.ops, self.stream2.ops)
      return
    }
    op1 = self.stream1.Read(-1)
    return
  }
  // Insert of stream1 goes first
  if self.stream1.ops[self.stream1.pos].Kind == InsertOp {
    op1 = self.stream1.Read(-1)
    return
  }
  // Insert of stream2 goes next
  if self.stream2.ops[self.stream2.pos].Kind == InsertOp {
    op2 = self.stream2.Read(-1)
    return
  }
  // Skip, and Delete go together
  l := min(self.stream1.ops[self.stream1.pos].Len-self.stream1.inside, self.stream2.ops[self.stream2.pos].Len-self.stream2.inside)
  op1 = self.stream1.Read(l)
  op2 = self.stream2.Read(l)
  return
}

// -------------------------------------------------------------------------
// Transformation of mutations

// Transforms one mutation against a sequence of mutations.
func TransformSeq(muts []Mutation, mut Mutation) (tmuts []Mutation, tmut Mutation, err error) {
  tmut = mut
  for _, m := range muts {
    m, tmut, err = Transform(m, tmut)
    if err != nil {
      return
    }
    tmuts = append(tmuts, m)
  }
  return
}

// Transform two mutations
func Transform(m1 Mutation, m2 Mutation) (tm1 Mutation, tm2 Mutation, err error) {
  tm1 = m1
  tm2 = m2
  if m1.Site == m2.Site && m1.ID == m2.ID {
    // If the IDs are equal, return empty mutations
  } else if m1.Site < m2.Site || (m1.Site == m2.Site && m1.ID < m2.ID) {
    tm1.Operation, tm2.Operation, err = transformOp(m1.Operation, m2.Operation)
  } else {
    tm2.Operation, tm1.Operation, err = transformOp(m2.Operation, m1.Operation)
  }
  return
}

func transformOp(op1 Operation, op2 Operation) (top1 Operation, top2 Operation, err error) {
  top1 = op1
  top2 = op2
  if op1.Kind == NoOp || op2.Kind == NoOp {
    return
  }
  if op1.Kind != op2.Kind {
    err = errors.New("Operations of both streams operate on a different data type or they are not allowed in this place")
    return
  }
  switch op1.Kind {
  case StringOp:
    top1.Operations, top2.Operations, err = transformOps(op1.Operations, op2.Operations, transformStringOp)
  case ArrayOp:
    // TODO
  case ObjectOp:
    top1.Operations, top2.Operations, err = transformObject(op1.Operations, op2.Operations)
  default:
    err = errors.New("Operation kind not allowed in this place")
  }
  return
}

type transformFunc func(op1 Operation, op2 Operation) (top1 Operation, top2 Operation, err error)

// Transform a sequence of operations
func transformOps(ops1 []Operation, ops2 []Operation, f transformFunc) (tops1 []Operation, tops2 []Operation, err error) {
  var reader = reader{stream1: &stream{ops: ops1}, stream2: &stream{ops: ops2}}
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
      tops1 = append(tops1, op1)
    }
    if op2.Kind != NoOp {
      tops2 = append(tops2, op2)
    }
  }
  return
}

// Transform a pair of operations that works on a string
func transformStringOp(op1 Operation, op2 Operation) (top1 Operation, top2 Operation, err error) {
  if op1.Kind != InsertOp && op1.Kind != SkipOp && op1.Kind != DeleteOp && op1.Kind != NoOp {
    err = errors.New(fmt.Sprintf("Operation not allowed in a string: %v", op1.Kind))
    return
  }
  if op2.Kind != InsertOp && op2.Kind != SkipOp && op2.Kind != DeleteOp && op2.Kind != NoOp {
    err = errors.New(fmt.Sprintf("Operation not allowed in a string: %v", op2.Kind))
    return
  }
  top1 = op1
  top2 = op2
  if op1.Kind == InsertOp {
    top2 = Operation{Kind: SkipOp, Len: op1.Len}
  } else if op2.Kind == InsertOp {
    top1 = Operation{Kind: SkipOp, Len: op2.Len}
  }
  return
}

func transformObject(ops1 []Operation, ops2 []Operation) (tops1 []Operation, tops2 []Operation, err error) {
  attr1 := make(map[string]int)
  attr2 := make(map[string]int)
  pos := 0
  for _, a := range ops1 {
    if a.Kind != AttributeOp {
      err = errors.New("Operation not allowed in an object context")
      return
    }
    attr1[a.Value.(string)] = pos
    pos++
  }
  pos = 0
  for _, a := range ops2 {
    if a.Kind != AttributeOp {
      err = errors.New("Operation not allowed in an object context")
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
    //    println(fmt.Sprintf("key=%v, pos1 = %v, pos2 = %v", key, pos1, pos2))
    tops1[pos1].Operations, tops2[pos2].Operations, err = transformOps(tops1[pos1].Operations, tops2[pos2].Operations, transformAttrOp)
    if err != nil {
      return
    }
  }
  return
}

func transformAttrOp(op1 Operation, op2 Operation) (top1 Operation, top2 Operation, err error) {
  if op1.Kind != InsertOp && op1.Kind != SkipOp && op1.Kind != ObjectOp && op1.Kind != ArrayOp && op1.Kind != StringOp && op1.Kind != NoOp {
    err = errors.New(fmt.Sprintf("Operation not allowed in an object context: %v", op1.Kind))
    return
  }
  if op2.Kind != InsertOp && op2.Kind != SkipOp && op2.Kind != ObjectOp && op2.Kind != ArrayOp && op2.Kind != StringOp && op2.Kind != NoOp {
    err = errors.New(fmt.Sprintf("Operation not allowed in an object context: %v", op2.Kind))
    return
  }
  top1 = op1
  top2 = op2
  if op1.Kind == InsertOp {
    top2 = Operation{Kind: SkipOp, Len: 1}
  } else if op2.Kind == InsertOp {
    top1 = Operation{Kind: SkipOp, Len: 1}
  } else if (op1.Kind == StringOp || op1.Kind == ArrayOp || op1.Kind == ObjectOp) && (op2.Kind == StringOp || op2.Kind == ArrayOp || op2.Kind == ObjectOp) {
    top1, top2, err = transformOp(op1, op2)
  }
  return
}

// Helper function
func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}
