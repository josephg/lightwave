package lightwaveot

import (
  "os"
  "log"
  "fmt"
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
    str := op.Value.(string)
    if len(str) > 0 {
      op.Value = str[self.inside:self.inside + length]
    } else {
      op.Value = ""
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
func (self *reader) Read() (op1 Operation, op2 Operation, err os.Error) {
  // EOF?
  if self.stream1.IsEOF() && self.stream2.IsEOF() {
    return
  }
  // EOF Stream1?
  if self.stream1.IsEOF() {
    if self.stream2.ops[self.stream2.pos].Kind != InsertOp {
      err = os.NewError("Streams have different length")
      log.Printf("STREAM1: %v\nSTREAM2: %v\n", self.stream1.ops, self.stream2.ops)
      return
    }
    op2 = self.stream2.Read(-1)
    return
  }
  // EOF Stream2?
  if self.stream2.IsEOF() {
    if self.stream1.ops[self.stream1.pos].Kind != InsertOp {
      err = os.NewError("Streams have different length")
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
  l := min(self.stream1.ops[self.stream1.pos].Len - self.stream1.inside, self.stream2.ops[self.stream2.pos].Len - self.stream2.inside)
  op1 = self.stream1.Read( l ) 
  op2 = self.stream2.Read( l )
  return
}

// -------------------------------------------------------------------------
// Transformation of mutations

// Transforms one mutation against a sequence of mutations.
func TransformSeq(muts []Mutation, mut Mutation) (tmuts []Mutation, tmut Mutation, err os.Error) {
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
func Transform(m1 Mutation, m2 Mutation) (tm1 Mutation, tm2 Mutation, err os.Error) {
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

func transformOp(op1 Operation, op2 Operation) (top1 Operation, top2 Operation, err os.Error) {
  top1 = op1
  top2 = op2
  if op1.Kind == NoOp || op2.Kind == NoOp {
    return
  }
  if op1.Kind == OverwriteOp {
    top2 = Operation{}
    return
  }
  if op2.Kind == OverwriteOp {
    top1 = Operation{}
    return
  }
  if op1.Kind != op2.Kind {
    err = os.NewError("Operations of both streams operate on a different data type or they are not allowed in this place")
    return
  }
  switch op1.Kind {
  case StringOp:
    top1.Operations, top2.Operations, err = transformOps(op1.Operations, op2.Operations, transformStringOp)
  case ArrayOp:
    // TODO
  case ObjectOp:
    // TODO
  default:
    err = os.NewError("Operation kind not allowed in this place")
  }
  return
}

type transformFunc func(op1 Operation, op2 Operation) (top1 Operation, top2 Operation, err os.Error)

// Transform a sequence of operations
func transformOps(ops1 []Operation, ops2 []Operation, f transformFunc) (tops1 []Operation, tops2 []Operation, err os.Error) {
  var reader = reader{stream1: &stream{ops:ops1}, stream2: &stream{ops:ops2}}
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
func transformStringOp(op1 Operation, op2 Operation) (top1 Operation, top2 Operation, err os.Error) {
  if op1.Kind != InsertOp && op1.Kind != SkipOp && op1.Kind != DeleteOp && op1.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: %v", op1.Kind))
    return
  }
  if op2.Kind != InsertOp && op2.Kind != SkipOp && op2.Kind != DeleteOp && op2.Kind != NoOp {
    err = os.NewError(fmt.Sprintf("Operation not allowed in a string: %v", op2.Kind))
    return
  }
  top1 = op1
  top2 = op2
  if op1.Kind == InsertOp {
    top2 = Operation{Kind: SkipOp, Len: op1.Len}
  } else if  op2.Kind == InsertOp {
    top1 = Operation{Kind: SkipOp, Len: op2.Len}
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

// ----------------------------------------------------------------------
// Frontier

// A Frontier is a set of mutation IDs.
// Storing the IDs of all mutations ever applied to a document is space consuming.
// Therefore, the frontier remembers only the 'latest' mutation IDs and throws away the 'old' ones.
// The trick is that the old ones can be recomputed by recursively following the Mutation.Dependencies field of
// each mutation in the frontier.
type Frontier map[string]bool

func (self Frontier) Add(mut Mutation) {
  self[mut.ID] = true
  for _, dep := range mut.Dependencies {
    self[dep] = false, false
  }
}

func (self Frontier) IDs() (list []string) {
  for id, _ := range self {
    list = append(list, id)
  }
  return
}

// ----------------------------------------------------------------------
// HistoryGraph

type HistoryGraph struct {
  frontier map[string]bool
  oldFrontier map[string]bool
  markedCount int
}

func NewHistoryGraph(frontier Frontier, dest []string) *HistoryGraph {
  d := make(map[string]bool)
  for _, id := range dest {
    d[id] = true
  }
  f := make(map[string]bool)
  markedCount := 0
  for id, _ := range frontier {
    _, mark := d[id]
    f[id] = mark
    if mark {
      markedCount++
    }
  }
  h := &HistoryGraph{frontier: f, oldFrontier: d, markedCount: markedCount}
  return h
}

func (self *HistoryGraph) Substitute(mut Mutation) bool {
  if _, ok := self.frontier[mut.ID]; !ok {
    panic("Substituting a mutation that is not part of the history graph")
  }
  ismarked := self.frontier[mut.ID]
  if ismarked {
    self.markedCount--
  }
  for _, dep := range mut.Dependencies {
    _, mark := self.oldFrontier[dep]
    existsMark, exists := self.frontier[dep] 
    mark = mark || ismarked
    if !exists || (mark && !existsMark) {
      self.frontier[dep] = mark
      if mark {
	self.markedCount++
      }
    }
  }
  self.frontier[mut.ID] = false, false
  return ismarked
}

func (self *HistoryGraph) Test() bool {
  return self.markedCount == len(self.frontier)
}
