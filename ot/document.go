package lightwaveot

import (
  "os"
  "fmt"
  vec "container/vector"
)

// Every data structure that can be mutated by string operations must implement
// this interface. Whenever a mutation is executed on the object, the
// functions of this interface are called to execute the basic operations
// insert, skip and delete. Begin and End are called before and after a mutation
// is executed on the object.
//
// For many purposes it is sufficient to use the SimpleText struct which
// implements the Text interface.
type Text interface {
  Begin()
  InsertChars(str string)
  InsertTombs(count int)
  Delete(count int) (err os.Error)
  Skip(count int) (err os.Error)
  End()
}

// ------------------------------------------------------------------
// TombStream

// A TombStream is useful when implementing the Text interface.
// It stores where inside the string of characters the tombs are located.
// See SimpleText for an example of how to use the TombStream.
type TombStream struct {
  seq *vec.IntVector
  pos, inside int
}

func NewTombStream(seq *vec.IntVector) *TombStream {
  return &TombStream{seq: seq}
}

func (self *TombStream) InsertChars(n int) {
  if n == 0 {
    // Do nothing by intention
  } else if self.pos == self.seq.Len() { // Insert at EOF (implying that the seq is empty)
    self.seq.Push(n)
    self.inside = n
  } else if self.seq.At(self.pos) >= 0 { // Insert inside a character sequence
    self.seq.Set(self.pos, self.seq.At(self.pos) + n)
    self.inside += n
  } else if self.inside == -self.seq.At(self.pos) { // End of a tomb sequence?
    self.pos++
    self.inside = 0
    self.InsertChars(n)
  } else if self.inside == 0 { // Beginning of a tomb sequence?
    if self.pos > 0 { // Go to the end of the previous character sequence
      self.pos--
      self.inside = self.seq.At(self.pos)
      self.InsertChars(n)
    }
    self.seq.Insert(self.pos, n)
    self.inside = n
  } else { // Insert inside a tomb sequence
    self.seq.Insert(self.pos + 1, n )
    self.seq.Insert(self.pos + 2, self.seq.At(self.pos) + self.inside)
    self.seq.Set(self.pos, -self.inside)
    self.pos++
    self.inside = n
  }
}

func (self *TombStream) InsertTombs(n int) {
  if n == 0 {
    // Do nothing by intention
  } else if self.pos == self.seq.Len() { // Insert at EOF (implying that the seq is empty)
    self.seq.Push(-n)
    self.inside = n
  } else if self.seq.At(self.pos) < 0 { // Insert inside a tomb sequence
    self.seq.Set(self.pos, self.seq.At(self.pos) - n)
    self.inside += n
  } else if self.inside == self.seq.At(self.pos) { // End of a character sequence?
    self.pos++
    self.inside = 0
    self.InsertTombs(n)
  } else if self.inside == 0 { // Beginning of a character sequence?
    if self.pos > 0 { // Go to the end of the previous tomb sequence
      self.pos--
      self.inside = -self.seq.At(self.pos)
      self.InsertTombs(n)
    }
    self.seq.Insert(self.pos, -n)
    self.inside = n
  } else { // Insert inside a character sequence
    self.seq.Insert(self.pos + 1, -n )
    self.seq.Insert(self.pos + 2, self.seq.At(self.pos) - self.inside)
    self.seq.Set(self.pos, self.inside)
    self.pos++
    self.inside = n
  }
}

func (self *TombStream) Bury(n int) (burried int, err os.Error) {
  for n != 0 {
    if self.pos == self.seq.Len() { // End of the sequence -> error
      err = os.NewError("Burry reached EOF")
      return
    }
    x := self.seq.At(self.pos)
    if x < 0 { // Bury characters that have already been burried?
      m := min(n, -x - self.inside)
      self.inside += m
      n -= m
      if self.inside == -x && n > 0 {
	self.pos++
	self.inside = 0
      }
      continue
    }
    m := min(n, x - self.inside)
    n -= m
    burried += m
    self.seq.Set(self.pos, -m)
    left := self.inside
    right := x - m - self.inside
    self.inside = m
    if left > 0 {
      self.seq.Insert(self.pos, left)
      self.pos++
    } else if self.pos > 0 {
      self.seq.Delete(self.pos)
      self.pos--
      self.seq.Set(self.pos, self.seq.At(self.pos) - m)
      self.inside = -self.seq.At(self.pos)
    }
    if right > 0 {
      self.seq.Insert(self.pos + 1, right)
    } else if self.pos + 1 < self.seq.Len() {
      self.seq.Set(self.pos, self.seq.At(self.pos) + self.seq.At(self.pos + 1))
      self.seq.Delete(self.pos + 1)
    }
  }
  return
}

func (self *TombStream) Skip(n int) (chars int, err os.Error) {
  for n > 0 {
    if self.pos >= self.seq.Len() {
      return chars, os.NewError("TombStream reached EOF")
    }
    x := self.seq.At(self.pos)
    if x >= 0 {
      if self.inside == x {
	self.pos++
	self.inside = 0
	continue
      }
      m := min(x - self.inside, n)
      self.inside += m
      n -= m
      chars += m
    } else {
      if self.inside == -x {
	self.pos++
	self.inside = 0
	continue
      }
      m := min(-x - self.inside, n)
      self.inside += m
      n -= m
    }
  }
  return
}

func (self *TombStream) SkipChars(n int) (skipped int, err os.Error) {
  for n > 0 {
    if self.pos >= self.seq.Len() {
      return skipped, os.NewError("TombStream reached EOF")
    }
    x := self.seq.At(self.pos)
    x2 := x
    if x < 0 {
      x2 = -x
    }
    if self.inside == x2 {
      self.pos++
      self.inside = 0
      continue
    }
    m := min(x2 - self.inside, n)
    self.inside += m
    skipped += m
    if x >= 0 {
      n -= m
    }
  }
  return
}

func (self *TombStream) SkipToEnd() (count int) {
  for self.pos < self.seq.Len() {
    x := self.seq.At(self.pos)
    if x < 0 {
      x = -x
    }
    count += x - self.inside
    self.inside = 0
    self.pos++
  }
  return
}

// ------------------------------------------------------------------
// Execution of mutations

func Execute(input interface{}, mut Mutation) (output interface{}, err os.Error) {
  output, err = execute(input, mut.Operation)
  return
}

func execute(input interface{}, op Operation) (output interface{}, err os.Error) {
  switch op.Kind {
  case NoOp:
    return input, nil
  case StringOp:
    text, ok := input.(Text)
    if !ok {
      err = os.NewError("Type mismatch: Not a string")
      return
    }
    err = executeString(text, op.Operations)
    output = text
  case ArrayOp:
  case ObjectOp:
  case OverwriteOp:
    output = op.Value
  default:
    err = os.NewError("Operation not allowed in this place")
  }
  return
}

// Apply a mutation to the input document
func executeString(text Text, ops []Operation) (err os.Error) {
  text.Begin()
  defer text.End()
  for _, op := range ops {
    switch op.Kind {
    case InsertOp:
      str := op.Value.(string)
      if len(str) > 0 {
	text.InsertChars(str)
      } else {
	text.InsertTombs(op.Len)
      }
    case SkipOp:
      e := text.Skip(op.Len)
      if e != nil {
	panic("TombStream ended unexpectedly")
      }
    case DeleteOp:
      err = text.Delete(op.Len)
      if err != nil {
	return
      }
    case NoOp:
      // Do nothing by intention
    default:
      panic(fmt.Sprintf("Operation not allowed in a string: %v", op.Kind))
    }
  }
  return
}

// -----------------------------------------------------------
// TextMarker and TextRange

type TextMarker struct {
  TextPos int
}

type TextRange struct {
  Current, Anchor TextMarker
}

func (self *TextMarker) Insert(pos int, length int) {
  if self.TextPos >= pos {
    self.TextPos += length
  }
}

func (self *TextMarker) Delete(pos int, length int) {
  if self.TextPos >= pos + length {
    self.TextPos -= length
  } else if self.TextPos > pos {
    self.TextPos = pos
  }
}

func (self *TextRange) Insert(pos int, length int) {
  self.Current.Insert(pos, length)
  self.Anchor.Insert(pos, length)
}

func (self *TextRange) Delete(pos int, length int) {
  self.Current.Delete(pos, length)
  self.Anchor.Delete(pos, length)
}

// --------------------------------------------
// SimpleText

// Plain text that can be edited concurrently.
// Implements the Text interface.
type SimpleText struct {
  Text string            // The string without any tombs
  // A positive number represents a sequence of visible characters.
  // A negative number represents a sequence of tombs.
  tombs vec.IntVector
  tombStream *TombStream // Used during a mutation
  pos int                // Used during a mutation
}

func NewSimpleText(text string) *SimpleText {
  s := &SimpleText{Text: text}
  s.tombs.Push(len(text))
  return s
}

func (self *SimpleText) String() string {
  return self.Text
}

func (self *SimpleText) Clone() SimpleText {
  return SimpleText{Text: self.Text, tombs: self.tombs.Copy()}
}

func (self *SimpleText) Begin() {
  self.tombStream = NewTombStream(&self.tombs)
  self.pos = 0
}

func (self *SimpleText) InsertChars(str string) {
  self.tombStream.InsertChars(len(str))
  self.Text = self.Text[:self.pos] + str + self.Text[self.pos:]
  self.pos += len(str)
}

func (self *SimpleText) InsertTombs(count int) {
  self.tombStream.InsertTombs(count)
}

func (self *SimpleText) Delete(count int) (err os.Error) {
  var burried int
  burried, err = self.tombStream.Bury(count)
  if err != nil {
    return
  }
  self.Text = self.Text[:self.pos] + self.Text[self.pos + burried:]
  return
}

func (self *SimpleText) Skip(count int) (err os.Error) {
  var chars int
  chars, err = self.tombStream.Skip(count)
  self.pos += chars
  return
}

func (self *SimpleText) End() {
  self.tombStream = nil
}
