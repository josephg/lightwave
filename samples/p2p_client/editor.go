package main

import (
  . "lightwave/ot"
  "github.com/nsf/termbox-go"
  "os"
  "fmt"
  "strings"
)

type Editor struct {
  indexer *Indexer
  frontier Frontier
  text string
  tombs IntVector
  // Required during mutations
  mutPos, mutLinePos, mutLine int
  mutTombs *TombStream
  Rows, Columns int
  ScrollX, ScrollY int
  ranges []*TextRange  // The first range is the cursor. Other ranges are cursors of other users
}

func NewEditor(indexer *Indexer) *Editor {
  rows, cols := termbox.Size()
  e := &Editor{indexer: indexer, Rows: rows, Columns: cols, frontier:make(Frontier)}
  indexer.AddListener(e)
  return e
}

func (self *Editor) Begin() {
  self.mutPos = 0
  self.mutLine = 0
  self.mutLinePos = 0
  self.mutTombs = NewTombStream(&self.tombs)
}

// Text interface
func (self *Editor) InsertChars(str string) {
  for _, r := range self.ranges {
    r.Insert(self.mutPos, len(str))
  }
  self.mutTombs.InsertChars(len(str))
  self.text = self.text[:self.mutPos] + str + self.text[self.mutPos:]
  newlines := strings.Count(str, "\n")
  if newlines > 0 {
    termbox.SetCursor(self.mutLinePos, self.mutLine)
    //Stdwin.Clrtobot()
    self.mutLine += newlines
    self.mutLinePos = len(str) - strings.LastIndex(str, "\n") - 1
  } else {
    self.mutLinePos += len(str)
  }
  self.mutPos += len(str)
}

// Text interface
func (self *Editor) InsertTombs(count int) {
  self.mutTombs.InsertTombs(count)
}

// Text interface
func (self *Editor) Delete(count int) (err error) {
  for _, r := range self.ranges {
    r.Delete(self.mutPos, count)
  }
  var burried int
  burried, err = self.mutTombs.Bury(count)
  if err != nil {
    return
  }
  termbox.SetCursor(self.mutLinePos, self.mutLine)
  if strings.Count(self.text[self.mutPos:self.mutPos + burried], "\n") > 0 {
    //Stdwin.Clrtobot()
  } else {
    //Stdwin.Clrtoeol()
  }  
  self.text = self.text[:self.mutPos] + self.text[self.mutPos + burried:]
  return
}

// Text interface
func (self *Editor) Skip(count int) (err error) {
  var chars int
  chars, err = self.mutTombs.Skip(count)
  str := self.text[self.mutPos:self.mutPos + chars]
  newlines := strings.Count(str, "\n")
  if newlines > 0 {
    self.mutLine += newlines
    self.mutLinePos = chars - strings.LastIndex(str, "\n") - 1
  } else {
    self.mutLinePos += chars
  }  
  self.mutPos += chars
  return
}

// Text interface
func (self *Editor) End() {
  self.mutTombs = nil
  self.Refresh()
}

func (self *Editor) LineCount() (result int) {
  result = 1
  for pos := 0; pos < len(self.text); pos++ {
    if self.text[pos] == '\n' {
      result++
    }
  }
  return
}

func (self *Editor) GetLineString(line int) string {
  l := 0
  start := 0
  for pos := 0; pos <= len(self.text); pos++ {
    if pos == len(self.text) || self.text[pos] == '\n' {
      if l == line {
	return self.text[start:pos]
      }
      l++
      start = pos + 1
    }
  }
  return ""
}

func (self *Editor) Cursor() int {
  return self.ranges[0].Current.TextPos
}

func (self *Editor) SetCursor(pos int) {
  self.ranges[0].Current.TextPos = pos
  linepos, line := self.CursorToScreenPos(pos)
  //Stdwin.Move(linepos - self.ScrollX, line - self.ScrollY)
  termbox.SetCursor(linepos - self.ScrollX, line - self.ScrollY)
  termbox.Flush()
}

func (self *Editor) CursorToScreenPos(pos int) (linepos int, line int) {
  for p := 0; p < pos; p++ {
    if p == len(self.text) || self.text[p] == '\n' {
      line++
      linepos = 0
    } else {
      linepos++
    }
  }
  return
}

func (self *Editor) ScreenPosToCursor(linepos, line int) int {
  l := 0
  lpos := 0
  for pos := 0; pos <= len(self.text); pos++ {
    if l == line && lpos == linepos {
      return pos
    }
    if pos == len(self.text) || self.text[pos] == '\n' {
      l++
      lpos = 0
    } else {
      lpos++
    }
  }
  return len(self.text)
}

func (self *Editor) Refresh() {
  line := 0
  linepos := 0
  start := 0
  for pos := 0; pos <= len(self.text); pos++ {
    if pos == len(self.text) || self.text[pos] == '\n' {
      // Is this line visible?
      if line - self.ScrollY >= 0 && line - self.ScrollY < self.Rows {
        str := self.text[start:pos]
        if len(self.text) > self.ScrollX {
          str = str[self.ScrollX:]
        }
        if len(str) > self.Columns {
          str = str[0:self.Columns]
        }
        for i, r := range(str) {
          termbox.SetCell(i, line - self.ScrollY, r, termbox.ColorDefault, termbox.ColorDefault)
        }
        //Stdwin.Addstr(0, line - self.ScrollY, str, 0)
      }
      line++
      linepos = 0
      start = pos + 1
    } else {
      linepos++
    }
  }
  // Show the cursor
  linepos, line = self.CursorToScreenPos(self.Cursor())
  //Stdwin.Move(linepos - self.ScrollX, line - self.ScrollY)
  termbox.SetCursor(linepos - self.ScrollX, line - self.ScrollY)
  termbox.Flush()
}

func (self *Editor) Loop() {
  for {
    e := termbox.PollEvent()
    if e.Type != termbox.EventKey {
      continue
    }

    linePos, line := self.CursorToScreenPos(self.Cursor())
    switch {
    case e.Ch == 'q':
      return
    case e.Key == termbox.KeyArrowLeft:
      if line == 0 && linePos == 0 {
        continue
      }
      if linePos == 0 {
        line--
        str := self.GetLineString(line)
        linePos = len(str)
        self.SetCursor(self.ScreenPosToCursor(linePos, line))
      } else {
        self.SetCursor(self.Cursor() - 1)
      }
    case e.Key == termbox.KeyArrowRight:
      str := self.GetLineString(line)
      if linePos >= len(str) {
        if line == self.LineCount() - 1 {
          continue
        }
        self.SetCursor(self.ScreenPosToCursor(0, line + 1))
      } else {
        self.SetCursor(self.Cursor() + 1)
      }
    case e.Key == termbox.KeyArrowUp:
      if line == 0 {
        continue
      }
      line--
      str := self.GetLineString(line)
      if linePos > len(str) {
        linePos = len(str)
      }
      self.SetCursor(self.ScreenPosToCursor(linePos, line))
    case e.Key == termbox.KeyArrowDown:
      if line + 1 == self.LineCount() {
        continue
      }
      line++
      str := self.GetLineString(line)
      if linePos > len(str) {
        linePos = len(str)
      }
      self.SetCursor(self.ScreenPosToCursor(linePos, line))
    case e.Key == termbox.KeyBackspace || e.Key == termbox.KeyBackspace2:
      if line == 0 && linePos == 0 {
        continue
      }
      var mut Mutation
      var ops []Operation
      stream := NewTombStream(&self.tombs)
      skipped, _ := stream.SkipChars(self.Cursor() - 1)
      if skipped > 0 {
        ops = append(ops, Operation{Kind: SkipOp, Len: skipped})
      }
      deleted, _ := stream.SkipChars(1)
      ops = append(ops, Operation{Kind: DeleteOp, Len: deleted})
      skipped = stream.SkipToEnd()
      if skipped > 0 {
        ops = append(ops, Operation{Kind: SkipOp, Len: skipped})
      }
      mut.Operation = Operation{Kind: StringOp, Operations: ops}
      self.indexer.HandleClientMutation(mut)
    case e.Ch != 0 || e.Key == termbox.KeyEnter:
      if e.Key == termbox.KeyEnter {
        e.Ch = '\n'
      }
      var mut Mutation
      var ops []Operation
      stream := NewTombStream(&self.tombs)
      skipped, _ := stream.SkipChars(self.Cursor())
      if skipped > 0 {
        ops = append(ops, Operation{Kind: SkipOp, Len: skipped})
      }
      ops = append(ops, Operation{Kind: InsertOp, Len: 1, Value: string(e.Ch)})
      skipped = stream.SkipToEnd()
      if skipped > 0 {
        ops = append(ops, Operation{Kind: SkipOp, Len: stream.SkipToEnd()})
      }
      mut.Operation = Operation{Kind: StringOp, Operations: ops}
      self.indexer.HandleClientMutation(mut)
    }
  }
}

// interface IndexListener
func (self *Editor) HandleMutation(mut Mutation) {
//  log.Printf("Apply %v", mut)
  _, err := Execute(self, mut)
  if err != nil {
    panic(err.Error())
  }
  self.frontier.Add(mut)
  self.Refresh()
}

func startGoCurses() (err error) {
  termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)
  return
}

func stopGoCurses() {
  termbox.Close()
}

func uuid() string {
  f, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0) 
  b := make([]byte, 16) 
  f.Read(b) 
  f.Close() 
  return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]) 
}
