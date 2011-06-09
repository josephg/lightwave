package main

import (
  . "curses"
  . "lightwaveot"
  "os"
  "fmt"
  "strings"
  vec "container/vector"
)

type Editor struct {
  store *Store
  frontier Frontier
  text string
  tombs vec.IntVector
  // Required during mutations
  mutPos, mutLinePos, mutLine int
  mutTombs *TombStream
  Rows, Columns int
  ScrollX, ScrollY int
  ranges []*TextRange  // The first range is the cursor. Other ranges are cursors of other users
  site string // The site identifier used in Mutation
}

func NewEditor(store *Store, indexer *Indexer) *Editor {
  e := &Editor{store:store, Rows: *Rows, Columns: *Cols, frontier:make(Frontier), site: uuid()}
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
    Stdwin.Move(self.mutLinePos, self.mutLine)
    Stdwin.Clrtobot()
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
func (self *Editor) Delete(count int) (err os.Error) {
  for _, r := range self.ranges {
    r.Delete(self.mutPos, count)
  }
  var burried int
  burried, err = self.mutTombs.Bury(count)
  if err != nil {
    return
  }
  Stdwin.Move(self.mutLinePos, self.mutLine)
  if strings.Count(self.text[self.mutPos:self.mutPos + burried], "\n") > 0 {
    Stdwin.Clrtobot()
  } else {
    Stdwin.Clrtoeol()
  }  
  self.text = self.text[:self.mutPos] + self.text[self.mutPos + burried:]
  return
}

// Text interface
func (self *Editor) Skip(count int) (err os.Error) {
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
  Stdwin.Move(linepos - self.ScrollX, line - self.ScrollY)
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
        Stdwin.Addstr(0, line - self.ScrollY, str, 0)
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
  Stdwin.Move(linepos - self.ScrollX, line - self.ScrollY)
}

func (self *Editor) Loop() {
  for {
    inp := Stdwin.Getch()
    //panic(fmt.Sprintf("KEY %v", inp))
    linePos, line := self.CursorToScreenPos(self.Cursor())
    switch inp {
    case 'q':
      return
    case 's':
      federation.Suspend()
    case 'r':
      federation.Resume()
    case KEY_LEFT:
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
    case KEY_RIGHT:
      str := self.GetLineString(line)
      if linePos >= len(str) {
	if line == self.LineCount() - 1 {
	  continue
	}
	self.SetCursor(self.ScreenPosToCursor(0, line + 1))
      } else {
	self.SetCursor(self.Cursor() + 1)
      }
    case KEY_UP:
      if line == 0 {
	continue
      }
      line--
      str := self.GetLineString(line)
      if linePos > len(str) {
	linePos = len(str)
      }
      self.SetCursor(self.ScreenPosToCursor(linePos, line))
    case KEY_DOWN:
      if line + 1 == self.LineCount() {
	continue
      }
      line++
      str := self.GetLineString(line)
      if linePos > len(str) {
	linePos = len(str)
      }
      self.SetCursor(self.ScreenPosToCursor(linePos, line))
    case KEY_BACKSPACE, 127:
      if line == 0 && linePos == 0 {
	continue
      }
      var mut Mutation
      var ops []Operation
      stream := NewTombStream(&self.tombs)
      skipped, _ := stream.SkipChars(self.Cursor() - 1)
      ops = append(ops, Operation{Kind: SkipOp, Len: skipped})
      skipped, _ = stream.SkipChars(1)
      ops = append(ops, Operation{Kind: DeleteOp, Len: skipped})
      ops = append(ops, Operation{Kind: SkipOp, Len: stream.SkipToEnd()})
      mut.Operation = Operation{Kind: StringOp, Operations: ops}
      mut.Dependencies = self.frontier.IDs()
      mut.Site = self.site
      blob, blobref, err := EncodeMutation(mut)
      if err !=  nil {
	panic(err.String())
      }
      self.store.StoreBlob(blob, blobref)
    default:
      if inp == KEY_ENTER || inp == 13 {
	inp = 10
      }
      var mut Mutation
      var ops []Operation
      stream := NewTombStream(&self.tombs)
      skipped, _ := stream.SkipChars(self.Cursor())
      ops = append(ops, Operation{Kind: SkipOp, Len: skipped})
      ops = append(ops, Operation{Kind: InsertOp, Len: 1, Value: string(inp)})
      ops = append(ops, Operation{Kind: SkipOp, Len: stream.SkipToEnd()})
      mut.Operation = Operation{Kind: StringOp, Operations: ops}
      mut.Dependencies = self.frontier.IDs()
      mut.Site = self.site
      blob, blobref, err := EncodeMutation(mut)
      if err !=  nil {
	panic(err.String())
      }
      self.store.StoreBlob(blob, blobref)
    }
  }
}

// interface IndexListener
func (self *Editor) HandleMutation(mut Mutation) {
//  log.Printf("Apply %v", mut)
  _, err := Execute(self, mut)
  if err != nil {
    panic(err.String())
  }
  self.frontier.Add(mut)
  Stdwin.Refresh()
}

func startGoCurses() (err os.Error) {
  Initscr()
  if Stdwin == nil {
    return os.NewError("Could not init curses")
  }	
  Noecho()	
  Nonl()
//  if err = Curs_set(2); err != nil {
//    return
//  }
  Stdwin.Keypad(true);	
  if err = Start_color(); err != nil {
    return
  }
  return
}

func stopGoCurses() {
  Endwin()
}

func uuid() string {
  f, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0) 
  b := make([]byte, 16) 
  f.Read(b) 
  f.Close() 
  return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]) 
}
