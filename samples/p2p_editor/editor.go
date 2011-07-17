package main

import (
  . "curses"
  . "lightwaveot"
  . "lightwavestore"
  tf "lightwavetransformer"
  grapher "lightwavegrapher"
  "os"
  "fmt"
  "strings"
  vec "container/vector"
)

type Editor struct {
  userID string
  store BlobStore
  grapher *grapher.Grapher
  api tf.UniAPI
  text string
  tombs vec.IntVector
  // Required during mutations
  mutPos, mutLinePos, mutLine int
  mutTombs *TombStream
  Rows, Columns int
  ScrollX, ScrollY int
  ranges []*TextRange  // The first range is the cursor. Other ranges are cursors of other users
  // The document we are currently editing
  permaBlobRef string
  // List of all perma nodes
  permaBlobs []string
  invitations map[string]string
  seqNumber int
}

func NewEditor(userid string, store BlobStore, grapher *grapher.Grapher, api tf.UniAPI) *Editor {
  e := &Editor{userID: userid, store:store, grapher: grapher, api: api, Rows: *Rows, Columns: *Cols, invitations: make(map[string]string)}
  api.SetApplication(e)
  return e
}

// Application interface
func (self *Editor) Signal_ReceivedInvitation(permission *tf.Permission) {
  self.invitations[permission.PermaBlobRef] = permission.PermissionBlobRef
}

// Application interface
func (self *Editor) Signal_AcceptedInvitation(Keep *tf.Keep) {
}

func (self *Editor) Signal_ProcessedKeep(keep *tf.Keep) {
  self.permaBlobs = append(self.permaBlobs, keep.PermaBlobRef)
}

// Application interface
func (self *Editor) Blob(blob interface{}, seqNumber int) {
  switch blob.(type) {
  case *tf.Keep:
    // Do nothing yet
  case *tf.Permission:
    // Do nothing yet
  case *tf.Mutation:
    mut := blob.(*tf.Mutation)
    op, ok := mut.Operation.(Operation)
    if !ok {
      panic("Unknown operator kind")
    }
    _, err := ExecuteOperation(self, op)
    if err != nil {
      panic(err.String())
    }
    Stdwin.Refresh()
  default:
    panic("Unknown blob kind")
  }
  
  if seqNumber >= 0 {
    self.seqNumber = seqNumber + 1
  }
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
  Stdwin.Addstr(0, self.Rows - 1, "ESC-q=Quit ESC-i=Invite ESC-l=List", 0)
  // Show the cursor
  linepos, line = self.CursorToScreenPos(self.Cursor())
  Stdwin.Move(linepos - self.ScrollX, line - self.ScrollY)
}

func (self *Editor) Loop() {
  for {
    Stdwin.Clear()
    Stdwin.Addstr(1, 1, "1. New document", 0)
    Stdwin.Addstr(1, 2, "2. Browse documents", 0)
    Stdwin.Addstr(1, 3, "3. Browse invitations", 0)
    Stdwin.Addstr(1, 4, "4. Quit", 0)
    Stdwin.Addstr(1, 6, "Enter a number: ", 0)
    Stdwin.Refresh()
    inp := Stdwin.Getch()
    switch inp {
    case '1':
      perma, err := self.grapher.CreatePermaBlob()
      if err != nil {
	panic(err.String())
      }
      _, err = self.grapher.CreateKeepBlob(perma, "")
      if err != nil {
	panic(err.String())
      }
      self.open(perma)
      self.editLoop()
    case '2':
      Stdwin.Clear()
      Stdwin.Addstr(1, 0, "Browse documents", 0)
      for i, name := range self.permaBlobs {
	Stdwin.Addstr(1, 2 + i, fmt.Sprintf("%v: %v", i + 1, name) , 0)
      }
      Stdwin.Addstr(0, self.Rows - 1, "ESC=Menu", 0)
      Stdwin.Refresh()
      inp := Stdwin.Getch()
      inp = inp - int('1')
      if inp >= 0 && inp < len(self.permaBlobs) {
	self.open(self.permaBlobs[inp])
	self.editLoop()
      }
    case '3':
      Stdwin.Clear()
      Stdwin.Addstr(1, 0, "Browse Invitations", 0)
      i := 0
      invitations := []string{}
      for name, _ := range self.invitations {
	Stdwin.Addstr(1, 2 + i, fmt.Sprintf("%v: %v", i + 1, name) , 0)
	i++
	invitations = append(invitations, name)
      }
      Stdwin.Addstr(0, self.Rows - 1, "ESC=Menu", 0)
      Stdwin.Refresh()
      inp := Stdwin.Getch()
      inp = inp - int('1')
      if inp >= 0 && inp < len(invitations) {
	_, err := self.grapher.CreateKeepBlob(invitations[inp], self.invitations[invitations[inp]])
	if err != nil {
	  panic(err.String())
	}
	self.open(invitations[inp])
	self.editLoop()
      }
    case '4':
      return
    }
  }
}

func (self *Editor) editLoop() {
  for {
    inp := Stdwin.Getch()
    // panic(fmt.Sprintf("KEY %v", inp))
    linePos, line := self.CursorToScreenPos(self.Cursor())
    switch inp {
    case 27: // ESC
      inp := Stdwin.Getch()
      switch inp {
      case 'i':
	Stdwin.Addstr(0, self.Rows - 1, "Invite: ", 0)
	Stdwin.Clrtoeol()
	entered := false
	userid := ""
	for !entered {
	  inp := Stdwin.Getch()
	  switch inp {
	  case KEY_ENTER, 13, 10:
	    entered = true
	    self.Refresh()
	  default:
	    userid = userid + string(inp)
	    Stdwin.Addstr(len(userid) + 7, self.Rows - 1, string(inp), 0)
	  }
	}
	self.grapher.CreatePermissionBlob(self.permaBlobRef, self.seqNumber, userid, grapher.Perm_Read | grapher.Perm_Write | grapher.Perm_Invite, 0, grapher.PermAction_Invite)
      case 'l':
      case 'q':
	return
      }
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
      _, err := self.grapher.CreateMutationBlob(self.permaBlobRef, Operation{Kind: StringOp, Operations: ops}, self.seqNumber)
      if err != nil {
	panic(err.String())
      }
    default:
      if inp == KEY_ENTER || inp == 13 {
	inp = 10
      }
      var ops []Operation
      stream := NewTombStream(&self.tombs)
      skipped, _ := stream.SkipChars(self.Cursor())
      if skipped > 0 {
	ops = append(ops, Operation{Kind: SkipOp, Len: skipped})
      }
      ops = append(ops, Operation{Kind: InsertOp, Len: 1, Value: string(inp)})
      skipped = stream.SkipToEnd()
      if skipped > 0 {
	ops = append(ops, Operation{Kind: SkipOp, Len: stream.SkipToEnd()})
      }
      _, err := self.grapher.CreateMutationBlob(self.permaBlobRef, Operation{Kind: StringOp, Operations: ops}, self.seqNumber)
      if err != nil {
	panic(err.String())
      }
    }
  }
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

func (self *Editor) open(perma_blobref string) {
  if self.permaBlobRef != "" {
    self.api.Close(self.permaBlobRef)
  }
  self.permaBlobRef = perma_blobref
  self.tombs = vec.IntVector{}
  self.text = ""
  self.seqNumber = 0
  self.ScrollX = 0
  self.ScrollY = 0
  self.ranges = []*TextRange{&TextRange{TextMarker{0}, TextMarker{0}}}
  Stdwin.Clear()
  self.Refresh()
  Stdwin.Refresh()
  self.api.Open(self.permaBlobRef, 0)
}

func stopGoCurses() {
  Endwin()
}

/*
func uuid() string {
  f, _ := os.OpenFile("/dev/urandom", os.O_RDONLY, 0) 
  b := make([]byte, 16) 
  f.Read(b) 
  f.Close() 
  return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]) 
}
*/
