package main

import (
  . "curses"
  . "lightwaveot"
  "flag"
)

func main() {
  // Parse the command line
  var csAddr string
  flag.StringVar(&csAddr, "s", ":6868", "Address of the server")
  flag.Parse()
  
  // Start Curses
  err := startGoCurses()
  defer stopGoCurses()
  if err != nil {
    panic(err.String())
  }
  Init_pair(1, COLOR_RED, COLOR_BLACK)

  // Initialize Indexer and Network
  indexer := NewIndexer()
  csProto := NewCSProtocol(csAddr, indexer)
  indexer.SetCSProtocol(csProto)
  
  // Launch the UI
  editor := NewEditor(indexer)
  editor.ranges = []*TextRange{&TextRange{TextMarker{0}, TextMarker{0}}}
  editor.Refresh()
  
  // Connect to the server
  err = csProto.Dial()
  if err != nil {
    panic("Could not connect to server")
  }
  
  // Wait for UI events
  editor.Loop()
}
