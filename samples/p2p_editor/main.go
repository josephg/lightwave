package main

import (
  . "curses"
  . "lightwaveot"
  "flag"
)

var federation *Federation

func main() {
  // Parse the command line
  var peerAddr string
  flag.StringVar(&peerAddr, "p", "", "Address of a remote peer")
  var localAddr string
  flag.StringVar(&localAddr, "l", ":12345", "Local address")
  flag.Parse()
  
  // Start Curses
  err := startGoCurses()
  defer stopGoCurses()
  if err != nil {
    panic(err.String())
  }
  Init_pair(1, COLOR_RED, COLOR_BLACK)

  // Initialize Store, Indexer and Network
  store := NewStore()
  federation = NewFederation(localAddr, store)
  indexer := NewIndexer(store, federation)
  
  // Launch the UI
  editor := NewEditor(store, indexer)
  editor.ranges = []*TextRange{&TextRange{TextMarker{0}, TextMarker{0}}}
  editor.Refresh()
  
  // Accept incoming network connections
  go federation.Listen()
  // Create an outgoing network connection
  if peerAddr != "" {
    federation.Dial(peerAddr)
  }
  // Wait for UI events
  editor.Loop()
}
