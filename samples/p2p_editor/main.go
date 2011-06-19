package main

import (
  . "curses"
  . "lightwaveot"
  fed "lightwavefed"
  "flag"
)

func main() {
  // Parse the command line
  var identity string
  flag.StringVar(&identity, "l", "", "Name of the server, e.g. 'fed1.com'")
  var peerName string
  flag.StringVar(&peerName, "p", "", "Name of a remote peer, e.g. 'fed2.com' (optional)")
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
  ns := &dummyNameService{}
  federation := fed.NewFederation(identity, ns, store)
  indexer := NewIndexer(store)
  
  // Launch the UI
  editor := NewEditor(store, indexer)
  editor.ranges = []*TextRange{&TextRange{TextMarker{0}, TextMarker{0}}}
  editor.Refresh()
  
  // Accept incoming network connections
  go federation.Listen()
  
  // Create an outgoing network connection
  if peerName != "" {
    err := federation.Dial(peerName)
    if err != nil {
      panic("Could not connect to remote peer")
    }
  }
  // Wait for UI events
  editor.Loop()
}
