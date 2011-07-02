package main

import (
  . "curses"
  . "lightwaveot"
  . "lightwavestore"
  "flag"
)

func main() {
  // Parse the command line
  var userid string
  flag.StringVar(&userid, "u", "", "ID of the user owning the blob store")
  var laddr string
  flag.StringVar(&laddr, "l", "", "Network address of the local peer, e.g. ':8181'")
  var raddr string
  flag.StringVar(&raddr, "r", "", "Netwrk address of a remote peer, e.g. 'fed2.com:8282' (optional)")
  flag.Parse()
  
  // Start Curses
  err := startGoCurses()
  defer stopGoCurses()
  if err != nil {
    panic(err.String())
  }
  Init_pair(1, COLOR_RED, COLOR_BLACK)

  // Initialize Store, Indexer and Network
  store := NewSimpleBlobStore()
  replication := NewReplication(userid, store, laddr, raddr)
  indexer := NewIndexer(store)
  
  // Launch the UI
  editor := NewEditor(store, indexer)
  editor.ranges = []*TextRange{&TextRange{TextMarker{0}, TextMarker{0}}}
  editor.Refresh()
  
  // Accept incoming network connections
  go replication.Listen()
  
  // Wait for UI events
  editor.Loop()
}
