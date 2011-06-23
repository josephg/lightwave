package main

import (
  "flag"
  . "lightwavestore"
)

func main() {
  // Parse the command line
  var identity string
  flag.StringVar(&identity, "l", "", "Name of the server, e.g. 'fed1.com'")
  var peerName string
  flag.StringVar(&peerName, "p", "", "Name of a remote peer, e.g. 'fed2.com' (optional)")
  var csAddr string
  flag.StringVar(&csAddr, "s", "6868", "Address of the client server protocol")
  flag.Parse()
  
  // Initialize Store, Indexer and Network
  store := NewSimpleBlobStore()
  ns := &dummyNameService{}
  replication := NewReplication(identity, ns, store)
  indexer := NewIndexer(store)
  csproto := NewCSProtocol(store, indexer, csAddr)
  
  // Accept incoming network connections
  go replication.Listen()

  // Create an outgoing network connection
  if peerName != "" {
    err := replication.Dial(peerName)
    if err != nil {
      panic("Could not connect to remote peer")
    }
  }
  
  // Accept clients
  csproto.Listen()
}
