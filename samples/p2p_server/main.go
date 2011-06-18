package main

import (
  "flag"
  fed "lightwavefed"
)

func main() {
  // Parse the command line
  var peerAddr string
  flag.StringVar(&peerAddr, "p", "", "Address of a remote peer")
  var localAddr string
  flag.StringVar(&localAddr, "l", ":12345", "Local address of the peer")
  var csAddr string
  flag.StringVar(&csAddr, "s", "6868", "Address of the client server protocol")
  flag.Parse()
  
  // Initialize Store, Indexer and Network
  store := NewStore()
  federation := fed.NewFederation(store)
  indexer := NewIndexer(store)
  csproto := NewCSProtocol(store, indexer, csAddr)
  
  // Accept incoming network connections
  go federation.Listen(localAddr)

  // Create an outgoing network connection
  if peerAddr != "" {
    federation.Dial(peerAddr)
  }
  
  // Accept clients
  csproto.Listen()
}
