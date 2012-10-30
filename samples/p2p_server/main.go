package main

import (
  "flag"
  . "lightwave/store"
  "os"
)

func main() {
  // Parse the command line
  var userid string
  flag.StringVar(&userid, "u", "user", "ID of the user owning the blob store, e.g. 'b@bob'")
  var laddr string
  flag.StringVar(&laddr, "l", "", "Network address of the local peer, e.g. ':8181'")
  var raddr string
  flag.StringVar(&raddr, "r", "", "Netwrk address of a remote peer, e.g. 'fed2.com:8282' (optional)")
  var csAddr string
  flag.StringVar(&csAddr, "s", "", "Address of the client server protocol")
  flag.Parse()
  
  // Initialize Store, Indexer and Network
  store := NewSimpleBlobStore()
  indexer := NewIndexer(store)
  
  // Accept incoming network connections
  replication := NewReplication(userid, store, laddr, raddr)
  if laddr != "" {
    println("Replication listening on port", laddr)
    go replication.Listen()
  }
  
  // Accept clients
  if csAddr != "" {
    println("Client protocol listening on port", csAddr)
    csproto := NewCSProtocol(store, indexer, csAddr)
    go csproto.Listen()
  }

  println("Press enter to quit")
  os.Stdin.Read(make([]byte, 1))
}
