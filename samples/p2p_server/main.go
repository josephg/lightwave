package main

import (
  "flag"
  . "lightwavestore"
)

func main() {
  // Parse the command line
  var userid string
  flag.StringVar(&userid, "u", "", "ID of the user owning the blob store, e.g. 'b@bob'")
  var laddr string
  flag.StringVar(&laddr, "l", "", "Network address of the local peer, e.g. ':8181'")
  var raddr string
  flag.StringVar(&raddr, "r", "", "Netwrk address of a remote peer, e.g. 'fed2.com:8282' (optional)")
  var csAddr string
  flag.StringVar(&csAddr, "s", ":6868", "Address of the client server protocol")
  flag.Parse()
  
  // Initialize Store, Indexer and Network
  store := NewSimpleBlobStore()
  replication := NewReplication(userid, store, laddr, raddr)
  indexer := NewIndexer(store)
  csproto := NewCSProtocol(store, indexer, csAddr)
  
  // Accept incoming network connections
  go replication.Listen()
  
  // Accept clients
  csproto.Listen()
}
