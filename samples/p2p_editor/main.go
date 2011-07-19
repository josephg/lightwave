package main

import (
  . "curses"
  ot "lightwaveot"
  store "lightwavestore"
  fed "lightwavefed"
  grapher "lightwavegrapher"
  tf "lightwavetransformer"
  api "lightwaveapi"
  "flag"
  "os"
  "http"
  "fmt"
)

type dummyNameService struct {
}

func (self *dummyNameService) Lookup(identity string) (url string, err os.Error) {
  switch identity {
  case "a@alice":
    return "http://localhost:8181/fed", nil
  case "b@bob":
    return "http://localhost:8282/fed", nil
  case "c@charly":
    return "http://localhost:8383/fed", nil
  case "d@daisy":
    return "http://localhost:8484/fed", nil
  }
  return "", os.NewError("Unknown identity")
}

func main() {
  // Parse the command line
  var userid string
  flag.StringVar(&userid, "u", "a@alice", "ID of the user owning the blob store, e.g. 'b@fed1.com'")
  var laddr string
  flag.StringVar(&laddr, "l", "", "Network address of the local store port, e.g. ':6161'")
  var raddr string
  flag.StringVar(&raddr, "r", "", "Network address of a remote store port to sync with, e.g. 'fed2.com:6161' (optional)")
  var port int
  flag.IntVar(&port, "p", 0, "Port for accepting HTTP connections")
  flag.Parse()
  
  // Start Curses
  err := startGoCurses()
  defer stopGoCurses()
  if err != nil {
    panic(err.String())
  }
  Init_pair(1, COLOR_RED, COLOR_BLACK)

  // Initialize Store, Indexer and Network
  s := store.NewSimpleBlobStore()
  var federation *fed.Federation
  if port != 0 {
    ns := &dummyNameService{}
    federation = fed.NewFederation(userid, "localhost", port, http.DefaultServeMux, ns, s)
    go http.ListenAndServe(fmt.Sprintf(":%v", port), nil)
  }
  grapher := grapher.NewGrapher(userid, s, federation)
  tf.NewTransformer(grapher)
  a := api.NewUniAPI(userid, grapher)
  
  if raddr != "" && laddr != "" {
    replication := store.NewReplication(userid, s, laddr, raddr)
    // Accept incoming network connections
    go replication.Listen()
  }    
  
  // Launch the UI
  editor := NewEditor(userid, grapher, a)
  editor.ranges = []*ot.TextRange{&ot.TextRange{ot.TextMarker{0}, ot.TextMarker{0}}}
  editor.Refresh()
  
  // Wait for UI events
  editor.Loop()
}
