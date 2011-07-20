package lightwave

import (
  "fmt"
  "os"
  "appengine"
  "appengine/datastore"
//  "appengine/user"
  "http"
  grapher "lightwavegrapher"
  tf "lightwavetransformer"
)

type Blob struct {
  Data []byte
}

type Graph struct {
  PermaBlobRef string
  PermaSigner string
  SeqNumber int64
}

type GraphNode struct {
  BlobRef string
  Arr []string
}

func init() {
  http.HandleFunc("/", handle)
  http.HandleFunc("/write", handleWriteGraph)
  http.HandleFunc("/write2", handleWriteNode)
  http.HandleFunc("/blob", handleBlob)
}

func handle(w http.ResponseWriter, r *http.Request) {
  fmt.Fprint(w, "<html><body>Hello LightWave demo</body></html>")
}

func handleBlob(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  s := newStore(c)
  g := grapher.NewGrapher("a@b", s, s, nil)
  s.SetGrapher(g)
  tf.NewTransformer(g)

  perma, err := g.CreatePermaBlob()
  if err != nil {
    http.Error(w, err.String(), http.StatusInternalServerError)
    return
  }

  _, err = g.CreateKeepBlob(perma, "")
  if err != nil {
    http.Error(w, err.String(), http.StatusInternalServerError)
    return
  }
  
  fmt.Fprintf(w, "Yep, that worked")
}

func handleWriteGraph(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  
  g := Graph{"1234", "a@b", 0}
  key, err := datastore.Put(c, datastore.NewKey("graph", g.PermaBlobRef, 0, nil), &g)
  if err != nil {
    http.Error(w, err.String(), http.StatusInternalServerError)
    return
  }
  
  var g2 Graph
  if err = datastore.Get(c, key, &g2); err != nil {
    http.Error(w, err.String(), http.StatusInternalServerError)
    return
  }
  
  fmt.Fprintf(w, "Stored and retrieved the Graph named %q", g2.PermaBlobRef)
}

func handleWriteNode(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)

  // Try 5 times to get the transaction through. Otherwise give up
  var terr os.Error
  var nodeKey *datastore.Key
  
  for i := 0; i < 5; i++ {
    terr = datastore.RunInTransaction(c, func(c appengine.Context) os.Error {
      graphKey := datastore.NewKey("graph", "1234", 0, nil)  
      var g Graph
      if err := datastore.Get(c, graphKey, &g); err != nil {
	return err
      }
  
      n := GraphNode{fmt.Sprintf("abcd%v", g.SeqNumber), []string{"a", "b"}}
      nodeKey = datastore.NewKey("node", "", g.SeqNumber + 1, graphKey)
      _, err := datastore.Put(c, nodeKey, &n)
      if err != nil {
	return err
      }

      g.SeqNumber++
      _, err = datastore.Put(c, graphKey, &g)
      if err != nil {
	return err
      }
      return nil
    })
    if terr != datastore.ErrConcurrentTransaction {
      break
    }
  }
  // Transaction failed, most likely because of contention or a server problem. The client needs to retry
  if terr != nil {
    if terr == datastore.ErrConcurrentTransaction {
      http.Error(w, terr.String(), http.StatusRequestTimeout)
    } else { 
      http.Error(w, terr.String(), http.StatusInternalServerError)
    }
    return
  }
  
  var n2 GraphNode
  if err := datastore.Get(c, nodeKey, &n2); err != nil {
    http.Error(w, err.String(), http.StatusInternalServerError)
    return
  }
  
  m := make(datastore.Map)
  if err := datastore.Get(c, nodeKey, m); err != nil {
    http.Error(w, err.String(), http.StatusInternalServerError)
    return
  }
  arr, ok := m["Arr"]
  if !ok {
    fmt.Fprintf(w, "Arr is missing")
  }
  if _, ok = arr.([]string); !ok {
    fmt.Fprintf(w, "Wrong Arr type %T", arr)
  }
  fmt.Fprintf(w, "2. Stored and retrieved the GraphNode named %q", n2.BlobRef)
}

