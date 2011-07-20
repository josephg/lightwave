package lightwave

import (
  "os"
  "appengine"
  "appengine/datastore"
  "crypto/sha256"
  "encoding/hex"
  grapher "lightwavegrapher"
)

type blobStruct struct {
  data []byte
}

type store struct {
  c appengine.Context
  grapher *grapher.Grapher
}

func newBlobRef(blob []byte) string {
  h := sha256.New()
  h.Write(blob)
  return string(hex.EncodeToString(h.Sum()))
}

func newStore(c appengine.Context) *store {
  return &store{c: c}
}

func (self *store) StoreBlob(blob []byte, blobref string) (finalBlobRef string, err os.Error) {
  b := blobStruct{data:blob}
  // Empty blob reference?
  if len(blobref) == 0 {
    blobref = newBlobRef(blob)
  }
  
  // Store it
  _, err = datastore.Put(self.c, datastore.NewKey("blob", blobref, 0, nil), &b)
  if err != nil {
    return
  }

  // Process it
  self.grapher.HandleBlob(blob, blobref)
  return blobref, nil
}

func (self *store) SetGrapher(grapher *grapher.Grapher) {
  self.grapher = grapher
}

func (self *store) GetBlob(blobref string) (blob []byte, err os.Error) {
  var b blobStruct
  if err = datastore.Get(self.c, datastore.NewKey("blob", blobref, 0, nil), &b); err != nil {
    return
  }
  return b.data, nil
}

// ------------------------------------------------------------------
// Graph Store

func (self *store) StoreNode(perma_blobref string, blobref string, data map[string]interface{}) (err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  // Since we cannot do anchestor queries :-(
  data["perma"] = perma_blobref
  // Store it
  _, err = datastore.Put(self.c, datastore.NewKey("node", blobref, 0, parent), datastore.Map(data))
  return
}

func (self *store) StorePermaNode(perma_blobref string, data map[string]interface{}) (err os.Error) {
  key := datastore.NewKey("perma", perma_blobref, 0, nil)
  // Store it
  _, err = datastore.Put(self.c, key, datastore.Map(data))
  return
}

func (self *store) GetPermaNode(blobref string) (data map[string]interface{}, err os.Error) {
  m := make(datastore.Map)
  if err = datastore.Get(self.c, datastore.NewKey("perma", blobref, 0, nil), m); err != nil {
    return nil, err
  }
  return m, nil
}

func (self *store) HasOTNodes(perma_blobref string, blobrefs []string) (missing_blobrefs []string, err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  for _ , blobref := range blobrefs {
    m := make(datastore.Map)
    if err = datastore.Get(self.c, datastore.NewKey("node", blobref, 0, parent), m); err != nil {
      if err == datastore.ErrNoSuchEntity {
	missing_blobrefs = append(missing_blobrefs, blobref)
      } else {
	return nil, err
      }
    }
  }
  return
}

func (self *store) GetOTNodeByBlobRef(perma_blobref string, blobref string) (data map[string]interface{}, err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  m := make(datastore.Map)
  if err = datastore.Get(self.c, datastore.NewKey("node", blobref, 0, parent), m); err != nil {
    return nil, err
  }
  return m, nil
}

func (self *store) GetOTNodesAscending(perma_blobref string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan map[string]interface{}, err os.Error) {
  query := datastore.NewQuery("node").Filter("perma =", perma_blobref).Filter("seq >=", startWithSeqNumber).Filter("seq <", endSeqNumber).Order("+seq")
  
  channel := make(chan map[string]interface{})
  f := func() {
    for it := query.Run(self.c) ; ; {
      m := make(datastore.Map)
      _, e := it.Next(m)
      if e == datastore.Done {
	break
      }
      if e != nil {
	close(channel)
	return
      }
    }
    close(channel)
  }
  go f()
  
  return channel, nil
}

func (self *store) GetOTNodesDescending(perma_blobref string) (ch <-chan map[string]interface{}, err os.Error) {
  query := datastore.NewQuery("node").Filter("perma =", perma_blobref).Order("-seq")
  
  channel := make(chan map[string]interface{})
  f := func() {
    for it := query.Run(self.c) ; ; {
      m := make(datastore.Map)
      _, e := it.Next(m)
      if e == datastore.Done {
	break
      }
      if e != nil {
	close(channel)
	return
      }
    }
    close(channel)
  }
  go f()
  
  return channel, nil
}

func (self *store) Enqueue(perma_blobref string, blobref string, dependencies []string) os.Error {
  return nil
}

func (self *store) Dequeue(perma_blobref string, blobref string) (blobrefs []string, err os.Error) {
  return
}

