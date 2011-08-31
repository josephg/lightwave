package lightwave

import (
  "os"
  "log"
  "appengine"
  "appengine/datastore"
  "crypto/sha256"
  "encoding/hex"
  grapher "lightwavegrapher"
)

type blobStruct struct {
  data []byte
}

type pendingStruct struct {
  WaitingForCount int64
}

type missingStruct struct {
  Pending []string
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
  err = self.grapher.HandleBlob(blob, blobref)
  return blobref, err
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

func (self *store) StoreNode(perma_blobref string, blobref string, data map[string]interface{}, perma_data map[string]interface{}) (err os.Error) {
  log.Printf("Storing node ...")
  // Copy the mime type for better searching
  if data["k"].(int64) == int64(grapher.OTNode_Keep) {
    data["mt"] = perma_data["mt"]
  }
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  // Since we cannot do anchestor queries :-(
//  data["perma"] = perma_blobref
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

func (self *store) GetOTNodeBySeqNumber(perma_blobref string, seqNumber int64) (data map[string]interface{}, err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  query := datastore.NewQuery("node").Ancestor(parent).Filter("seq>=", seqNumber)
  it := query.Run(self.c)
  data = make(datastore.Map)
  _, e := it.Next(data)
  if e == datastore.Done {
    return nil, os.NewError("No such node")
  }
  return
}

func (self *store) GetMutationsAscending(perma_blobref string, entity_blobref string, field string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan map[string]interface{}, err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  query := datastore.NewQuery("node").Ancestor(parent).Filter("k =", int64(grapher.OTNode_Mutation)).Filter("e =", entity_blobref).Filter("f =", field).Filter("seq >=", startWithSeqNumber).Order("seq")
  if endSeqNumber >= 0 {
    query = query.Filter("seq <", endSeqNumber)
  }
  channel := make(chan map[string]interface{})
  f := func() {
    for it := query.Run(self.c) ; ; {
      m := make(datastore.Map)
      _, e := it.Next(m)
      if e == datastore.Done {
	break
      }
      if e != nil {
	log.Printf("Err: Query %v", e)
	close(channel)
	return
      }
      channel <- m
    }
    close(channel)
  }
  go f()
  
  return channel, nil
}

func (self *store) GetOTNodesAscending(perma_blobref string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan map[string]interface{}, err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  query := datastore.NewQuery("node").Ancestor(parent).Filter("seq >=", startWithSeqNumber).Order("seq")
  if endSeqNumber >= 0 {
    query = query.Filter("seq <", endSeqNumber)
  }
  channel := make(chan map[string]interface{})
  f := func() {
    for it := query.Run(self.c) ; ; {
      m := make(datastore.Map)
      _, e := it.Next(m)
      if e == datastore.Done {
	break
      }
      if e != nil {
	log.Printf("Err: Query %v", e)
	close(channel)
	return
      }
      channel <- m
    }
    close(channel)
  }
  go f()
  
  return channel, nil
}

func (self *store) GetOTNodesDescending(perma_blobref string) (ch <-chan map[string]interface{}, err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  query := datastore.NewQuery("node").Ancestor(parent).Order("-seq")
  
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
      channel <- m
    }
    close(channel)
  }
  go f()
  
  return channel, nil
}

func (self *store) Enqueue(perma_blobref string, blobref string, dependencies []string) (err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  key := datastore.NewKey("pending", blobref, 0, parent)
  p := pendingStruct{int64(len(dependencies))}
  // Store it
  _, err = datastore.Put(self.c, key, &p)
  if err != nil {
    return
  }
  for _, dep := range dependencies {
    key := datastore.NewKey("missing", dep, 0, parent)
    var m missingStruct
    if err = datastore.Get(self.c, key, &m); err != nil {
      if err != datastore.ErrNoSuchEntity {
	return
      }
    }
    m.Pending = append(m.Pending, blobref)
    // Store it
    _, err = datastore.Put(self.c, key, &m)
    if err != nil {
      return err
    }
  }
  return nil
}

func (self *store) Dequeue(perma_blobref string, blobref string) (blobrefs []string, err os.Error) {
  parent := datastore.NewKey("perma", perma_blobref, 0, nil)
  key := datastore.NewKey("missing", blobref, 0, parent)
  var m missingStruct
  if err = datastore.Get(self.c, key, &m); err != nil {
    if err == datastore.ErrNoSuchEntity {
      return nil, nil
    }
  }
  err = datastore.Delete(self.c, key)
  if err != nil {
    return
  }
  for _, dep := range m.Pending {
    key := datastore.NewKey("pending", dep, 0, parent)
    var p pendingStruct
    if err = datastore.Get(self.c, key, &m); err != nil {
      if err != datastore.ErrNoSuchEntity {
	continue
      }
    }
    p.WaitingForCount--
    if p.WaitingForCount == 0 {
      blobrefs = append(blobrefs, dep)
      datastore.Delete(self.c, key)
    }
  }
  return
}

func (self *store) ListPermas(userid string, mimeType string) (perma_blobrefs []string, err os.Error) {
  // TODO: Use query GetAll?
  query := datastore.NewQuery("node").Filter("k =", int64(grapher.OTNode_Keep)).Filter("s =", userid).KeysOnly()
  if mimeType != "" {
    query = query.Filter("mt =", mimeType)
  }
  for it := query.Run(self.c) ; ; {
    key, e := it.Next(nil)
    if e == datastore.Done {
      return
    }
    if e != nil {
      log.Printf("Err: in query: %v",e)
      return nil, e
    }
    perma_blobrefs = append(perma_blobrefs, key.Parent().StringID())
  }
  return
}

type inboxStruct struct {
  LastSeq int64
  Archived bool
}

func (self *store) ListInbox(userid string, includeArchived bool) (inbox []map[string]interface{}, err os.Error) {
  // TODO: Use query GetAll?
  parent := datastore.NewKey("user", userid, 0, nil)
  query := datastore.NewQuery("inbox").Ancestor(parent)
  if !includeArchived {
    query = query.Filter("Archived =", false)
  }
  for it := query.Run(self.c) ; ; {
    val := &inboxStruct{}
    key, e := it.Next(val)
    if e == datastore.Done {
      return
    }
    if e != nil {
      log.Printf("Err: in query: %v",e)
      return nil, e
    }
    entry := make(map[string]interface{})
    entry["perma"] = key.StringID()
    entry["seq"] = val.LastSeq
    inbox = append(inbox, entry)
  }
  return
}

func (self *store) GetInboxItem(userid string, perma_blobref string) (item *inboxStruct, err os.Error) {
  b := &inboxStruct{}
  parent := datastore.NewKey("user", userid, 0, nil)
  err = datastore.Get(self.c, datastore.NewKey("inbox", perma_blobref, 0, parent), b)
  return b, err  
}

func (self *store) MarkInboxItemAsRead(userid string, perma_blobref string, seq int64) (err os.Error) {
  b, err := self.GetInboxItem(userid, perma_blobref)
//  if err == datastore.ErrNoSuchEntity {
//    b = &inboxStruct{LastSeq: seq, Archived: false}
//  } else if err != nil {
  if err != nil {
    return err
  }
  if seq <= b.LastSeq {
    return
  }
  b.LastSeq = seq
  // Store it
  parent := datastore.NewKey("user", userid, 0, nil)
  _, err = datastore.Put(self.c, datastore.NewKey("inbox", perma_blobref, 0, parent), b)
  return err  
}

func (self *store) MarkInboxItemAsArchived(userid string, perma_blobref string) (err os.Error) {
  b, err := self.GetInboxItem(userid, perma_blobref)
  if err != nil {
    return err
  }
  if (b.Archived) {
    return
  }
  b.Archived = true
  // Store it
  parent := datastore.NewKey("user", userid, 0, nil)
  _, err = datastore.Put(self.c, datastore.NewKey("inbox", perma_blobref, 0, parent), b)
  return err  
}

func (self *store) CreateUser(userid, passwd, email string) (usr *userStruct, err os.Error) {
  // TODO: Salt the password
  usr = &userStruct{UserEmail: email, UserPasswd: passwd}
  _, err = datastore.Put(self.c, datastore.NewKey("user", userid, 0, nil), usr)
  return
}
