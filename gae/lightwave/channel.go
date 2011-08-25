package lightwave

import (
  "appengine"
  "appengine/channel"
  "appengine/user"
  "appengine/datastore"
  "appengine/memcache"
  "appengine/taskqueue"
  "encoding/binary"
  grapher "lightwavegrapher"
  ot "lightwaveot"
  "json"
  "log"
  "os"
)

type channelAPI struct {
  grapher *grapher.Grapher
  c appengine.Context
  store *store
  sessionID string
  userID string
  bufferOnly bool
  messageBuffer[] string
}

func newChannelAPI(c appengine.Context, store *store, sessionid string, bufferOnly bool, grapher *grapher.Grapher) *channelAPI {
  u := user.Current(c)
  a := &channelAPI{sessionID: sessionid, store: store, userID: u.Id, c: c, bufferOnly: bufferOnly, grapher: grapher}
  grapher.SetAPI(a)
  return a
}

func (self* channelAPI) Signal_ReceivedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode) {
  // TODO: Compute digest
  var digest = "Untitled page";
  msgJson := map[string]interface{}{ "perma":perma.BlobRef(), "type":"invitation", "signer":permission.Signer(), "permission":permission.BlobRef(), "digest": digest}
  fillInboxItem(self.store, perma.BlobRef(), int64(0), msgJson)
  schema, err := json.Marshal(msgJson)
  if err != nil {
    panic(err.String())
  }
  if self.bufferOnly {
    self.messageBuffer = append(self.messageBuffer, string(schema));
  } else {  
    if perma.MimeType() == "application/x-lightwave-page" {
      addToInbox(self.c, permission.UserName(), perma.BlobRef(), 0);
    }
    err = self.forwardToUser(permission.UserName(), string(schema))
  }
  if err != nil {
    log.Printf("Err Forward: %v", err)
  }
  
  // Automatically accept the invitation
//  self.grapher.CreateKeepBlob(perma.BlobRef(), permission.BlobRef())
}

func (self* channelAPI) Signal_AcceptedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
  msgJson := map[string]interface{}{ "perma":perma.BlobRef(), "type":"accept", "signer":permission.Signer(), "permission":permission.BlobRef()}
  schema, err := json.Marshal(msgJson)
  if err != nil {
    panic(err.String())
  }
  if self.bufferOnly {
    self.messageBuffer = append(self.messageBuffer, string(schema));
  } else {
    err = self.forwardToUser(keep.Signer(), string(schema))
  }
  if err != nil {
    log.Printf("Err Forward: %v", err)
  }
}

func (self* channelAPI) Blob_Keep(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
  mutJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": keep.SequenceNumber(), "type":"keep", "signer":keep.Signer(), "mimetype": perma.MimeType()}
  if permission != nil {
    mutJson["permission"] = permission.SequenceNumber()
  }
  schema, err := json.Marshal(mutJson)
  if err != nil {
    panic(err.String())
  }
  message := string(schema)
  if self.bufferOnly {
    self.messageBuffer = append(self.messageBuffer, string(schema));
  } else {
    self.forwardToUser(keep.Signer(), message)
    err = self.forwardToFollowers(perma.BlobRef(), message)
  }
  if err != nil {
    log.Printf("Err Forward: %v", err)
  }
}

func (self *channelAPI) Blob_Entity(perma grapher.PermaNode, entity grapher.EntityNode) {
  entityJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": entity.SequenceNumber(), "type":"entity", "signer":entity.Signer(), "mimetype": entity.MimeType(), "blobref": entity.BlobRef()}
  msg := json.RawMessage(entity.Content())
  entityJson["content"] = &msg
  schema, err := json.Marshal(entityJson)
  if err != nil {
    panic(err.String())
  }
  if self.bufferOnly {
    self.messageBuffer = append(self.messageBuffer, string(schema));
  } else {
    err = self.forwardToFollowers(perma.BlobRef(), string(schema))
  }
  if err != nil {
    log.Printf("Err Forward: %v", err)
  }
}

func (self *channelAPI) Blob_DeleteEntity(perma grapher.PermaNode, entity grapher.DelEntityNode) {
  entityJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": entity.SequenceNumber(), "type":"delentity", "signer":entity.Signer(), "blobref": entity.BlobRef(), "entity": entity.EntityBlobRef()}
  schema, err := json.Marshal(entityJson)
  if err != nil {
    panic(err.String())
  }
  if self.bufferOnly {
    self.messageBuffer = append(self.messageBuffer, string(schema));
  } else {
    err = self.forwardToFollowers(perma.BlobRef(), string(schema))
  }
  if err != nil {
    log.Printf("Err Forward: %v", err)
  }
}

func (self* channelAPI) Blob_Mutation(perma grapher.PermaNode, mutation grapher.MutationNode) {
  mutJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": mutation.SequenceNumber(), "type":"mutation", "signer":mutation.Signer(), "entity": mutation.EntityBlobRef(), "field": mutation.Field(), "time": mutation.Time()}
  switch mutation.Operation().(type) {
  case ot.Operation:
    op := mutation.Operation().(ot.Operation) // The following two lines work around a problem in GO/JSON
    mutJson["op"] = &op
  case []byte:
    msg := json.RawMessage(mutation.Operation().([]byte))
    mutJson["op"] = &msg
  default:
    panic("Unsupported operation kind")
  }
  schema, err := json.Marshal(mutJson)
  if err != nil {
    panic(err.String())
  }
  if self.bufferOnly {
    self.messageBuffer = append(self.messageBuffer, string(schema));
  } else {
    err = self.forwardToFollowers(perma.BlobRef(), string(schema))
  }
  if err != nil {
    log.Printf("Err Forward: %v", err)
  }
}

func (self* channelAPI) Blob_Permission(perma grapher.PermaNode, permission grapher.PermissionNode) {
  mutJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": permission.SequenceNumber(), "type":"permission", "user": permission.UserName(), "allow": permission.AllowBits(), "deny": permission.DenyBits(), "blobref": permission.BlobRef()}
  switch permission.Action() {
  case grapher.PermAction_Invite:
    mutJson["action"] = "invite"
  case grapher.PermAction_Expel:
    mutJson["action"] = "expel"
  case grapher.PermAction_Change:
    mutJson["action"] = "change"
  default:
    panic("Unknown action")
  }
  schema, err := json.Marshal(mutJson)
  if err != nil {
    panic(err.String())
  }
  if self.bufferOnly {
    self.messageBuffer = append(self.messageBuffer, string(schema));
  } else {
    err = self.forwardToFollowers(perma.BlobRef(), string(schema))
  }
  if err != nil {
    log.Printf("Err Forward: %v", err)
  }
}

func (self* channelAPI) forwardToSession(userid string, sessionid string, message string) (err os.Error) {
//  log.Printf("Sending to session %v: %v", userid+ "/" + sessionid, message)
  err = channel.Send(self.c, userid + "/" + sessionid, message)
  if err != nil {
    log.Printf("Failed sending to channel %v", userid + "/" + sessionid)
  }
  return nil
}

func (self* channelAPI) forwardToUser(username string, message string) (err os.Error) {
  channels, err := self.channelsByUser(username)
  if err != nil {
    return err
  }
  for _, ch := range channels {
    log.Printf("Sending to user %v", ch.UserID + "/" + ch.SessionID)
    err = channel.Send(self.c, ch.UserID + "/" + ch.SessionID, message)
    if err != nil {
      log.Printf("Failed sending to channel %v", ch.UserID + "/" + ch.SessionID)
    }
  }
  
  return nil
}

func (self* channelAPI) forwardToFollowers(perma_blobref string, message string) (err os.Error) {
  channels, err := self.channelsByFollowers(perma_blobref)
  if err != nil {
    return err
  }
  for _, ch := range channels {
    // Do not send to the session that caused this
    if ch.UserID + "/" + ch.SessionID == self.userID + "/" + self.sessionID {
      continue
    }
    log.Printf("Sending to %v", ch.UserID + "/" + ch.SessionID)
    err = channel.Send(self.c, ch.UserID + "/" + ch.SessionID, message)
    if err != nil {
      log.Printf("Failed sending to channel %v", ch.UserID + "/" + ch.SessionID)
    }
  }
  
  self.sendSlowNotifications(perma_blobref)
  return nil
}

func (self* channelAPI) channelsByFollowers(perma_blobref string) (channels []channelStruct, err os.Error) {
  // TODO: Use query GetAll?
  query := datastore.NewQuery("channel").Filter("OpenPermas =", perma_blobref)
  for it := query.Run(self.c) ; ; {
    var data channelStruct
    _, e := it.Next(&data)
    if e == datastore.Done {
      return
    }
    if e != nil {
      log.Printf("Err: in query: %v",e)
      return nil, e
    }
    channels = append(channels, data)
  }
  return
}

func (self* channelAPI) channelsByUser(username string) (channels []channelStruct, err os.Error) {
  // TODO: Use query GetAll?
  log.Printf("Searching for usr %v", username)
  query := datastore.NewQuery("channel").Filter("UserEmail =", username)
  for it := query.Run(self.c) ; ; {
    var data channelStruct
    _, e := it.Next(&data)
    if e == datastore.Done {
      return
    }
    if e != nil {
      log.Printf("Err: in query: %v",e)
      return nil, e
    }
    channels = append(channels, data)
  }
  return
}

func (self *channelAPI) sendSlowNotifications(perma_blobref string) {
  // Try 10 times then give up
  for i := 0; i < 10; i++ {
    item, err := memcache.Get(self.c, "notify-" + perma_blobref)    
    if err == memcache.ErrCacheMiss {
      // Try to start a task
      if item != nil {
	binary.LittleEndian.PutUint64(item.Value, 1)
	err = memcache.CompareAndSwap(self.c, item)
      } else {
	item = &memcache.Item{ Key: "notify-" + perma_blobref, Value: make([]byte, 8) }
	binary.LittleEndian.PutUint64(item.Value, 1)
	err = memcache.Add(self.c, item)
      }
      if err == memcache.ErrCASConflict {
	// Somebody else managed to launch the task first
	return
      }
      // Enqueue the task
      t := taskqueue.NewPOSTTask("/internal/notify", map[string][]string{"perma": {perma_blobref} })
      t.Delay = 30 * 1000000
      if _, err := taskqueue.Add(self.c, t, ""); err != nil {
	log.Printf("ERR: " + err.String())
	return
      }
      return
    } else if err != nil {
      continue
    }
    val := binary.LittleEndian.Uint64(item.Value)
    val++
    binary.LittleEndian.PutUint64(item.Value, val)
    err = memcache.CompareAndSwap(self.c, item)
    // The task is enqueued and has not yet sent notifications
    if err == nil {
      return
    }
  }
}
