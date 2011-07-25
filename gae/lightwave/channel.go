package lightwave

import (
  "appengine"
  "appengine/channel"
  "appengine/datastore"
  grapher "lightwavegrapher"
  ot "lightwaveot"
  "json"
  "log"
  "os"
)

type channelAPI struct {
  c appengine.Context
  sessionID string
  userID string
  grapher *grapher.Grapher
}

func newChannelAPI(c appengine.Context, userid string, sessionid string, grapher *grapher.Grapher) *channelAPI {
  a := &channelAPI{grapher: grapher, sessionID: sessionid, userID: userid, c: c}
  grapher.SetAPI(a)
  return a
}

func (self* channelAPI) Signal_ReceivedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode) {
  msgJson := map[string]interface{}{ "perma":perma.BlobRef(), "type":"invitation", "signer":permission.Signer(), "permission":permission.BlobRef()}
  schema, err := json.Marshal(msgJson)
  if err != nil {
    panic(err.String())
  }
  self.forwardToUser(permission.UserName(), string(schema))
}

func (self* channelAPI) Signal_AcceptedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
  msgJson := map[string]interface{}{ "perma":perma.BlobRef(), "type":"accept", "signer":permission.Signer(), "permission":permission.BlobRef()}
  schema, err := json.Marshal(msgJson)
  if err != nil {
    panic(err.String())
  }
  self.forwardToUser(keep.Signer(), string(schema))
}

func (self* channelAPI) Blob_Keep(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
  mutJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": keep.SequenceNumber(), "type":"keep", "signer":keep.Signer()}
  if permission != nil {
    mutJson["permission"] = permission.SequenceNumber()
  }
  schema, err := json.Marshal(mutJson)
  if err != nil {
    panic(err.String())
  }
  message := string(schema)
  self.forwardToUser(keep.Signer(), message)
  self.forwardToFollowers(perma.BlobRef(), message)
}

func (self* channelAPI) Blob_Mutation(perma grapher.PermaNode, mutation grapher.MutationNode) {
  mutJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": mutation.SequenceNumber(), "type":"mutation", "signer":mutation.Signer()}
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
  self.forwardToFollowers(perma.BlobRef(), string(schema))
}

func (self* channelAPI) Blob_Permission(perma grapher.PermaNode, permission grapher.PermissionNode) {
  mutJson := map[string]interface{}{ "perma":perma.BlobRef(), "seq": permission.SequenceNumber(), "type":"permission", "user": permission.UserName(), "allow": permission.AllowBits(), "deny": permission.DenyBits()}
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
  self.forwardToFollowers(perma.BlobRef(), string(schema))
}

func (self* channelAPI) forwardToUser(username string, message string) (err os.Error) {
  channels, err := self.channelsByUser(username)
  if err != nil {
    return err
  }
  for _, ch := range channels {
    log.Printf("Sending to %v", ch.UserID + "/" + ch.SessionID)
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
    log.Printf("Sending to %v", ch.UserID + "/" + ch.SessionID)
    err = channel.Send(self.c, ch.UserID + "/" + ch.SessionID, message)
    if err != nil {
      log.Printf("Failed sending to channel %v", ch.UserID + "/" + ch.SessionID)
    }
  }
  
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
  query := datastore.NewQuery("channel").Filter("UserName =", username)
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
