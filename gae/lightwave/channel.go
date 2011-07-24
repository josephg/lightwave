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
}

func (self* channelAPI) Signal_AcceptedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
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
  self.forward(perma.BlobRef(), string(schema), self.userID + "/" + self.sessionID)
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
  self.forward(perma.BlobRef(), string(schema), "")
}

func (self* channelAPI) Blob_Permission(perma grapher.PermaNode, permission grapher.PermissionNode) {
  log.Printf("Perm")
}

func (self* channelAPI) forward(perma_blobref string, message string, dest_channel string) (err os.Error) {
  channels, err := self.followers(perma_blobref)
  if err != nil {
    return err
  }
  for _, ch := range channels {
    if ch.UserID + "/" + ch.SessionID == dest_channel {
      dest_channel = ""
    }
    log.Printf("Sending to %v", ch.UserID + "/" + ch.SessionID)
    err = channel.Send(self.c, ch.UserID + "/" + ch.SessionID, message)
    if err != nil {
      log.Printf("Failed sending to channel %v", ch.UserID + "/" + ch.SessionID)
    }
  }
  
  if dest_channel != "" {
    log.Printf("Sending to %v", dest_channel)
    err = channel.Send(self.c, dest_channel, message)
    if err != nil {
      log.Printf("Failed sending to channel %v", dest_channel)
    }
  }
  
  return nil
}

func (self* channelAPI) followers(perma_blobref string) (channels []channelStruct, err os.Error) {
  // TODO: This should be the IN operator?
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
