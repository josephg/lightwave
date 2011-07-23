package lightwave

import (
  "appengine"
  "appengine/channel"
  "appengine/datastore"
  grapher "lightwavegrapher"
  "log"
  "os"
)

type channelAPI struct {
  c appengine.Context
  grapher *grapher.Grapher
}

func newChannelAPI(c appengine.Context, grapher *grapher.Grapher) *channelAPI {
  a := &channelAPI{grapher: grapher, c: c}
  grapher.SetAPI(a)
  return a
}

func (self* channelAPI) Signal_ReceivedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode) {
}

func (self* channelAPI) Signal_AcceptedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
}

func (self* channelAPI) Blob_Keep(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
  log.Printf("Keep")
  self.forward(perma.BlobRef(), "Hello keep")
}

func (self* channelAPI) Blob_Mutation(perma grapher.PermaNode, mutation grapher.MutationNode) {
  log.Printf("Mut")
  self.forward(perma.BlobRef(), "Hello mut")
}

func (self* channelAPI) Blob_Permission(perma grapher.PermaNode, permission grapher.PermissionNode) {
  log.Printf("Perm")
}

func (self* channelAPI) forward(perma_blobref string, message string) (err os.Error) {
  channels, err := self.followers(perma_blobref)
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
