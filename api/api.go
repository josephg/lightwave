package lightwaveapi

import (
  grapher "lightwavegrapher"
  "os"
  "sync"
  "log"
)

type Application interface {
  Signal_ReceivedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode)
  // This function is called right after the local user issued his keep but (most of the tim)
  // before all blobs have been downloaded
  Signal_AcceptedInvitation(perma grapher.PermaNode, perm grapher.PermissionNode, keep grapher.KeepNode)
  // This function is called when the keep issued by the local user has been processed.
  // If the perma blob belongs to someone else, then this means all blobs have been downloaded.
  // If the perma blob belongs to the local user, then this signal comes directly after Signal_AcceptedInvitation.
  Signal_ProcessedKeep(perma grapher.PermaNode, keep grapher.KeepNode)
  Blob(perma grapher.PermaNode, blob grapher.OTNode)
}

// The API layer as seen by the application.
// This API assumes that only one application (uni) is sitting on top of the layer stack.
type API interface {
  SetApplication(app Application)
  Open(perma_blobref string, startWithSeqNumber int64) os.Error
  Close(perma_blobref string)
}

type uniAPI struct {
  userID string
  grapher *grapher.Grapher
  app Application
  // The value is the last mutation sent 
  open map[string]int64
  queues map[string]int64
  mutex sync.Mutex
}

func NewUniAPI(userid string, grapher *grapher.Grapher) API {
  a := &uniAPI{userID: userid, grapher: grapher, open: make(map[string]int64), queues: make(map[string]int64)}
  grapher.SetAPI(a)
  return a
}

func (self *uniAPI) SetApplication(app Application) {
  self.app = app
}

func (self *uniAPI) Open(perma_blobref string, startWithSeqNumber int64) (err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  // Is already open?
  if _, ok := self.open[perma_blobref]; ok {
    return os.NewError("Cannot open twice")
  }
  // TODO: Check permissions
  self.open[perma_blobref] = startWithSeqNumber
  // Send all messages queued so far.
  err = self.grapher.Repeat(perma_blobref, startWithSeqNumber) 
  return
}

func (self *uniAPI) Close(perma_blobref string) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  self.open[perma_blobref] = 0, false
  return
}

func (self* uniAPI) Signal_ReceivedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode) {
  self.app.Signal_ReceivedInvitation(perma, permission)
}

func (self* uniAPI) Signal_AcceptedInvitation(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
  self.app.Signal_AcceptedInvitation(perma, permission, keep)
}

func (self* uniAPI) Blob_Keep(perma grapher.PermaNode, permission grapher.PermissionNode, keep grapher.KeepNode) {
  log.Printf("Keep")
  self.blob(perma, keep)
}

func (self* uniAPI) Blob_Mutation(perma grapher.PermaNode, mutation grapher.MutationNode) {
  log.Printf("Mut")
  self.blob(perma, mutation)
}

func (self* uniAPI) Blob_Permission(perma grapher.PermaNode, permission grapher.PermissionNode) {
  log.Printf("Perm")
  self.blob(perma, permission)
}

func (self* uniAPI) blob(perma grapher.PermaNode, blob grapher.OTNode) {
  log.Printf("API blob %v", blob.SequenceNumber())
  self.mutex.Lock()
  // Is this perma blob opened?
  nextSeqNumber, ok := self.open[perma.BlobRef()]
  if !ok {
    // Is this a new keep of the local user? If yes, send it. Otherwise ignore it
    if keep, ok := blob.(grapher.KeepNode); ok && keep.Signer() == self.userID {
      self.mutex.Unlock()
      self.app.Signal_ProcessedKeep(perma, keep)
      return
    }
    self.mutex.Unlock()
    return      
  }
  if nextSeqNumber > blob.SequenceNumber() {
    // Ignore this mutation. We have seen it already (should not happen anyway)
    self.mutex.Unlock()
    return
  }
  if nextSeqNumber < blob.SequenceNumber() {
    // Remember that we need to process these mutations later on, too, but not now
    q, ok := self.queues[perma.BlobRef()]
    if !ok || blob.SequenceNumber() < q {
      self.queues[perma.BlobRef()] = blob.SequenceNumber()
    }
    self.mutex.Unlock()
    return
  }
  // Store the next expected sequence number
  self.open[perma.BlobRef()] = blob.SequenceNumber() + 1
  // Is there a need to continue with queued mutations?
  cont := int64(-1)
  j, ok := self.queues[perma.BlobRef()]
  if ok {
    cont = j
    self.queues[perma.BlobRef()] = 0, false
  }
  self.mutex.Unlock()
  // Notify the application
  self.app.Blob(perma, blob)
  // Ask to repeat further blobs in case we have already seen some
  if cont != -1 {
    self.grapher.Repeat(perma.BlobRef(), cont)
  }
}
