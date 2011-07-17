package lightwavetransformer

import (
  grapher "lightwavegrapher"
  "os"
  "sync"
)

// The API layer as seen by the Transformer
type API interface {
  // This function is called when an invitation has been received.
  // The user can now download the document and issue a keep to follow it.
  Signal_ReceivedInvitation(t Transformer, permission *Permission)
  // This function is called when the local user has accepted an invitation by creating a keep blob
  Signal_AcceptedInvitation(t Transformer, keep *Keep)
  Blob_Keep(t Transformer, keep *Keep, seqNumber int)
  // This function is called when a mutation has been applied.
  // The mutation passed in the parameter is already transformed.
  Blob_Mutation(t Transformer, mut *Mutation, seqNumber int)
  // This function is called when a permission mutation has been applied.
  // The permission passed in the parameter is already transformed
  Blob_Permission(t Transformer, permission *Permission, seqNumber int)
}

type Application interface {
  Signal_ReceivedInvitation(permission *Permission)
  // This function is called right after the local user issued his keep but (most of the tim)
  // before all blobs have been downloaded
  Signal_AcceptedInvitation(keep *Keep)
  // This function is called when the keep issued by the local user has been processed.
  // If the perma blob belongs to someone else, then this means all blobs have been downloaded.
  // If the perma blob belongs to the local user, then this signal comes directly after Signal_AcceptedInvitation.
  Signal_ProcessedKeep(keep *Keep)
  Blob(blob interface{}, seqNumber int)
}

// The API layer as seen by the application.
// This API assumes that only one application (uni) is sitting on top of the layer stack.
type UniAPI interface {
  SetApplication(app Application)
  Open(perma_blobref string, startWithSeqNumber int) os.Error
  Close(perma_blobref string)
}

const (
  Signal_ReceivedInvitation = 1 + iota
  Signal_AcceptedInvitation
  Blob_Permission
  Blob_Keep
  Blob_Mutation
)

type Keep grapher.Keep
type Permission grapher.Permission
type Mutation struct {
  PermaBlobRef string
  PermaSigner string
  MutationBlobRef string
  MutationSigner string
  // For example ot.Operation is stored in here
  Operation interface{}
}

type uniAPI struct {
  userID string
  app Application
  // The value is the last mutation sent 
  open map[string]int
  queues map[string]int
  permas map[string]Transformer
  mutex sync.Mutex
}

func NewUniAPI(userid string) (appInterface UniAPI, transformerInterface API) {
  a := &uniAPI{userID: userid, open: make(map[string]int), permas: make(map[string]Transformer), queues: make(map[string]int)}
  return a, a
}

func (self *uniAPI) SetApplication(app Application) {
  self.app = app
}

func (self *uniAPI) Open(perma_blobref string, startWithSeqNumber int) (err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  // Is already open?
  if _, ok := self.open[perma_blobref]; ok {
    return os.NewError("Cannot open twice")
  }
  // TODO: Check permissions
  self.open[perma_blobref] = startWithSeqNumber
  // Send all messages queued so far.
  t, ok := self.permas[perma_blobref]
  if ok {
    err = t.Repeat(perma_blobref, startWithSeqNumber) 
  }
  return
}

func (self *uniAPI) Close(perma_blobref string) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  self.open[perma_blobref] = 0, false
  return
}

func (self* uniAPI) Signal_ReceivedInvitation(t Transformer, permission *Permission) {
  self.app.Signal_ReceivedInvitation(permission)
}

func (self* uniAPI) Signal_AcceptedInvitation(t Transformer, keep *Keep) {
  self.app.Signal_AcceptedInvitation(keep)
}

func (self* uniAPI) Blob_Keep(t Transformer, keep *Keep, seqNumber int) {
  self.blob(t, keep.PermaBlobRef, keep, seqNumber)
}

func (self* uniAPI) Blob_Mutation(t Transformer, mutation *Mutation, seqNumber int) {
  self.blob(t, mutation.PermaBlobRef, mutation, seqNumber)
}

func (self* uniAPI) Blob_Permission(t Transformer, permission *Permission, seqNumber int) {
  self.blob(t, permission.PermaBlobRef, permission, seqNumber)
}

func (self* uniAPI) blob(t Transformer, permanode_blobref string, blob interface{}, seqNumber int) {
  self.mutex.Lock()
  // Remember that this perma blob exists
  _, ok := self.permas[permanode_blobref]
  if !ok {
    self.permas[permanode_blobref] = t
  }
  // Is this perma blob opened?
  nextSeqNumber, ok := self.open[permanode_blobref]
  if !ok {
    // Is this a new keep of the local user? If yes, send it. Otherwise ignore it
    if keep, ok := blob.(*Keep); ok && keep.KeepSigner == self.userID {
      self.mutex.Unlock()
      self.app.Signal_ProcessedKeep(keep)
      return
    }
    self.mutex.Unlock()
    return      
  }
  if nextSeqNumber > seqNumber {
    // Ignore this mutation. We have seen it already (should not happen anyway)
    self.mutex.Unlock()
    return
  }
  if nextSeqNumber < seqNumber {
    // Remember that we need to process these mutations later on, too, but not now
    q, ok := self.queues[permanode_blobref]
    if !ok || seqNumber < q {
      self.queues[permanode_blobref] = seqNumber
    }
    self.mutex.Unlock()
    return
  }
  // Store the next expected sequence number
  self.open[permanode_blobref] = seqNumber + 1
  // Is there a need to continue with queued mutations?
  cont := -1
  j, ok := self.queues[permanode_blobref]
  if ok {
    cont = j
    self.queues[permanode_blobref] = 0, false
  }
  self.mutex.Unlock()
  // Notify the application
  self.app.Blob(blob, seqNumber)
  // Ask to repeat further blobs in case we have already seen some
  if cont != -1 {
    t.Repeat(permanode_blobref, cont)
  }
}
