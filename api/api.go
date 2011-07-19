package lightwaveapi

import (
  grapher "lightwavegrapher"
  "os"
  "sync"
  "log"
)

type Application interface {
  Signal_ReceivedInvitation(permission *grapher.Permission)
  // This function is called right after the local user issued his keep but (most of the tim)
  // before all blobs have been downloaded
  Signal_AcceptedInvitation(keep *grapher.Keep)
  // This function is called when the keep issued by the local user has been processed.
  // If the perma blob belongs to someone else, then this means all blobs have been downloaded.
  // If the perma blob belongs to the local user, then this signal comes directly after Signal_AcceptedInvitation.
  Signal_ProcessedKeep(keep *grapher.Keep)
  Blob(blob interface{}, seqNumber int)
}

// The API layer as seen by the application.
// This API assumes that only one application (uni) is sitting on top of the layer stack.
type API interface {
  SetApplication(app Application)
  Open(perma_blobref string, startWithSeqNumber int) os.Error
  Close(perma_blobref string)
}

type uniAPI struct {
  userID string
  grapher *grapher.Grapher
  app Application
  // The value is the last mutation sent 
  open map[string]int
  queues map[string]int
  mutex sync.Mutex
}

func NewUniAPI(userid string, grapher *grapher.Grapher) API {
  a := &uniAPI{userID: userid, grapher: grapher, open: make(map[string]int), queues: make(map[string]int)}
  grapher.SetAPI(a)
  return a
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
  err = self.grapher.Repeat(perma_blobref, startWithSeqNumber) 
  return
}

func (self *uniAPI) Close(perma_blobref string) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  self.open[perma_blobref] = 0, false
  return
}

func (self* uniAPI) Signal_ReceivedInvitation(permission *grapher.Permission) {
  self.app.Signal_ReceivedInvitation(permission)
}

func (self* uniAPI) Signal_AcceptedInvitation(keep *grapher.Keep) {
  self.app.Signal_AcceptedInvitation(keep)
}

func (self* uniAPI) Blob_Keep(keep *grapher.Keep, seqNumber int) {
  log.Printf("Keep")
  self.blob(keep.PermaBlobRef, keep, seqNumber)
}

func (self* uniAPI) Blob_Mutation(mutation *grapher.Mutation, seqNumber int) {
  log.Printf("Mut")
  self.blob(mutation.PermaBlobRef, mutation, seqNumber)
}

func (self* uniAPI) Blob_Permission(permission *grapher.Permission, seqNumber int) {
  log.Printf("Perm")
  self.blob(permission.PermaBlobRef, permission, seqNumber)
}

func (self* uniAPI) blob(permanode_blobref string, blob interface{}, seqNumber int) {
  log.Printf("API blob %v", seqNumber)
  self.mutex.Lock()
  // Is this perma blob opened?
  nextSeqNumber, ok := self.open[permanode_blobref]
  if !ok {
    // Is this a new keep of the local user? If yes, send it. Otherwise ignore it
    if keep, ok := blob.(*grapher.Keep); ok && keep.KeepSigner == self.userID {
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
    self.grapher.Repeat(permanode_blobref, cont)
  }
}
