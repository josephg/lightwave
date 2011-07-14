package lightwavetransformer

import (
  ot "lightwaveot"
  "os"
  "sync"
)

// The API layer as seen by the Transformer
type API interface {
  // This function is called when an invitation has been received
  Invitation(t Transformer, permanode_blobref, invitation_blobref string, userid string)
  // This function is called when the local user has accepted an invitation
  AcceptedInvitation(t Transformer, permanode_blobref, invitation_blobref string, keep_blobref string)
  // This function is called when a new user has been added to a perma node.
  NewFollower(t Transformer, permanode_blobref string, invitation_blobref, keep_blobref, userid string)
  // This function is called when a perma node has been added
  PermaNode(t Transformer, permanode_blobref string, invitation_blobref, keep_blobref string)
  // This function is called when a mutation has been applied.
  // The mutation passed in the parameter is already transformed.
  Mutation(t Transformer, permanode_blobref string, mutation interface{}, seqNumber int)
  // This function is called when a permission mutation has been applied.
  // The permission passed in the parameter is already transformed
  Permission(t Transformer, permanode_blobref string, action int, permission ot.Permission)
}

// The API layer as seen by the application.
// This API assumes that only one application (uni) is sitting on top of the layer stack.
type UniAPI interface {
  SetApplication(app Application)
  Open(perma_blobref string, startWithSeqNumber int) os.Error
  Close(perma_blobref string)
  Frontier(perma_blobref string) (blobrefs []string, err os.Error)
}

// The application as seen from the API
type Application interface {
  // This function is called when an invitation has been received
  Invitation(permanode_blobref, invitation_blobref string)
  // This function is called when the local user has accepted an invitation
  AcceptedInvitation(permanode_blobref, invitation_blobref string, keep_blobref string)
  // This function is called when a new user has been added to a perma node.
  NewFollower(permanode_blobref string, invitation_blobref, keep_blobref, userid string)
  // This function is called when a perma node has been added
  PermaNode(permanode_blobref string, invitation_blobref, keep_blobref string)
  // This function is called when a mutation has been applied.
  // The mutation passed in the parameter is already transformed.
  Mutation(permanode_blobref string, mutation interface{})
  // This function is called when a permission mutation has been applied.
  // The permission passed in the parameter is already transformed
  Permission(permanode_blobref string, action int, permission ot.Permission)
}

type uniAPI struct {
  app Application
  // The value is the last mutation sent 
  open map[string]int
  queues map[string]int
  permas map[string]Transformer
  mutex sync.Mutex
}

func NewUniAPI() (appInterface UniAPI, transformerInterface API) {
  a := &uniAPI{open: make(map[string]int), permas: make(map[string]Transformer), queues: make(map[string]int)}
  return a, a
}

func (self *uniAPI) SetApplication(app Application) {
  self.app = app
}

func (self *uniAPI) Frontier(perma_blobref string) (blobrefs []string, err os.Error) {
  t, ok := self.permas[perma_blobref]
  if !ok {
    return nil, os.NewError("Unknown perma")
  }
  return t.Frontier(perma_blobref)
}

func (self *uniAPI) Open(perma_blobref string, startWithSeqNumber int) (err os.Error) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  // TODO: Check permissions
  self.open[perma_blobref] = startWithSeqNumber
  // Send all messages queued so far.
  t, ok := self.permas[perma_blobref]
  if ok {
    err = t.RepeatMutations(perma_blobref, startWithSeqNumber) 
  }
  return
}

func (self *uniAPI) Close(perma_blobref string) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  self.open[perma_blobref] = 0, false
  return
}

func (self* uniAPI) Invitation(t Transformer, permanode_blobref, invitation_blobref string, userid string) {
  self.app.Invitation(permanode_blobref, invitation_blobref)
}

func (self* uniAPI) AcceptedInvitation(t Transformer, permanode_blobref, invitation_blobref string, keep_blobref string) {
  self.app.AcceptedInvitation(permanode_blobref, invitation_blobref, keep_blobref)
}

func (self* uniAPI) NewFollower(t Transformer, permanode_blobref string, invitation_blobref, keep_blobref, userid string) {
  self.app.NewFollower(permanode_blobref, invitation_blobref, keep_blobref, userid)
}

func (self* uniAPI) PermaNode(t Transformer, permanode_blobref string, invitation_blobref, keep_blobref string) {
  self.mutex.Lock()
  self.permas[permanode_blobref] = t
  self.mutex.Unlock()
  self.app.PermaNode(permanode_blobref, invitation_blobref, keep_blobref)
}

func (self* uniAPI) Mutation(t Transformer, permanode_blobref string, mutation interface{}, seqNumber int) {
  self.mutex.Lock()
  // Is this perma blob opened?
  nextSeqNumber, ok := self.open[permanode_blobref]
  if !ok {
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
  self.open[permanode_blobref] = seqNumber + 1
  // Is there a need to continue with queued mutations?
  cont := -1
  j, ok := self.queues[permanode_blobref]
  if ok {
    cont = j
    self.queues[permanode_blobref] = 0, false
  }
  self.mutex.Unlock()
  self.app.Mutation(permanode_blobref, mutation)
  if cont != -1 {
    t.RepeatMutations(permanode_blobref, cont)
  }
}

func (self* uniAPI) Permission(t Transformer, permanode_blobref string, action int, permission ot.Permission) {
  self.mutex.Lock()
  if _, ok := self.open[permanode_blobref]; ok {
    self.mutex.Unlock()
    self.app.Permission(permanode_blobref, action, permission)
  } else {
    self.mutex.Unlock()
  }
}