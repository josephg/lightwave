package lightwavegrapher

import (
  ot "lightwaveot"
  "json"
  "log"
  "os"
//  "time"
  "rand"
  "fmt"
)

// --------------------------------------------------
// Struct to deserialize any schema blob

type superSchema struct {
  // Allowed value are "permanode", "mutation", "permission", "keep"
  Type    string "type"
  Time    string "t"
  Signer string "signer"
  
  Permission string "permission"
  Action string "action"
//  Sig    string "sig"

  Dependencies []string "dep"
  
  Random string "random"
  PermaNode string "perma"
  
  User string "user"
  Allow int "allow"
  Deny int "deny"
  
  Operation *json.RawMessage "op"
}

// -----------------------------------------------------
// Permission bits

const (
  Perm_Read = 1 << iota
  Perm_Write
  Perm_Invite
  Perm_Expel
  // This is not really a permission. It just indicates that the permission owner
  // has a keep on the perma blob.
  Perm_Keep
)


// ------------------------------------------------------
// Interfaces
 
type Federation interface {
  SetGrapher(indexer *Grapher)
  Forward(blobref string, users []string)
  DownloadPermaNode(permission_blobref string) os.Error
}

// The transformer as seen by the Grapher
type Transformer interface {
  TransformMutation(mutation MutationNode, rollback <-chan OTNode, concurrent []string) os.Error
  TransformClientMutation(mutation_input MutationNode, rollback <-chan OTNode) os.Error
}

// The API layer as seen by the Grapher
type API interface {
  // This function is called when an invitation has been received.
  // The user can now download the document and issue a keep to follow it.
  Signal_ReceivedInvitation(perma PermaNode, permission PermissionNode)
  // This function is called when the local user has accepted an invitation by creating a keep blob
  Signal_AcceptedInvitation(perma PermaNode, perm PermissionNode, keep KeepNode)
  Blob_Keep(perma PermaNode, perm PermissionNode, keep KeepNode)
  // This function is called when a mutation has been applied.
  // The mutation passed in the parameter is already transformed.
  Blob_Mutation(perma PermaNode, mut MutationNode)
  // This function is called when a permission mutation has been applied.
  // The permission passed in the parameter is already transformed
  Blob_Permission(perma PermaNode, permission PermissionNode)
}

// The blob store as seen by the Grapher
type BlobStore interface {
  StoreBlob(blob []byte, blobref string) (finalBlobRef string, err os.Error)
  GetBlob(blobref string) (blob []byte, err os.Error)
}

type GraphStore interface {
  StoreNode(perma_blobref string, blobref string, data map[string]interface{}) os.Error
  StorePermaNode(perma_blobref string, data map[string]interface{}) os.Error
  GetPermaNode(blobref string) (data map[string]interface{}, err os.Error)
  HasOTNodes(perma_blobref string, blobrefs []string) (missing_blobrefs []string, err os.Error)
//  GetOTNodeBySeqNumber(perma_blobref string, seqNumber int) (data map[string]interface{}, err os.Error)
  GetOTNodeByBlobRef(perma_blobref string, blobref string) (data map[string]interface{}, err os.Error)
  GetOTNodesAscending(perma_blobref string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan map[string]interface{}, err os.Error)
  GetOTNodesDescending(perma_blobref string) (ch <-chan map[string]interface{}, err os.Error)
  Enqueue(perma_blobref string, blobref string, dependencies []string) os.Error
  Dequeue(perma_blobref string, blobref string) (blobrefs []string, err os.Error)
}

// ------------------------------------------------------
// Grapher

type Grapher struct {
  gstore GraphStore
  // Map of all processed nodes. The key is the blob ref
//  nodes map[string]interface{}
  // Map of all blobs that have been processed.
  // The keys are blobrefs. The value is true if the blob has been successfully processed and
  // false if the blob was rejected for some reason.
//  blobs map[string]bool
  // The blobrefs of blobs that cannot be processed because they depend on
  // another blob that has not yet been indexed.
//  waitingBlobs map[string]bool
  // The blobrefs of blobs which are missing. The value is a list of strings
  // which are the blobrefs of pending mutations.
//  waitingLists map[string]*lst.List
  // The blobrefs or blobs that are in the store but not yet indexed
  // because they depend on blobs which are not yet indexed.
  // The value is the number of unsatisfied dependencies.
//  pendingBlobs map[string]int
  // Keys are blobrefs of permaNodes to which the local user has been invited.
  // The keys are the blobrefs of the invitation.
  // The map holds invitations that have not yet been accepted by the local user.
//  openInvitations map[string]string
  store BlobStore
  fed Federation
  // 'user@domain' of the local user.
  userID string 
  transformer Transformer
  api API
}

// Creates a new indexer for the specified user based on the blob store.
// The indexer calls the federation object to send messages to other users.
// Federation may be nil as well.
func NewGrapher(userid string, store BlobStore, gstore GraphStore, fed Federation) *Grapher {
  idx := &Grapher{userID: userid, store: store, gstore: gstore, fed: fed}
  if fed != nil {
    fed.SetGrapher(idx)
  }
  return idx
}

func (self *Grapher) SetTransformer(transformer Transformer) {
  self.transformer = transformer
}

func (self *Grapher) SetAPI(api API) {
  self.api = api
}

func (self *Grapher) Frontier(blobref string) (frontier []string, err os.Error) {
  p, err := self.permaNode(blobref)
  if err != nil {
    return nil, err
  }
  return p.frontier.IDs(), nil
}

func (self *Grapher) Followers(blobref string) (users []string, err os.Error) {
  p, err := self.permaNode(blobref)
  if err != nil {
    return nil, err
  }
  return p.followers(), nil
}

func (self *Grapher) permaNode(blobref string) (perma *permaNode, err os.Error) {
  m, err := self.gstore.GetPermaNode(blobref)
  if err != nil || m == nil {
    return nil, err
  }
  if m["k"].(int64) != OTNode_Perma {
    return nil, os.NewError("Blob is not a perma blob")
  } 
  p := newPermaNode(self)
  p.FromMap(blobref, m)
  return p, nil  
}

func (self *Grapher) permission(perma_blobref string, blobref string) (permission *permissionNode, err os.Error) {
  m, err := self.gstore.GetOTNodeByBlobRef(perma_blobref, blobref)
  if err != nil || m == nil {
    return nil, err
  }
  if m["k"].(int64) != OTNode_Permission {
    return nil, os.NewError("Blob is not a permission blob")
  } 
  p := &permissionNode{}
  p.FromMap(perma_blobref, m)
  return p, nil  
}

func (self *Grapher) enqueue(perma_blobref, blobref string, deps []string) os.Error {
  return self.gstore.Enqueue(perma_blobref, blobref, deps)
}

func (self *Grapher) dequeue(perma_blobref, waitFor string) (blobrefs []string, err os.Error) {
  return self.gstore.Dequeue(perma_blobref, waitFor)
}

func (self *Grapher) decodeNode(schema *superSchema, blobref string) (result interface{}, err os.Error) {
  if schema.Signer == "" {
    return nil, os.NewError("Missing signer")
  }
  /*
  var tstruct *time.Time
  tstruct, err = time.Parse(time.RFC3339, schema.Time)
  if err != nil || tstruct == nil {
    return
  }
  t := tstruct.Seconds()
   */
  switch schema.Type {
  case "keep":
    // TODO time t
    n := &keepNode{keepBlobRef: blobref, keepSigner: schema.Signer, permaBlobRef: schema.PermaNode, dependencies: schema.Dependencies, permissionBlobRef: schema.Permission}
    return n, nil
  case "permanode":
    n := newPermaNode(self)
    n.blobref = blobref
    // TODO n.time = t
    n.signer = schema.Signer
    // n.parent = schema.PermaNode
    // The owner of the permanode has all the rights on it
    n.permissions = map[string]int{n.signer: ^0}
    return n, nil
  case "mutation":
    // TODO: time t
    if schema.Operation == nil {
      return nil, os.NewError("Mutation is lacking an operation")
    }
    n := &mutationNode{mutationSigner: schema.Signer, permaBlobRef: schema.PermaNode, mutationBlobRef: blobref, dependencies: schema.Dependencies, operation: []byte(*schema.Operation)}
    return n, nil
  case "permission":
    if schema.User == "" {
      err = os.NewError("permission is lacking a target user")
      return
    }
    // TODO: time t
    n := &permissionNode{permissionSigner: schema.Signer, permaBlobRef: schema.PermaNode, ot.Permission: ot.Permission{ID: blobref, Deps: schema.Dependencies}}
    n.User = schema.User
    n.Allow = schema.Allow
    n.Deny = schema.Deny
    switch schema.Action {
    case "invite":
      n.action = PermAction_Invite
    case "expel":
      n.action = PermAction_Expel
    case "change":
      n.action = PermAction_Change
    default:
      err = os.NewError("Unknown action type in permission blob")
      return
    }
    return n, nil    
  default:
    log.Printf("Err: Unknown schema type")
  }
  return nil, os.NewError("Unknown schema type")
}

// Invoked from the blob store
func (self *Grapher) HandleBlob(blob []byte, blobref string) {
  var signer string
  var perma *permaNode
  // First, determine the mimetype
  mimetype := MimeType(blob)
  if mimetype == "application/x-lightwave-schema" { // Is it a schema blob?
    var processed bool
    if perma, signer, processed = self.handleSchemaBlob(blob, blobref); !processed {
      return
    }
  } else {
    // TODO: Handle ordinary binary blobs
  }
    
  // Forward the blob to all followers
  if self.fed != nil && signer == self.userID {
    users := perma.followersWithPermission(Perm_Read)
    if len(users) > 0 {
      self.fed.Forward(blobref, users)
    }
  }

  // Remember that this blob has been processed
//  self.blobs[blobref] = true
    
  // Did other blobs wait on this one?
  deps, err := self.dequeue(perma.BlobRef(), blobref)
  if err != nil {
    return
  }
  for _, dep := range deps {
    b, err := self.store.GetBlob(dep)
    if err != nil {
      log.Printf("Err: Failed retrieving blob: %v\n", err)
      continue
    }
    self.HandleBlob(b, dep)
  }
}

func (self *Grapher) handleSchemaBlob(blob []byte, blobref string) (perma *permaNode, signer string, processed bool) {
  // Try to decode it into a camli-store schema blob
  var schema superSchema
  err := json.Unmarshal(blob, &schema)
  if err != nil {
    log.Printf("Err: Malformed schema blob: %v\n", err)
    return nil, "", false
  }

  newnode, err := self.decodeNode(&schema, blobref)
  if err != nil {
    log.Printf("Err: Schema blob is not valid: %v\n", err)
    return nil, "", false
  }
  ptr := newnode.(AbstractNode)
  signer = ptr.Signer()
  // The node is linked to another permaNode?
  if ptr.PermaBlobRef() != "" {
    perma, err = self.permaNode(ptr.PermaBlobRef())
    if err != nil {
      log.Printf("Err: The specified node is not a perma node")
      return nil, "", false
    }
    if perma == nil {
      self.enqueue(ptr.PermaBlobRef(), blobref, []string{ptr.PermaBlobRef()})
      return nil, "", false
    }
  }
  switch newnode.(type) {
  case *permaNode:
    perma = newnode.(*permaNode)
    self.gstore.StorePermaNode(perma.BlobRef(), perma.ToMap())
    processed = true
    return
  case OTNode:
    if perma == nil {
      log.Printf("Err: Permission or mutation without a permanode")
      return nil, "", false
    }
    // Is this an invitation? Then we cannot apply it, because most data is missing.
    if inv, ok := newnode.(*permissionNode); ok && inv.action == PermAction_Invite && inv.User == self.userID && !self.hasBlobs(perma.BlobRef(), inv.Dependencies()) {
      processed = self.handleInvitation(perma, inv)
      // Do not apply the blob here. We must first download all the data
      self.enqueue(perma.BlobRef(), blobref, inv.Dependencies())
      return
    } else if keep, ok := newnode.(*keepNode); ok {
      processed = self.checkKeep(perma, keep)
      if !processed {
	return
      }
    }
    deps, err := perma.apply(newnode.(OTNode), self.transformer)
    if err != nil {
      log.Printf("Err: applying blob failed: %v\nblobref=%v\n", err, blobref)
      return nil, "", false
    }
    // The blob could not be applied because of unresolved dependencies?
    if len(deps) > 0 {
      self.enqueue(perma.BlobRef(), blobref, deps)
      return nil, "", false
    }
    
    processed = true
    if _, ok := newnode.(*permissionNode); ok {
      processed = self.handlePermission(perma, newnode.(*permissionNode))
    } else if _, ok := newnode.(*keepNode); ok {
      processed = self.handleKeep(perma, newnode.(*keepNode))
    } else if _, ok := newnode.(*mutationNode); ok {
      processed = self.handleMutation(perma, newnode.(*mutationNode))
    }
    
    if processed {
      self.gstore.StoreNode(perma.BlobRef(), newnode.(OTNode).BlobRef(), newnode.(OTNode).ToMap())
      self.gstore.StorePermaNode(perma.BlobRef(), perma.ToMap())
      log.Printf("Grapher processed blob %v at %v\n", ptr.BlobRef(), self.userID)
    }
    return
  }

  log.Printf("Err: Unknown blob type\n")
  return nil, "", false
}

func (self *Grapher) handleInvitation(perma *permaNode, perm *permissionNode) bool {
//  self.openInvitations[perma.BlobRef()] = perm.BlobRef()
  // Signal to the next layer that an invitation has been received
  if self.api != nil {
    self.api.Signal_ReceivedInvitation(perma, perm)
  }
  return true
}

func (self *Grapher) handleMutation(perma *permaNode, mut *mutationNode) bool {
  if self.api != nil {
    self.api.Blob_Mutation(perma, mut)
  }
  return true
}

func (self *Grapher) handlePermission(perma *permaNode, perm *permissionNode) bool {
  switch perm.action {
  case PermAction_Change:
    // TODO
  case PermAction_Expel:
    // TODO
  case PermAction_Invite:
    // Add the invitation to remember that this user has been invited.
//    perma.pendingInvitations[perm.User] = perm.BlobRef()
    // Forward the invitation to the user being invited
    if self.fed != nil && perm.Signer() == self.userID {
      self.fed.Forward(perm.BlobRef(), []string{perm.User})
      // Forward the permanode to the invited user as well
      self.fed.Forward(perma.BlobRef(), []string{perm.User})
    }
  default:
    panic("Unknown action type")
  }
  if self.api != nil {
    self.api.Blob_Permission(perma, perm)
  }  
  return true
}

func (self *Grapher) checkKeep(perma *permaNode, keep *keepNode) bool {
  log.Printf("Check keep for %v", keep.Signer())
  // The signer of the keep is not the signer of the permanode?
  // In this case he must present a valid invitation
  if keep.Signer() == perma.Signer() {
    return true
  }
  
  if keep.permissionBlobRef == "" {
    log.Printf("Err: Keep on a foreign permanode is missing a reference to a permission blob")
    return false
  }
    
  var err os.Error
  perm, err := self.permission(perma.BlobRef(), keep.permissionBlobRef)
  // Not an invitation?
  if err != nil {
    log.Printf("Err: Keep references a permision that is something else or malformed")
    return false
  }
  // Permission has not yet been received or processed? -> enqueue
  if perm == nil {
    log.Printf("Permission is not yet applied for the keep")
    self.enqueue(perma.BlobRef(), keep.BlobRef(), []string{keep.permissionBlobRef})
    // The local user accepted the invitation?
    if keep.Signer() == self.userID {
      if self.fed != nil {
	go self.fed.DownloadPermaNode(keep.permissionBlobRef)
      }
    }
    return false
  }
  // TODO: Is the permission still valid or has it been overruled?
  
  // The invitation has indeed been issued for the user who issued the keep? If not -> error
  if perm.User != keep.Signer() {
    log.Printf("Err: Keep references an invitation targeted at a different user")
    return false
  }
  
  // The local user accepted the invitation?
  if keep.Signer() == self.userID {
    if self.api != nil {
      self.api.Signal_AcceptedInvitation(perma, perm, keep)
    }
  }
  return true
}

func (self *Grapher) handleKeep(perma *permaNode, keep *keepNode) bool {
  var perm *permissionNode
  // The signer of the keep is not the signer of the permanode?
  // In this case he must present a valid invitation
  if keep.Signer() != perma.Signer() {
    var err os.Error
    perm, err = self.permission(perma.BlobRef(), keep.permissionBlobRef)
    if err != nil || perm == nil {  // Problem already catched at checkKeep 
      panic("Keep references a permision that is something else or malformed")
    }
  }

  // Does this implicitly accept a pending invitation? Clean it up.
//  if _, ok := perma.pendingInvitations[keep.Signer()]; ok {
//    perma.pendingInvitations[keep.Signer()] = "", false
//  }
  // This keep is new. The permaNode has a new user.
  perma.addKeep(keep.Signer())
  log.Printf("Processing keep of %v\n", keep.Signer())
  // Signal the keep to the application
  if self.api != nil {
    self.api.Blob_Keep(perma, perm, keep)
  }
  
  // This implies that the local user is accepting an invitation?
  if perm != nil && perm.User == self.userID {
    // Send the keep (which accepts the invitation) to the signer of the invitation
    if self.fed != nil && keep.Signer() != self.userID {
      self.fed.Forward(keep.BlobRef(), []string{keep.Signer()})
    }
//    self.openInvitations[perma.BlobRef()] = "", false
    log.Printf("The local user accepted the invitation\nREF=%v\n", keep.BlobRef())
  } else if perm != nil {
    // Some other user is accepting his invitation?
    log.Printf("The user %v accepted the invitation\n", keep.Signer())
    // Send this user all blobs of the local user that are not in the other user's frontier yet.
    if self.fed != nil {
      h := ot.NewHistoryGraph(perma.frontier, keep.Dependencies())
      h.SubstituteBlob(keep.BlobRef(), keep.Dependencies())
      forwards := []string{}
      if !h.Test() {
	ch, _ := self.getOTNodesDescending(perma.BlobRef())
	for history_node := range ch {
	  if !h.SubstituteBlob(history_node.BlobRef(), history_node.Dependencies()) {
	    // Send nodes created by the local user
	    if history_node.Signer() == self.userID {
	      forwards = append(forwards, history_node.BlobRef())
              // Send keeps that rely on a permission given by the local user
	    } else if k, ok := history_node.(*keepNode); ok && k.permissionBlobRef != "" {
	      if p, e := self.permission(perma.BlobRef(), k.permissionBlobRef); e == nil && p != nil && p.Signer() == self.userID {
		forwards = append(forwards, history_node.BlobRef())		  
	      }
	    }
	  }
	  if h.Test() {
	    break
	  }
	}
      }
      for _, f := range forwards {
	self.fed.Forward(f, []string{keep.Signer()})
      }
    }
  }    
  return true
}

func (self *Grapher) hasBlobs(perma_blobref string, blobrefs []string) bool {
  m, _ := self.gstore.HasOTNodes(perma_blobref, blobrefs)
  return len(m) == 0
}

func (self *Grapher) getOTNodesAscending(perma_blobref string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan OTNode, err os.Error) {
  ch2, err := self.gstore.GetOTNodesAscending(perma_blobref, startWithSeqNumber, endSeqNumber)
  if err != nil {
    return nil, err
  }
  
  c := make(chan OTNode)
  f := func() {
    for data := range ch2 {
      c <- self.otNodeFromMap(perma_blobref, data)
    }
    close(c)
  }
  
  go f()
  return c, nil
}

func (self *Grapher) getOTNodesDescending(perma_blobref string) (ch <-chan OTNode, err os.Error) {
  ch2, err := self.gstore.GetOTNodesDescending(perma_blobref)
  if err != nil {
    return nil, err
  }
  
  c := make(chan OTNode)
  f := func() {
    for data := range ch2 {
      c <- self.otNodeFromMap(perma_blobref, data)
    }
    close(c)
  }
  
  go f()
  return c, nil
}

func (self *Grapher) otNodeFromMap(perma_blobref string, data map[string]interface{}) OTNode {
  switch data["k"].(int64) {
  case OTNode_Permission:
    p := &permissionNode{}
    p.FromMap(perma_blobref, data)
    return p
  case OTNode_Mutation:
    m := &mutationNode{}
    m.FromMap(perma_blobref, data)
    return m
  case OTNode_Keep:
    k := &keepNode{}
    k.FromMap(perma_blobref, data)
    return k
  default:
    panic("Malformed data")
  }
  return nil
}

// Interface towards the API
func (self *Grapher) Repeat(perma_blobref string, startWithSeqNumber int64) (err os.Error) {
  perma, err := self.permaNode(perma_blobref)
  if err != nil {
    return err
  }  
  ch, err := self.getOTNodesAscending(perma_blobref, startWithSeqNumber, -1)
  if err != nil {
    return err
  }
  
  f := func() {
    i := int64(0)
    for n := range ch {
      switch n.(type) {
      case *mutationNode:
	mut := n.(*mutationNode)
	self.api.Blob_Mutation(perma, mut)
      case *keepNode:
	keep := n.(*keepNode)
	var perm *permissionNode
	if keep.permissionBlobRef != "" {
	  perm, _ = self.permission(perma.BlobRef(), keep.permissionBlobRef)
	}
	self.api.Blob_Keep(perma, perm, keep)
      case *permissionNode:
	perm := n.(*permissionNode)
	self.api.Blob_Permission(perma, perm)
      default:
	panic("Unknown blob type")
      }
      i++
    }
  }
  go f()
  return
}

func (self *Grapher) CreatePermaBlob() (blobref string, err os.Error) {
  permaJson := map[string]interface{}{ "signer": self.userID, "random":fmt.Sprintf("%v", rand.Int63()), "t":"2006-01-02T15:04:05+07:00"}
  // TODO: Get time correctly
  permaBlob, err := json.Marshal(permaJson)
  if err != nil {
    panic(err.String())
  }
  permaBlob = append([]byte(`{"type":"permanode",`), permaBlob[1:]...)
  log.Printf("Storing perma %v\n", string(permaBlob))
  permaBlobRef, err := self.store.StoreBlob(permaBlob, "")
  return permaBlobRef, err
}

// The parameter 'permission_blobref' may be empty if the keep is from the same user that created the permaNode
func (self *Grapher) CreateKeepBlob(perma_blobref, permission_blobref string) (blobref string, err os.Error) {
  // Create a keep on the permaNode.
  keepJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref, "t":"2006-01-02T15:04:05+07:00"}
  if permission_blobref != "" {
    keepJson["dep"] = []string{permission_blobref}
    keepJson["permission"] = permission_blobref
  }
  // TODO: Get time correctly
  keepBlob, err := json.Marshal(keepJson)
  if err != nil {
    panic(err.String())
  }
  keepBlob = append([]byte(`{"type":"keep",`), keepBlob[1:]...)
  log.Printf("Storing keep %v\n", string(keepBlob))
  keepBlobRef, err := self.store.StoreBlob(keepBlob, "")
  return keepBlobRef, err
}

func (self *Grapher) CreatePermissionBlob(perma_blobref string, applyAtSeqNumber int64, userid string, allow int, deny int, action int) (blobref string, err os.Error) {
  perma, e := self.permaNode(perma_blobref)
  if e != nil {
    err = e
    return
  }  
  permNode := &permissionNode{permissionSigner:self.userID, permaBlobRef: perma_blobref}
  permNode.ID = fmt.Sprintf("%v%v", self.userID, applyAtSeqNumber + 1) // This is not a hash ID. This ID is only temporary
  permNode.User = userid
  permNode.Allow = allow
  permNode.Deny = deny
  permNode.action = action
  permNode, err = perma.transformLocalPermission(permNode, applyAtSeqNumber)
  if err != nil {
    return
  }
  frontier, e := self.Frontier(perma_blobref)
  if e != nil {
    err = e
    return
  }  
  permJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref, "dep": frontier, "t":"2006-01-02T15:04:05+07:00", "user": userid, "allow":allow, "deny": deny}
  // TODO: Get time correctly
  switch action {
  case PermAction_Invite:
    permJson["action"] = "invite"
  case PermAction_Expel:
    permJson["action"] = "expel"
  case PermAction_Change:
    permJson["action"] = "change"
  default:
    panic("Unknown action")
  }
  permBlob, err := json.Marshal(permJson)
  if err != nil {
    panic(err.String())
  }
  permBlob = append([]byte(`{"type":"permission",`), permBlob[1:]...)
  log.Printf("Storing perm %v\n", string(permBlob))
  permBlobRef, err := self.store.StoreBlob(permBlob, "")
  return permBlobRef, err
}

func (self *Grapher) CreateMutationBlob(perma_blobref string, operation interface{}, applyAtSeqNumber int64) (blobref string, err os.Error) {
  perma, e := self.permaNode(perma_blobref)
  if e != nil {
    err = e
    return
  }  
  // Update the operation such that it can be applied after all currently applied operations
  m := &mutationNode{}
  m.permaBlobRef = perma_blobref
  m.mutationBlobRef = fmt.Sprintf("%v%v", self.userID, applyAtSeqNumber + 1) // This is not a hash ID. This ID is only temporary
  m.mutationSigner = self.userID
  m.operation = operation
  ch, e := self.getOTNodesAscending(perma.BlobRef(), applyAtSeqNumber, perma.sequenceNumber())
  if e != nil {
    return
  }  
  e = self.transformer.TransformClientMutation(m, ch)
  if e != nil {
    return
  }
  frontier, e := self.Frontier(perma_blobref)
  if e != nil {
    err = e
    return
  }  
  mutJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref, "dep": frontier, "t":"2006-01-02T15:04:05+07:00"}
  // TODO: Get time correctly
  op := m.operation.(ot.Operation) // The following two lines work around a problem in GO/JSON
  mutJson["op"] = &op
  schema, err := json.Marshal(mutJson)
  if err != nil {
    panic(err.String())
  }
  mutBlob := []byte(`{"type":"mutation",`)
  mutBlob = append(mutBlob, schema[1:]...)
  log.Printf("Storing mut %v\n", string(mutBlob))
  mutBlobRef, err := self.store.StoreBlob(mutBlob, "")
  return mutBlobRef, err
}

