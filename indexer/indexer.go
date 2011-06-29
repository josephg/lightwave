package lightwaveidx

import (
  ot "lightwaveot"
  . "lightwavestore"
  "json"
  "log"
  "os"
  lst "container/list"
)

// --------------------------------------------------
// Struct to deserialize any schema blob

type superSchema struct {
  // Allowed value are "permanode", "mutation", "permission", "keep"
  Type    string "type"

  Signer string "signer"
  
  Permission string "permission"
  Action string "action"
//  Sig    string "sig"

  Dependencies []string "dep"
  AppliedAt int "at"
  Site string "site"
  
  Random string "random"
  PermaNode string "perma"
  
  User string "user"
  Allow int "allow"
  Deny int "deny"
  
  Operation *ot.Operation "op"
}

// -----------------------------------------------------
// Permission bits

const (
  Perm_Read = 1 << iota
  Perm_Write
  Perm_Invite
  Perm_Expel
)

// -----------------------------------------------------
// The tree structure that the indexer is building up

// Abstract base class for nodes
type node struct {
  // BlobRef of the parent
  parent string
  signer string
}

// The blobref of the parent node or the empty string
func (self *node) Parent() string {
  return self.parent
}

// The userid of the signer
func (self *node) Signer() string {
  return self.signer
}

// All nodes must implement this interface
type abstractNode interface {
  BlobRef() string
  Parent() string
  Signer() string
}

type PermaNode struct {
  node
  // The blobref of this node
  blobref string
  // Optional. Contains OT mutations that constitute the content of the document
  ot *otHistory
  // The keys are userids. The values are blobrefs of the keep-blob.
  // This map contains keeps of foreign users.
  // The keep of the local user is not stored here.
  keeps map[string]string
  // The keys are userids. The values are blobrefs of the keep-blob.
  pendingInvitations map[string]string
}

func (self *PermaNode) OT() OTHistory {
  return self.ot
}

func (self *PermaNode) BlobRef() string {
  return self.blobref
}

func (self *PermaNode) FollowersWithPermission(bits int) (users []string) {
  for userid, _ := range self.keeps {
    if self.ot != nil && bits != 0 { // Need to check for special permission bits?
      if self.signer != userid { // The user is not the owner. Then he needs special permissions
	allowed, ok := self.ot.permissions[userid]
	if !ok {
	  continue
	}
	if allowed & bits != bits {
	  continue
	}
      }
    }
    users = append(users, userid)
  }
  return
}

func (self *PermaNode) Followers() (users []string) {
  for userid, _ := range self.keeps {
    users = append(users, userid)
  }
  return
}

func (self *PermaNode) HasKeep(userid string) bool {
  _, ok := self.keeps[userid]
  return ok
}

// All nodes participating in Operational Transformation must implement this interface
type otNode interface {
  abstractNode
  Dependencies() []string
}

const (
  PermAction_Invite = iota
  PermAction_Expel
  PermAction_Change
)

type permissionNode struct {
  node
  permission ot.Permission
  action int
}

func (self *permissionNode) BlobRef() string {
  return self.permission.ID
}

func (self *permissionNode) Dependencies() []string {
  return self.permission.Dependencies
}

type mutationNode struct {
  node
  mutation ot.Mutation
}

func (self *mutationNode) BlobRef() string {
  return self.mutation.ID
}

func (self *mutationNode) Dependencies() []string {
  return self.mutation.Dependencies
}

type keepNode struct {
  node
  blobref string
  dependencies []string
  permission string
}

func (self *keepNode) BlobRef() string {
  return self.blobref
}

func (self *keepNode) Dependencies() []string {
  return self.dependencies
}

// ------------------------------------------------------
// Interfaces
 
type Federation interface {
  SetIndexer(indexer *Indexer)
  Forward(blobref string, users []string)
}

type ApplicationIndexer interface {
  Invitation(invitation_blobref string)
}

// ------------------------------------------------------
// Indexer

type Indexer struct {
  // Map of all processed nodes. The key is the blob ref
  nodes map[string]interface{}
  // Map of all blobs that have been processed.
  // The keys are blobrefs. The value is true if the blob has been successfully processed and
  // false if the blob was rejected for some reason.
  blobs map[string]bool
  // The blobrefs of blobs that cannot be processed because they depend on
  // another blob that has not yet been indexed.
  waitingBlobs map[string]bool
  // The blobrefs of blobs which are missing. The value is a list of strings
  // which are the blobrefs of pending mutations.
  waitingLists map[string]*lst.List
  // The blobrefs or blobs that are in the store but not yet indexed
  // because they depend on blobs which are not yet indexed.
  // The value is the number of unsatisfied dependencies.
  pendingBlobs map[string]int
  // Keys are blobrefs of permaNodes to which the local user has been invited.
  // The keys are the blobrefs of the invitation.
  // The map holds invitations that have not yet been accepted by the local user.
  openInvitations map[string]string
  // The keys are blobrefs to permaNodes that are kept by command of a keep.
  // The values are the blobrefs of the respective keep blocks.
  // This list contains only keeps of the local user
//  keeps map[string]string
  store BlobStore
  fed Federation
  // 'user@domain' of the local user.
  userID string 
  appIndexers []ApplicationIndexer
}

func NewIndexer(userid string, store BlobStore, fed Federation) *Indexer {
  idx := &Indexer{userID: userid, store: store, nodes: make(map[string]interface{}), waitingBlobs: make(map[string]bool), waitingLists: make(map[string]*lst.List), pendingBlobs: make(map[string]int) /* keeps: make(map[string]string) */, openInvitations: make(map[string]string), blobs:make(map[string]bool), fed: fed}
  store.AddListener(idx)
  fed.SetIndexer(idx)
  return idx
}

func (self *Indexer) AddListener(appIndexer ApplicationIndexer) {
  self.appIndexers = append(self.appIndexers, appIndexer)
}

func (self *Indexer) PermaNode(blobref string) (perma *PermaNode, err os.Error) {
  n, ok := self.nodes[blobref]
  if !ok {
    return nil, nil
  }
  perma, ok = n.(*PermaNode)
  if !ok {
    err = os.NewError("Blob is not a permanode")
  }
  return
}

func (self *Indexer) Permission(blobref string) (permission *permissionNode, err os.Error) {
  n, ok := self.nodes[blobref]
  if !ok {
    return nil, nil
  }
  permission, ok = n.(*permissionNode)
  if !ok {
    err = os.NewError("Blob is not a permissionNode")
  }
  return
}

func (self *Indexer) enqueue(blobref string, deps []string) {
  // Remember the blob
  self.waitingBlobs[blobref] = true
  // For which other blob is 'blobref' waiting?
  for _, dep := range deps {
    // Remember that someone is waiting on 'dep'
    l, ok := self.waitingLists[dep]
    if !ok {
      l = lst.New()
      self.waitingLists[dep] = l
    }
    l.PushBack(blobref)
  }
  self.pendingBlobs[blobref] = len(deps)
}

func (self *Indexer) dequeue(waitFor string) (blobrefs []string) {
  // Is any other mutation waiting for 'waitFor'?
  if l, ok := self.waitingLists[waitFor]; ok {
    self.waitingLists[waitFor] = nil, false
    for l.Len() > 0 {
      waiting_id := l.Remove(l.Front()).(string)
      self.pendingBlobs[waiting_id]--
      // The waiting mutation is no waiting for anything anymore -> return it
      if self.pendingBlobs[waiting_id] == 0 {
	self.pendingBlobs[waiting_id] = 0, false
	blobrefs = append(blobrefs, waiting_id)
	self.waitingBlobs[waiting_id] = false, false
      }
    }
  }
  return
}

func (self *Indexer) decodeNode(schema *superSchema, blobref string) (result interface{}, err os.Error) {
  if schema.Signer == "" {
    return nil, os.NewError("Missing signer")
  }
  switch schema.Type {
  case "keep":
    n := &keepNode{blobref: blobref, node: node{signer: schema.Signer, parent: schema.PermaNode}, dependencies: schema.Dependencies, permission: schema.Permission}
    return n, nil
  case "permanode":
    n := &PermaNode{blobref: blobref, node: node{signer: schema.Signer, parent: schema.PermaNode}, keeps: make(map[string]string), pendingInvitations: make(map[string]string)}
    return n, nil
  case "mutation":
    if schema.Operation == nil {
      err = os.NewError("mutation is lacking an operation")
      return
    }
    if schema.Site == "" {
      err = os.NewError("mutation is lacking a site identifier")
    }
    n := &mutationNode{node: node{signer: schema.Signer, parent: schema.PermaNode}}
    n.mutation.Operation = *schema.Operation
    n.mutation.ID = blobref
    n.mutation.Site = schema.Site
    n.mutation.Dependencies = schema.Dependencies
    return n, nil
  case "permission":
    if schema.User == "" {
      err = os.NewError("permission is lacking a target user")
      return
    }
    n := &permissionNode{node: node{signer: schema.Signer, parent: schema.PermaNode}}
    n.permission.ID = blobref
    n.permission.Dependencies = schema.Dependencies
    n.permission.User = schema.User
    n.permission.Allow = schema.Allow
    n.permission.Deny = schema.Deny
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
    log.Printf("Unknown schema type")
  }
  return nil, os.NewError("Unknown schema type")
}

func (self *Indexer) HandleBlob(blob []byte, blobref string) {
  var signer string
  var perma *PermaNode
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
    users := perma.FollowersWithPermission(Perm_Read)
    if len(users) > 0 {
      self.fed.Forward(blobref, users)
    }
  }

  // Remember that this blob has been processed
  self.blobs[blobref] = true
    
  // Did other blobs wait on this one?
  for _, dep := range self.dequeue(blobref) {
    b, err := self.store.GetBlob(dep)
    if err != nil {
      log.Printf("Failed retrieving blob: %v\n", err)
      continue
    }
    self.HandleBlob(b, dep)
  }
}

func (self *Indexer) handleSchemaBlob(blob []byte, blobref string) (perma *PermaNode, signer string, processed bool) {
  // Try to decode it into a camli-store schema blob
  var schema superSchema
  err := json.Unmarshal(blob, &schema)
  if err != nil {
    log.Printf("Malformed schema blob: %v\n", err)
    return nil, "", false
  }

  newnode, err := self.decodeNode(&schema, blobref)
  if err != nil {
    log.Printf("Schema blob is not valid: %v\n", err)
    return nil, "", false
  }
  ptr := newnode.(abstractNode)
  signer = ptr.Signer()
  // The node is linked to another permaNode?
  if ptr.Parent() != "" {
    p, ok := self.nodes[ptr.Parent()]
    if !ok { // The other permaNode is not yet applied? -> enqueue
      self.enqueue(blobref, []string{ptr.Parent()})
      return nil, "", false
    }
    if perma, ok = p.(*PermaNode); !ok {
      log.Printf("The specified node is not a perma node")
      return nil, "", false
    }
  }
  switch newnode.(type) {
  case *PermaNode:
    perma = newnode.(*PermaNode)
    self.nodes[blobref] = newnode
    log.Printf("Added a permanode successfully")
    processed = true
    return
  case otNode:
    if perma == nil {
      log.Printf("Permission or mutation without a permanode")
      return nil, "", false
    }
    if perma.ot == nil {
      perma.ot = newOTHistory()
      // The owner of the permanode has all the rights on it
      perma.ot.permissions[perma.signer] = ^0
    }
    // Is this an invitation? Then we cannot apply it, because most data is missing.
    if inv, ok := newnode.(*permissionNode); ok && inv.action == PermAction_Invite && inv.permission.User == self.userID && !self.hasBlobs(inv.Dependencies()) {
      processed = self.handleInvitation(perma, inv)
      // Do not apply the blob here. We must first download all the data
      self.enqueue(blobref, inv.Dependencies())
      return
    } else if keep, ok := newnode.(*keepNode); ok {
      processed = self.checkKeep(perma, keep)
      if !processed {
	log.Printf("Keep block failed at inspection\n")
	return
      }
    }
    deps, err := perma.ot.Apply(newnode.(otNode))
    if err != nil {
      log.Printf("Err: applying blob failed: %v\nblobref=%v\n", err, blobref)
      return nil, "", false
    }
    if len(deps) > 0 {
      self.enqueue(blobref, deps)
      return nil, "", false
    }
    self.nodes[blobref] = newnode
    log.Printf("Applied blob %v at %v\n", ptr.BlobRef(), self.userID)

    processed = true
    if _, ok := newnode.(*permissionNode); ok {
      processed = self.handlePermission(perma, newnode.(*permissionNode))
    } else if _, ok := newnode.(*keepNode); ok {
      processed = self.handleKeep(perma, newnode.(*keepNode))
    }
    return
  }

  log.Printf("Err: Unknown blob type\n")
  return nil, "", false
}

func (self *Indexer) handleInvitation(perma *PermaNode, perm *permissionNode) bool {
  log.Printf("Handling invitation at %v\n", self.userID)
  self.openInvitations[perma.BlobRef()] = perm.BlobRef()
  // Signal to the next layer that an invitation has been received
  for _, app := range self.appIndexers {
    app.Invitation(perm.BlobRef())
  }
  return true
}

func (self *Indexer) handlePermission(perma *PermaNode, perm *permissionNode) bool {
  switch perm.action {
  case PermAction_Change:
    // TODO
  case PermAction_Expel:
    // TODO
  case PermAction_Invite:
    // Add the invitation to remember that this user has been invited.
    perma.pendingInvitations[perm.permission.User] = perm.BlobRef()
    log.Printf("User %v has been invited\n", perm.permission.User)
    // Forward the invitation to the user being invited
    if self.fed != nil && perm.Signer() == self.userID {
      self.fed.Forward(perm.BlobRef(), []string{perm.permission.User})
      // Forward the permanode to the invited user as well
      self.fed.Forward(perma.BlobRef(), []string{perm.permission.User})
    }
  default:
    panic("Unknown action type")
  }
  return true
}

func (self *Indexer) checkKeep(perma *PermaNode, keep *keepNode) bool {
  var perm *permissionNode
  // The signer of the keep is not the signer of the permanode?
  // In this case he must present a valid invitation
  if keep.Signer() != perma.Signer() {
    if keep.permission == "" {
      log.Printf("Err: Keep on a foreign permanode is missing a reference to a permission blob")
      return false
    }
    var err os.Error
    perm, err = self.Permission(keep.permission)
    // Not an invitation?
    if err != nil {
      log.Printf("Err: Keep references a permision that is something else or malformed")
      return false
    }
    // Permission has not yet been received or processed? -> enqueue
    if perm == nil {
      self.enqueue(keep.BlobRef(), []string{keep.permission})
      return false
    }
    // TODO: Is the permission still valid or has it been overruled?
    
    // The invitation has indeed been issued for the user who issued the keep? If not -> error
    if perm.permission.User != keep.Signer() {
      log.Printf("Err: Keep references an invitation targeted at a different user")
      return false
    }
  }
  return true
}

func (self *Indexer) handleKeep(perma *PermaNode, keep *keepNode) bool {
  log.Printf("Handling Keep from %v at %v\n", keep.Signer(), self.userID)
  var perm *permissionNode
  // The signer of the keep is not the signer of the permanode?
  // In this case he must present a valid invitation
  if keep.Signer() != perma.Signer() {
    var err os.Error
    perm, err = self.Permission(keep.permission)
    if err != nil || perm == nil {  // Problem already catched at checkKeep 
      panic("Keep references a permision that is something else or malformed")
    }
  }

  // Does this implicitly accept a pending invitation? Clean it up.
  if _, ok := perma.pendingInvitations[keep.Signer()]; ok {
    perma.pendingInvitations[keep.Signer()] = "", false
  }
  // This keep is new. The permaNode has a new user.
  perma.keeps[keep.Signer()] = keep.BlobRef()

  // This implies accepting an invitation?
  if perm != nil && perm.permission.User == self.userID {
    // Send the keep (which accepts the invitation) to the signer of the invitation
    if self.fed != nil && keep.Signer() != self.userID {
      self.fed.Forward(keep.BlobRef(), []string{keep.Signer()})
    }
    self.openInvitations[perma.BlobRef()] = "", false
    log.Printf("The local user accepted the invitation\n")
    // TODO: Signal this to the application
  } else {
    if perm != nil {
      log.Printf("The user %v accepted the invitation\n", keep.Signer())
      // TODO: Signal this to the application
      // TODO: Send this user all blobs of the local user that are not in the other user's frontier yet.
    } else {
      log.Printf("The user %v keeps his own perma node\n", keep.Signer())
      // TODO: Signal this to the application
    }
  }
  return true
}

func (self *Indexer) HasPermission(userid string, blobref string, mask int) (ok bool, err os.Error) {
  perma, err := self.PermaNode(blobref)
  if err != nil || perma == nil {
    err = os.NewError("No such perma node")
    return false, err
  }
  if perma.ot == nil { // permaNode has no content ?
    return false, nil
  }
  return perma.ot.HasPermission(userid, mask), nil
}

func (self *Indexer) Followers(blobref string) (users []string, err os.Error) {
  perma, err := self.PermaNode(blobref)
  if err != nil || perma == nil {
    err = os.NewError("No such perma node")
    return nil, err
  }
  users = perma.Followers()
  return
}

func (self *Indexer) hasBlobs(blobrefs []string) bool {
  for _, blobref := range blobrefs {
    _, ok := self.nodes[blobref]
    if !ok {
      return false
    }
  }
  return true
}

// ----------------------------------------------------------------
// Transformation and Pruning functions

func transformSeq(nodes []otNode, node otNode) (tnodes []otNode, tnode otNode, err os.Error) {
  tnode = node
  for _, n := range nodes {
    n, tnode, err = transform(n, tnode)
    if err != nil {
      return
    }
    tnodes = append(tnodes, n)
  }
  return
}

func transform(node1 otNode, node2 otNode) (tnode1, tnode2 otNode, err os.Error) {
  tnode1 = node1
  tnode2 = node2
  switch node1.(type) {
  case *mutationNode:
    switch node2.(type) {
    case *mutationNode:
      m1 := *(node1.(*mutationNode))
      m2 := *(node2.(*mutationNode))
      m1.mutation, m2.mutation, err = ot.Transform(node1.(*mutationNode).mutation, node2.(*mutationNode).mutation)
      tnode1 = &m1
      tnode2 = &m2
    case *permissionNode, *keepNode:
      // Do nothing by intention
    default:
      panic("Unknown node type")
    }
  case *permissionNode:
    switch node2.(type) {
    case *mutationNode, *keepNode:
      // Do nothing by intention
    case *permissionNode:
      p1 := *(node1.(*permissionNode))
      p2 := *(node2.(*permissionNode))
      p1.permission, p2.permission, err = ot.TransformPermission(node1.(*permissionNode).permission, node2.(*permissionNode).permission)
      tnode1 = &p1
      tnode2 = &p2
    default:
      panic("Unknown node type")
    }
  case *keepNode:
    // Do nothing by intention    
  default:
    panic("Unknown node type")
  }
  return
}

func pruneSeq(nodes []otNode, prune map[string]bool) (result []otNode, err os.Error) {
  started := false
  var u ot.Mutation
  for _, n := range nodes {
    // This mutation/permission is not to be pruned?
    if _, isundo := prune[n.BlobRef()]; !isundo {
      if started { // Started pruning?
	switch n.(type) {
	case *permissionNode:
	  p := *(n.(*permissionNode))
	  p.permission, err = ot.PrunePermission(n.(*permissionNode).permission, prune)
	  result = append(result, &p)
	case *mutationNode:
	  m := *(n.(*mutationNode))
	  m.mutation, u, err = ot.PruneMutation(n.(*mutationNode).mutation, u)
	  result = append(result, &m)
	case *keepNode:
	  result = append(result, n)
	}
	if err != nil {
	  return
	}
      } else { // Pruning did not yet start. Just append 'n' to the result
	result = append(result, n)
      }
      continue
    }
    switch n.(type) {
    case *permissionNode, *keepNode: // Ignore the permission node
      // Do nothing by intention
    case *mutationNode: // Store in u that the mutation in 'n' are pruned.
      if !started { // Initialize 'u'
	started = true
	u = n.(*mutationNode).mutation
      } else { // Add the mutation of node 'n' to 'u'
	u, err = ot.Compose(u, n.(*mutationNode).mutation)
	if err != nil {
	  return
	}
      }
    }
  }
  return
}
