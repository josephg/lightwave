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
  // Allowed value are "permanode", "mutation", "permission", "invitation", "keep"
  Type    string "type"

  Signer string "signer"
  
  Invitation string "invitation"
//  Sig    string "sig"

  Dependencies []string "dep"
  AppliedAt int "at"
  Site string "site"
  
  Random string "random"
  PermaNode string "perma"
  
  User string "user"
  Domain string "domain"
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

//func (self *node) SetParent(parent string) {
//  self.parent = parent
//}

func (self *node) Parent() string {
  return self.parent
}

func (self *node) Signer() string {
  return self.signer
}

// All nodes must implement this interface
type abstractNode interface {
  BlobRef() string
//  SetParent(parent string)
  Parent() string
}

type permaNode struct {
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

func (self *permaNode) BlobRef() string {
  return self.blobref
}

type otNode interface {
  abstractNode
  Dependencies() []string
}

type permissionNode struct {
  node
  permission ot.Permission
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

// -----------------------------------------------------
// Data structures which are a specialization of the superSchema

type invitationSchema struct {
  Type string "type"
  Signer string "signer"
  PermaNode string "perma"
  User string "user"
  Domain string "domain"
}

  /*
type keepNode struct {
  node
  blobref string
}

func (self *keepNode) BlobRef() string {
  return self.blobref
}
*/

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
  keeps map[string]string
  store BlobStore
  userID string 
}

func NewIndexer(userid string, store BlobStore) *Indexer {
  idx := &Indexer{userID: userid, store: store, nodes: make(map[string]interface{}), waitingBlobs: make(map[string]bool), waitingLists: make(map[string]*lst.List), pendingBlobs: make(map[string]int), keeps: make(map[string]string), openInvitations: make(map[string]string), blobs:make(map[string]bool)}
  store.AddListener(idx)
  return idx
}

func (self *Indexer) permaNode(blobref string) (perma *permaNode, err os.Error) {
  n, ok := self.nodes[blobref]
  if !ok {
    return nil, nil
  }
  perma, ok = n.(*permaNode)
  if !ok {
    err = os.NewError("Blob is not a permanode")
  }
  return
}

func (self *Indexer) invitation(blobref string) (invitation *invitationSchema, err os.Error) {
  // Check that the blob has already been processed by the indexer.
  // This ensures that it is well-formed and signed
   valid, processed := self.blobs[blobref]
  if !processed {
    return nil, nil
  }
  if !valid {
    return nil, os.NewError("The referenced blob is malformed and was rejected by the indexer")
  }
  
  blob, err := self.store.GetBlob(blobref)
  if err != nil { // Blob not found?
    return nil, nil
  }
  var schema invitationSchema
  err = json.Unmarshal(blob, &schema)
  if err != nil {
    return nil, err
  }
  if schema.Type != "invitation" {
    err = os.NewError("Not an invitation")
    return
  }
  return &schema, nil
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
  switch schema.Type {
  case "permanode":
    n := &permaNode{blobref: blobref, node: node{signer: schema.Signer, parent: schema.PermaNode}, keeps: make(map[string]string), pendingInvitations: make(map[string]string)}
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
    if schema.User == "" || schema.Domain == "" {
      err = os.NewError("permission is lacking a target user")
      return
    }
    n := &permissionNode{node: node{signer: schema.Signer, parent: schema.PermaNode}}
    n.permission.ID = blobref
    n.permission.Dependencies = schema.Dependencies
    n.permission.User = schema.User
    n.permission.Domain = schema.Domain
    n.permission.Allow = schema.Allow
    n.permission.Deny = schema.Deny
    return n, nil    
/*  case "invitation":
    if schema.User == "" || schema.Domain == "" {
      err = os.NewError("Invitation is lacking a target user")
      return
    }
    n := &invitationNode{user: schema.User, domain: schema.Domain, node: node{signer: schema.Signer, parent: schema.PermaNode}}
    return n, nil
  case "keep":
    if schema.PermaNode == "" {
      err = os.NewError("Keep is lacking a blob reference to what to keep")
      return
    }
    n := &keepNode{node: node{signer: schema.Signer, parent: schema.PermaNode}}
    return n, nil */
  default:
    log.Printf("Unknown schema type")
  }
  return nil, os.NewError("Unknown schema type")
}

func (self *Indexer) HandleBlob(blob []byte, blobref string) {
  // First, determine the mimetype
  mimetype := MimeType(blob)
  if mimetype == "application/x-lightwave-schema" { // Is it a schema blob?
    self.handleSchemaBlob(blob, blobref)
    return
  }
  // The blob is not interpreted by the indexer because it is not a schema blob
  self.postApply(blobref)
}

func (self *Indexer) handleSchemaBlob(blob []byte, blobref string) {
  // Try to decode it into a camli-store schema blob
  var schema superSchema
  err := json.Unmarshal(blob, &schema)
  if err != nil {
    log.Printf("Malformed schema blob: %v\n", err)
  }
  
  switch schema.Type {
  case "invitation":
    self.handleInvitation(&schema, blobref)
    return
  case "keep":
    self.handleKeep(&schema, blobref)
    return
  }
  
  newnode, err := self.decodeNode(&schema, blobref)
  if err != nil {
    log.Printf("Schema blob is not valid: %v\n", err)
  }
  ptr := newnode.(abstractNode)
  // The node is linked to another permaNode?
  var perma *permaNode
  if ptr.Parent() != "" {
    p, ok := self.nodes[ptr.Parent()]
    if !ok { // The other permaNode is not yet applied? -> enqueue
      self.enqueue(blobref, []string{schema.PermaNode})
      return
    }
    if perma, ok = p.(*permaNode); !ok {
      log.Printf("The specified node is not a perma node")
      return
    }
  }
  switch newnode.(type) {
  case *permaNode:
    self.nodes[blobref] = newnode
    log.Printf("Added a permanode successfully")
  case otNode:
    if perma == nil {
      log.Printf("Permission or mutation without a permanode")
      return
    }
    if perma.ot == nil {
      perma.ot = newOTHistory()
      // The owner of the permanode has all the rights on it
      perma.ot.permissions[perma.signer] = ^0
    }
    deps, err := perma.ot.Apply(newnode.(otNode))
    if err != nil {
      log.Printf("Err: applying blob failed: %v\n", err)
    }
    if len(deps) > 0 {
      self.enqueue(blobref, deps)
      return
    }
    self.nodes[blobref] = newnode
  default:
    log.Printf("Err: Unknown blob type\n")
    return
  }
  self.postApply(blobref)
}

func (self *Indexer) postApply(blobref string) {
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

func (self *Indexer) handleInvitation(schema *superSchema, blobref string) {
  if schema.User == "" || schema.Domain == "" {
    log.Printf("Err: Invitation is lacking a target user")
    return
  }
  if schema.PermaNode == "" {
    log.Printf("Err: Invitation is lacking a blob reference to what to keep")
    return
  }

  // TODO: Is the signer of this invitation allowed to invite in the first place?
  
  userid := schema.User + "@" + schema.Domain
  // Who is invited here? The local user?
  if userid == self.userID {
    // Is there a keep for the referenced permaNode already
    if _, ok := self.keeps[schema.PermaNode]; ok {
      // The invitation has already been accepted. Forget about it
      return
    }
    self.openInvitations[schema.PermaNode] = blobref
    log.Printf("An invitation has been received")
  } else {
    // Do we have the perma node to which this invitation belongs?
    perma, err := self.permaNode(schema.PermaNode)
    if err != nil { // Not a perma node?
      log.Printf("Err: Not a perma node: %v\n", err)
      return
    }
    if perma == nil { // Permanode is not yet in the store?
      self.enqueue(blobref, []string{schema.PermaNode})
      return
    }
    // Add the invitation to remember that this user has been invited.
    perma.pendingInvitations[userid] = blobref
    log.Printf("User %v has been invited\n", userid)
  }
  
  self.postApply(blobref)
}

func (self *Indexer) handleKeep(schema *superSchema, blobref string) {
  if schema.PermaNode == "" {
    log.Printf("Err: Invitation is lacking a blob reference to what to keep")
    return
  }

  // Who signed this keep?
  if schema.Signer == self.userID { // Signed by the local user?
    self.keeps[schema.PermaNode] = blobref
    // Does this implicitly accept an open invitation?
    if _, ok := self.openInvitations[schema.PermaNode]; ok {
      log.Printf("The local user accepted the invitation\n")
      self.openInvitations[schema.PermaNode] = "", false
    }
  } else { // Signed by someone else
    // Do we have the perma node to which this invitation belongs?
    perma, err := self.permaNode(schema.PermaNode)
    if err != nil { // Not a perma node?
      log.Printf("Err: Not a perma node: %v\n", err)
      return
    }
    if perma == nil { // Permanode is not yet in the store?
      self.enqueue(blobref, []string{schema.PermaNode})
      return
    }
    // The signer of the keep is not the signer of the permanode?
    // In this case he must present a valid invitation
    if schema.Signer != perma.signer {
      if schema.Invitation == "" {
	log.Printf("Err: Keep on a foreign permanode is missing an invitation")
	return
      }
      invitation, err := self.invitation(schema.Invitation)
      // Not an invitation?
      if err != nil {
	log.Printf("Err: Keep references an invitation that is something else or malformed")
	return
      }
      // Invitation has not yet been processed? -> enqueue
      if invitation == nil {
	self.enqueue(blobref, []string{schema.Invitation})
	return
      }
      // The invitation has indeed been issued for the user who issued the keep?
      if invitation.User + "@" + invitation.Domain != schema.Signer {
	log.Printf("Err: Keep references an invitation targeted at a different user")
	return
      }
    }
    // Does this implicitly accept a pending invitation? Clean it up.
    if _, ok := perma.pendingInvitations[schema.Signer]; ok {
      perma.pendingInvitations[schema.Signer] = "", false
    }
    // This keep is new? Then the permaNode has a new user.
    if _, ok := perma.keeps[schema.Signer]; !ok {
      perma.keeps[schema.Signer] = blobref
      log.Printf("The user %v accepted the invitation\n", schema.Signer)
    }
  }
  
  self.postApply(blobref)
}

func (self *Indexer) HasPermission(userid string, blobref string, mask int) (ok bool, err os.Error) {
  perma, err := self.permaNode(blobref)
  if err != nil || perma == nil {
    err = os.NewError("No such perma node")
    return false, err
  }
  if perma.ot == nil { // permaNode has no content ?
    return false, nil
  }    
  bits, ok := perma.ot.permissions[userid]
  if !ok { // The requested user is not a user of this permaNode
    return false, nil
  }
  return bits & mask == mask, nil
}

func (self *Indexer) Users(blobref string) (users []string, err os.Error) {
  perma, err := self.permaNode(blobref)
  if err != nil || perma == nil {
    err = os.NewError("No such perma node")
    return nil, err
  }
  users = append(users, perma.signer)
  for userid, _ := range perma.keeps {
    users = append(users, userid)
  }
  return
}

// ----------------------------------------------------------------
// Transformation and Pruning functions

func transformSeq(nodes []otNode, node otNode) (tnodes []otNode, tnode otNode, err os.Error) {
  tnode = node
  for _, n := range nodes {
    err = transform(n, tnode)
    if err != nil {
      return
    }
    tnodes = append(tnodes, n)
  }
  return
}

func transform(node1 otNode, node2 otNode) (err os.Error) {
  switch node1.(type) {
  case *mutationNode:
    switch node2.(type) {
    case *mutationNode:
      m1 := node1.(*mutationNode)
      m2 := node2.(*mutationNode)
      m1.mutation, m2.mutation, err = ot.Transform(m1.mutation, m2.mutation)
    case *permissionNode:
      // Do nothing by intention
    default:
      panic("Unknown node type")
    }
  case *permissionNode:
    switch node2.(type) {
    case *mutationNode:
      // Do nothing by intention
    case *permissionNode:
      p1 := node1.(*permissionNode)
      p2 := node2.(*permissionNode)
      p1.permission, p2.permission, err = ot.TransformPermission(p1.permission, p2.permission)
    default:
      panic("Unknown node type")
    }
  default:
    panic("Unknown node type")
  }
  return
}

func pruneSeq(nodes []otNode, prune map[string]bool) (result []otNode, err os.Error) {
  started := false
  var u ot.Mutation
  for _, n := range nodes {
    // This mutation/permission is not to be pruned
    if _, isundo := prune[n.BlobRef()]; !isundo {
      if started { // Started pruning?
	switch n.(type) {
	case *permissionNode:
	  p := *(n.(*permissionNode))
	  p.permission, err = ot.PrunePermission(p.permission, prune)
	  result = append(result, &p)
	case *mutationNode:
	  m := *(n.(*mutationNode))
	  m.mutation, u, err = ot.PruneMutation(m.mutation, u)
	  result = append(result, &m)
	}
	if err != nil {
	  return
	}
      } else { // Pruning did not yet start. Just append 'n' to the result
	result = append(result, n)
	continue
      }
    }
    switch n.(type) {
    case *permissionNode: // Ignore the permission node
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
