package lightwavegrapher

import (
  ot "lightwaveot"
  "json"
  "log"
  "os"
  "time"
  "rand"
  "fmt"
  "strings"
  "strconv"
  "crypto/sha256"
  "encoding/hex"
)

// --------------------------------------------------
// Struct to deserialize any schema blob

type superSchema struct {
  // Allowed value are "permanode", "mutation", "permission", "keep"
  Type    string "type"
  Time    int64 "t"
  Signer string "signer"
  
  Permission string "permission"
  Action string "action"
//  Sig    string "sig"

  Dependencies []string "dep"
  
  Random string "random"
  PermaNode string "perma"
  MimeType string "mimetype"
  
  User string "user"
  Allow int "allow"
  Deny int "deny"
  
  Operation *json.RawMessage "op"
  Entity string "entity"
  Field string "field"
  
  Content *json.RawMessage "content"
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
  TransformMutation(mutation MutationNode, rollback <-chan MutationNode, concurrent []string) os.Error
  TransformClientMutation(mutation_input MutationNode, rollback <-chan MutationNode) os.Error
  Kind() int
  DataType() int
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
  Blob_Entity(perma PermaNode, entity EntityNode)
  Blob_DeleteEntity(perma PermaNode, entity DelEntityNode)
}

// The blob store as seen by the Grapher
type BlobStore interface {
  StoreBlob(blob []byte, blobref string) (finalBlobRef string, err os.Error)
  GetBlob(blobref string) (blob []byte, err os.Error)
}

type GraphStore interface {
  StoreNode(perma_blobref string, blobref string, data map[string]interface{}, perma_data map[string]interface{}) os.Error
  StorePermaNode(perma_blobref string, data map[string]interface{}) os.Error
  GetPermaNode(blobref string) (data map[string]interface{}, err os.Error)
  HasOTNodes(perma_blobref string, blobrefs []string) (missing_blobrefs []string, err os.Error)
  GetOTNodeBySeqNumber(perma_blobref string, seqNumber int64) (data map[string]interface{}, err os.Error)
  GetOTNodeByBlobRef(perma_blobref string, blobref string) (data map[string]interface{}, err os.Error)
  GetOTNodesAscending(perma_blobref string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan map[string]interface{}, err os.Error)
  GetOTNodesDescending(perma_blobref string) (ch <-chan map[string]interface{}, err os.Error)
  GetMutationsAscending(perma_blobref string, entity_blobref string, field string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan map[string]interface{}, err os.Error)
  Enqueue(perma_blobref string, blobref string, dependencies []string) os.Error
  Dequeue(perma_blobref string, blobref string) (blobrefs []string, err os.Error)
}

// ------------------------------------------------------
// Grapher

type Grapher struct {
  gstore GraphStore
  store BlobStore
  fed Federation
  // 'user@domain' of the local user.
  userID string 
  transformers map[string]Transformer
  api API
  schema *Schema
}

// Creates a new indexer for the specified user based on the blob store.
// The indexer calls the federation object to send messages to other users.
// Federation may be nil as well.
func NewGrapher(userid string, schema *Schema, store BlobStore, gstore GraphStore, fed Federation) *Grapher {
  idx := &Grapher{userID: userid, store: store, gstore: gstore, fed: fed, schema: schema, transformers: make(map[string]Transformer)}
  if fed != nil {
    fed.SetGrapher(idx)
  }
  return idx
}

func (self *Grapher) AddTransformer(transformer Transformer) {
  self.transformers[strconv.Itoa(transformer.Kind()) + "/" + strconv.Itoa(transformer.DataType())] = transformer
  log.Printf("REGISTERED %v", strconv.Itoa(transformer.Kind()) + "/" + strconv.Itoa(transformer.DataType()));
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
  return p.Followers(), nil
}

func (self *Grapher) permaNode(blobref string) (perma *permaNode, err os.Error) {
  m, err := self.gstore.GetPermaNode(blobref)
  if err != nil || m == nil {
    return nil, err
  }
  if m["k"].(int64) != OTNode_Perma {
    return nil, os.NewError("Blob is not a perma blob")
  } 
  p := NewPermaNode(self)
  p.FromMap(blobref, m)
  return p, nil  
}

func (self *Grapher) transformer(perma PermaNode, entity EntityNode, field string) (t Transformer, err os.Error) {
  fileSchema, ok := self.schema.FileSchemas[perma.MimeType()]
  if !ok {
    err = os.NewError("Unknown document mime type")
    return
  }
  entitySchema, ok := fileSchema.EntitySchemas[entity.MimeType()]
  if !ok {
    err = os.NewError("Unknown entity mime type")
    return
  }
  fieldSchema, ok := entitySchema.FieldSchemas[field]
  if !ok {
    err = os.NewError("Unknown field")
    return
  }
  if fieldSchema.Transformation == TransformationNone {
    return nil, nil
  }
  t, ok = self.transformers[strconv.Itoa(fieldSchema.Transformation) + "/" + strconv.Itoa(fieldSchema.Type)]
  if !ok {
    err = os.NewError("Unknown transformer " + strconv.Itoa(fieldSchema.Transformation) + "/" + strconv.Itoa(fieldSchema.Type))
    return
  }
  return
}

func (self *Grapher) entity(perma_blobref string, blobref string) (entity *entityNode, err os.Error) {
  m, err := self.gstore.GetOTNodeByBlobRef(perma_blobref, blobref)
  if err != nil || m == nil {
    return nil, err
  }
  if m["k"].(int64) != OTNode_Entity {
    return nil, os.NewError("Blob is not an entity blob")
  } 
  e := &entityNode{}
  e.FromMap(perma_blobref, m)
  return e, nil  
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
  switch schema.Type {
  case "keep":
    if schema.PermaNode == "" {
      return nil, os.NewError("Missing perma in keep")
    }
    n := &keepNode{keepBlobRef: blobref, keepSigner: schema.Signer, permaBlobRef: schema.PermaNode, dependencies: schema.Dependencies, permissionBlobRef: schema.Permission}
    return n, nil
  case "permanode":
    n := NewPermaNode(self)
    n.blobref = blobref
    n.mimeType = schema.MimeType
    n.signer = schema.Signer
    // The owner of the permanode has all the rights on it
    n.permissions = map[string]int{n.signer: ^0}
    return n, nil
  case "entity":
    if schema.PermaNode == "" {
      return nil, os.NewError("Missing perma in entity")
    }
    if schema.Content == nil {
      return nil, os.NewError("Entity must have some content")
    }
    n := &entityNode{entityBlobRef: blobref, entitySigner: schema.Signer, permaBlobRef: schema.PermaNode, dependencies: schema.Dependencies, mimeType: schema.MimeType, content: []byte(*schema.Content)}
    return n, nil
  case "delentity":
    if schema.PermaNode == "" {
      return nil, os.NewError("Missing perma in entity")
    }
    if schema.Entity == "" {
      return nil, os.NewError("Mutation is lacking an entity")
    }
    n := &delEntityNode{delBlobRef: blobref, delSigner: schema.Signer, entityBlobRef: schema.Entity, permaBlobRef: schema.PermaNode, dependencies: schema.Dependencies}
    return n, nil
  case "mutation":
    if schema.Operation == nil {
      return nil, os.NewError("Mutation is lacking an operation")
    }
    if schema.Entity == "" {
      return nil, os.NewError("Mutation is lacking an entity")
    }
    if schema.Field == "" {
      return nil, os.NewError("Mutation is lacking a field")
    }
    if schema.PermaNode == "" {
      return nil, os.NewError("Missing perma in mutation")
    }
    n := &mutationNode{mutationSigner: schema.Signer, permaBlobRef: schema.PermaNode, mutationBlobRef: blobref, dependencies: schema.Dependencies, operation: []byte(*schema.Operation), entityBlobRef: schema.Entity, field: schema.Field, time: schema.Time}
    return n, nil
  case "permission":
    if schema.User == "" {
      err = os.NewError("permission is lacking a target user")
      return
    }
    if schema.PermaNode == "" {
      return nil, os.NewError("Missing perma in permission")
    }
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
    log.Printf("Err: Unknown schema type: " + schema.Type)
  }
  return nil, os.NewError("Unknown schema type: " + schema.Type)
}

// Invoked from the blob store
func (self *Grapher) HandleBlob(blob []byte, blobref string) (err os.Error) {
  var perma *permaNode
  // First, determine the mimetype
  mimetype := MimeType(blob)
  if mimetype == "application/x-lightwave-schema" { // Is it a schema blob?
    // Try to decode it into a camli-store schema blob
    var schema superSchema
    err = json.Unmarshal(blob, &schema)
    if err != nil {
      log.Printf("Err: Malformed schema blob: %v\n", err)
      return err
    }
    var node AbstractNode
    if perma, node, err = self.handleSchemaBlob(&schema, blobref); node == nil || err != nil {
      return err
    }
  } else {
    // TODO: Handle ordinary binary blobs
    panic("Unsupported blob type")
  }
    
  // Did other blobs wait on this one?
  deps, err := self.dequeue(perma.BlobRef(), blobref)
  if err != nil {
    return err
  }
  for _, dep := range deps {
    b, err := self.store.GetBlob(dep)
    if err != nil {
      log.Printf("Err: Failed retrieving blob: %v\n", err)
      continue
    }
    self.HandleBlob(b, dep)
  }
  return nil;
}

func (self *Grapher) handleSchemaBlob(schema *superSchema, blobref string) (perma *permaNode, node AbstractNode, err os.Error) {
  newnode, err := self.decodeNode(schema, blobref)
  if err != nil {
    log.Printf("Err: Schema blob is not valid: %v\n", err)
    return nil, nil, err
  }
  node = newnode.(AbstractNode)
  // The node is linked to another permaNode?
  if node.PermaBlobRef() != "" {
    perma, err = self.permaNode(node.PermaBlobRef())
    if err != nil {
      log.Printf("Err: The specified node is not a perma node")
      return nil, nil, err
    }
    if perma == nil {
      self.enqueue(node.PermaBlobRef(), blobref, []string{node.PermaBlobRef()})
      return nil, nil, nil
    }
  }
  switch newnode.(type) {
  case *permaNode:
    perma = newnode.(*permaNode)
    // Store to persistent storage
    self.gstore.StorePermaNode(perma.BlobRef(), perma.ToMap())
  case OTNode:
    if perma == nil {
      log.Printf("Err: OT node without a permanode: %v", node.PermaBlobRef())
      return nil, nil, os.NewError("OT node without a permanode");
    }
    // Is this an invitation?
    if inv, ok := newnode.(*permissionNode); ok && inv.action == PermAction_Invite {
      self.handleInvitation(perma, inv)
      // Do not apply the blob here. We must first download all the data
      //self.enqueue(perma.BlobRef(), blobref, inv.Dependencies())
      //return
    } else if keep, ok := newnode.(*keepNode); ok {
      processed := self.checkKeep(perma, keep)
      if !processed {
	return
      }
    }
    var transformer Transformer
    if mut, ok := newnode.(*mutationNode); ok {
      entity, err := self.entity(perma.BlobRef(), mut.EntityBlobRef())
      if err != nil {
	return nil, nil, err
      }
      transformer, err = self.transformer(perma, entity, mut.Field())
      if err != nil {
	return nil, nil, err
      }
    }
    deps, err := perma.apply(newnode.(OTNode), transformer)
    if err != nil {
      log.Printf("Err: applying blob failed: %v\nblobref=%v\n", err, blobref)
      return nil, nil, err
    }
    // The blob could not be applied because of unresolved dependencies?
    if len(deps) > 0 {
      self.enqueue(perma.BlobRef(), blobref, deps)
      return nil, nil, nil
    }
    
    processed := true
    if _, ok := newnode.(*permissionNode); ok {
      processed = self.handlePermission(perma, newnode.(*permissionNode))
    } else if _, ok := newnode.(*keepNode); ok {
      processed = self.handleKeep(perma, newnode.(*keepNode))
    } else if _, ok := newnode.(*mutationNode); ok {
      processed = self.handleMutation(perma, newnode.(*mutationNode))
    } else if _, ok := newnode.(*entityNode); ok {
      processed = self.handleEntity(perma, newnode.(*entityNode))
    } else if _, ok := newnode.(*delEntityNode); ok {
      processed = self.handleDelEntity(perma, newnode.(*delEntityNode))
    }
    
    // Store to persistent storage
    if processed {
      perma_data := perma.ToMap()
      self.gstore.StoreNode(perma.BlobRef(), newnode.(OTNode).BlobRef(), newnode.(OTNode).ToMap(), perma_data)
      self.gstore.StorePermaNode(perma.BlobRef(), perma_data)
      log.Printf("Grapher processed blob %v at %v\n", node.BlobRef(), self.userID)
    }
  default:
    log.Printf("Err: Unknown blob type\n")
    return nil, nil, os.NewError("Unknown blob type")
  }

  // Forward the blob to all followers
  if self.fed != nil && node.Signer() == self.userID {
    users := perma.followersWithPermission(Perm_Read)
    if len(users) > 0 {
      self.fed.Forward(blobref, users)
    }
  }
  return perma, node, nil
}

func (self *Grapher) handleInvitation(perma *permaNode, perm *permissionNode) {
  log.Printf("Handle invitation")
//  self.openInvitations[perma.BlobRef()] = perm.BlobRef()
  // Signal to the next layer that an invitation has been received
  if self.api != nil {
    self.api.Signal_ReceivedInvitation(perma, perm)
  }
}

func (self *Grapher) handleMutation(perma *permaNode, mut *mutationNode) bool {
  if self.api != nil {
    self.api.Blob_Mutation(perma, mut)
  }
  return true
}

func (self *Grapher) handleEntity(perma *permaNode, entity *entityNode) bool {
  if self.api != nil {
    self.api.Blob_Entity(perma, entity)
  }
  return true
}

func (self *Grapher) handleDelEntity(perma *permaNode, delentity *delEntityNode) bool {
  if self.api != nil {
    self.api.Blob_DeleteEntity(perma, delentity)
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
    log.Printf("Err: Keep references a permission that is something else or malformed")
    return false
  }
  // Permission has not yet been received or processed? -> enqueue
  if perm == nil {
    log.Printf("Permission is not yet applied for the keep")
    self.enqueue(perma.BlobRef(), keep.BlobRef(), []string{keep.permissionBlobRef})
    // The user accepted the invitation?
    if keep.Signer() == self.userID {
      // Both users are on the different domains? -> Download the nodes
      if domain(keep.Signer()) != domain(self.userID) {
	if self.fed != nil {
	  go self.fed.DownloadPermaNode(keep.permissionBlobRef)
	} else {
	  log.Printf("Err: Cannot accept invitation from remote user when federation is turned off")
	}
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
  
  // The user accepted the invitation?
  if self.api != nil {
    self.api.Signal_AcceptedInvitation(perma, perm, keep)
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

  // This keep is new. The permaNode has a new user.
  perma.addKeep(keep.Signer())
  log.Printf("Processing keep of %v\n", keep.Signer())
  // Signal the keep to the application
  if self.api != nil {
    if perm != nil {
      self.api.Blob_Keep(perma, perm, keep)
    } else {
      self.api.Blob_Keep(perma, nil, keep)
    }
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

func (self *Grapher) getMutationsAscending(perma_blobref string, entity_blobref string, field string, startWithSeqNumber int64, endSeqNumber int64) (ch <-chan MutationNode, err os.Error) {
  ch2, err := self.gstore.GetMutationsAscending(perma_blobref, entity_blobref, field, startWithSeqNumber, endSeqNumber)
  if err != nil {
    return nil, err
  }
  
  c := make(chan MutationNode)
  f := func() {
    for data := range ch2 {
      c <- self.mutationNodeFromMap(perma_blobref, data)
    }
    close(c)
  }
  
  go f()
  return c, nil
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
  case OTNode_Entity:
    e := &entityNode{}
    e.FromMap(perma_blobref, data)
    return e
  case OTNode_DelEntity:
    e := &delEntityNode{}
    e.FromMap(perma_blobref, data)
    return e
  default:
    panic("Malformed data")
  }
  return nil
}

func (self *Grapher) mutationNodeFromMap(perma_blobref string, data map[string]interface{}) MutationNode {
  switch data["k"].(int64) {
  case OTNode_Mutation:
    m := &mutationNode{}
    m.FromMap(perma_blobref, data)
    return m
  default:
    panic("Not a mutation node")
  }
  return nil
}

// Interface towards the API
func (self *Grapher) Repeat(perma_blobref string, startWithSeqNumber int64) (perma PermaNode, err os.Error) {
  perma, err = self.permaNode(perma_blobref)
  if err != nil {
    return nil, err
  }  
  ch, err := self.getOTNodesAscending(perma_blobref, startWithSeqNumber, -1)
  if err != nil {
    return nil, err
  }
  
  for n := range ch {
    switch n.(type) {
    case *mutationNode:
      mut := n.(*mutationNode)
      self.api.Blob_Mutation(perma, mut)
    case *keepNode:
      keep := n.(*keepNode)
      var perm PermissionNode = nil
      if keep.permissionBlobRef != "" {
	perm, err = self.permission(perma.BlobRef(), keep.permissionBlobRef)
	if err != nil {
	  log.Printf("Could not get permission")
	  return nil, err
        }
      }
      self.api.Blob_Keep(perma, perm, keep)
    case *permissionNode:
      perm := n.(*permissionNode)
      self.api.Blob_Permission(perma, perm)
    case *entityNode:
      e := n.(*entityNode)
      self.api.Blob_Entity(perma, e)
    case *delEntityNode:
      e := n.(*delEntityNode)
      self.api.Blob_DeleteEntity(perma, e)
    default:
      panic("Unknown blob type")
    }
  }
  return
}

func (self *Grapher) CreatePermaBlob(mimeType string) (node AbstractNode, err os.Error) {
  // Create the JSON to compute the hash
  permaJson := map[string]interface{}{ "signer": self.userID, "random":fmt.Sprintf("%v", rand.Int63()), "mimeType":mimeType}
  permaBlob, err := json.Marshal(permaJson)
  if err != nil {
    panic(err.String())
  }
  permaBlob = append([]byte(`{"type":"permanode",`), permaBlob[1:]...)
  log.Printf("Storing perma %v\n", string(permaBlob))
  permaBlobRef := newBlobRef(permaBlob)
  // Process it
  var schema superSchema
  schema.Type = "permanode"
  schema.Signer = self.userID
  schema.Random = fmt.Sprintf("%v", rand.Int63())
  schema.MimeType = mimeType
  _, node, err = self.handleSchemaBlob(&schema, permaBlobRef)
  return
}

// The parameter 'permission_blobref' may be empty if the keep is from the same user that created the permaNode
func (self *Grapher) CreateKeepBlob(perma_blobref, permission_blobref string) (node AbstractNode, err os.Error) {
  // Create a keep on the permaNode.
  keepJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref}
  if permission_blobref != "" {
    keepJson["dep"] = []string{permission_blobref}
    keepJson["permission"] = permission_blobref
  }
  keepBlob, err := json.Marshal(keepJson)
  if err != nil {
    panic(err.String())
  }
  keepBlob = append([]byte(`{"type":"keep",`), keepBlob[1:]...)
  log.Printf("Storing keep %v\n", string(keepBlob))
  keepBlobRef := newBlobRef(keepBlob)
  // Process it
  var schema superSchema
  schema.Type = "keep"
  schema.Signer = self.userID
  schema.PermaNode = perma_blobref
  if permission_blobref != "" {
    schema.Dependencies = []string{permission_blobref}
    schema.Permission = permission_blobref
  }
  _, node, err = self.handleSchemaBlob(&schema, keepBlobRef)
  return
}

func (self *Grapher) CreateEntityBlob(perma_blobref string, mimeType string, content []byte) (node AbstractNode, err os.Error) {
  perma, e := self.permaNode(perma_blobref)
  if e != nil {
    err = e
    return
  }  
  c := json.RawMessage(content)
  deps := perma.frontier.IDs()
  entityJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref, "content": &c, "dep": deps, "mimetype": mimeType}
  entityBlob, err := json.Marshal(entityJson)
  if err != nil {
    panic(err.String())
  }
  entityBlob = append([]byte(`{"type":"entity",`), entityBlob[1:]...)
  log.Printf("Storing entity %v\n", string(entityBlob))
  entityBlobRef := newBlobRef(entityBlob)
  // Process it
  var schema superSchema
  schema.Type = "entity"
  schema.Signer = self.userID
  schema.PermaNode = perma_blobref
  schema.Content = &c
  schema.MimeType = mimeType
  schema.Dependencies = deps
  _, node, err = self.handleSchemaBlob(&schema, entityBlobRef)
  return
}

func (self *Grapher) CreateDeleteEntityBlob(perma_blobref string, entity_blobref string) (node AbstractNode, err os.Error) {
  perma, e := self.permaNode(perma_blobref)
  if e != nil {
    err = e
    return
  }  
  _, e = self.entity(perma.BlobRef(), entity_blobref)
  if e != nil {
    err = e
    return
  }
  deps := perma.frontier.IDs()
  entityJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref, "entity": entity_blobref, "dep": deps}
  entityBlob, err := json.Marshal(entityJson)
  if err != nil {
    panic(err.String())
  }
  entityBlob = append([]byte(`{"type":"delentity",`), entityBlob[1:]...)
  log.Printf("Storing entity %v\n", string(entityBlob))
  entityBlobRef := newBlobRef(entityBlob)
  // Process it
  var schema superSchema
  schema.Type = "delentity"
  schema.Signer = self.userID
  schema.PermaNode = perma_blobref
  schema.Entity = entity_blobref
  _, node, err = self.handleSchemaBlob(&schema, entityBlobRef)
  return
}

func (self *Grapher) CreatePermissionBlob(perma_blobref string, applyAtSeqNumber int64, userid string, allow int, deny int, action int) (node AbstractNode, err os.Error) {
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
  permNode, _, err = perma.transformLocalPermission(permNode, applyAtSeqNumber)
  if err != nil {
    return
  }
  frontier, e := self.Frontier(perma_blobref)
  if e != nil {
    err = e
    return
  }
  // Create JSON to compute the blobref
  permJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref, "dep": frontier, "user": permNode.User, "allow":permNode.Allow, "deny": permNode.Deny}
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
  permBlobRef := newBlobRef(permBlob)
  // Process it
  var schema superSchema
  schema.Type = "permission"
  schema.Signer = self.userID
  schema.PermaNode = perma_blobref
  schema.Dependencies = frontier
  schema.User = permNode.User
  schema.Allow = permNode.Allow
  schema.Deny = permNode.Deny
  schema.Action = permJson["action"].(string)
  _, node, err = self.handleSchemaBlob(&schema, permBlobRef)
  return
}

func (self *Grapher) CreateMutationBlob(perma_blobref string, entity_blobref string, field string, operation interface{}, applyAtSeqNumber int64) (node AbstractNode, err os.Error) {
  perma, e := self.permaNode(perma_blobref)
  if e != nil {
    err = e
    return
  }
  entity, e := self.entity(perma.BlobRef(), entity_blobref)
  if e != nil {
    err = e
    return
  }
  transformer, e := self.transformer(perma, entity, field)
  if e != nil {
    err = e
    return
  }
  // Update the operation such that it can be applied after all currently applied operations
  m := &mutationNode{}
  m.permaBlobRef = perma_blobref
  m.mutationBlobRef = "Z"  // This ensures that the client mutation looses against all server mutations. The client-side must handle it the same.
  m.mutationSigner = self.userID
  m.entityBlobRef = entity_blobref
  m.field = field
  m.operation = operation
  m.time = time.Seconds()
  if transformer != nil {
    ch, e := self.getMutationsAscending(perma.BlobRef(), entity_blobref, field, applyAtSeqNumber, perma.SequenceNumber())
    if e != nil {
      err = e
      return
    }  
    e = transformer.TransformClientMutation(m, ch)
    if e != nil {
      err = e
      return
    }
  }
  deps := perma.frontier.IDs()
  mutJson := map[string]interface{}{ "signer": self.userID, "perma":perma_blobref, "dep": deps, "entity":entity_blobref, "field":field}
  var msg json.RawMessage
  switch m.operation.(type) {
  case ot.Operation:
    op := m.operation.(ot.Operation) // The following two lines work around a problem in GO/JSON
    op_bytes, err := op.MarshalJSON()
    if err != nil {
      return nil, err
    }
    msg = json.RawMessage(op_bytes)
  case []byte:
    msg = json.RawMessage(m.operation.([]byte))
  default:
    panic("Unsupported operation kind")
  }
  mutJson["op"] = &msg
  schema, err := json.Marshal(mutJson)
  if err != nil {
    panic(err.String())
  }
  mutBlob := []byte(`{"type":"mutation",`)
  mutBlob = append(mutBlob, schema[1:]...)
  log.Printf("Storing mut %v\n", string(mutBlob))
  mutBlobRef := newBlobRef(mutBlob)
  // Process it
  var schema2 superSchema
  schema2.Type = "mutation"
  schema2.Signer = self.userID
  schema2.PermaNode = perma_blobref
  schema2.Dependencies = deps
  schema2.Entity = entity_blobref
  schema2.Field = field
  schema2.Operation = &msg
  _, node, err = self.handleSchemaBlob(&schema2, mutBlobRef)
  return
}

type clientSuperSchema struct {
  // Allowed value are "permanode", "mutation", "permission", "keep"
  Type    string "type"
  
  Permission string "permission"
  Action string "action"
  
  Random string "random"
  PermaNode string "perma"
  MimeType string "mimetype"
  
  User string "user"
  Allow int "allow"
  Deny int "deny"
  
  ApplyAt int64 "at"
  Operation *json.RawMessage "op"
  Entity string "entity"
  Field string "field"
  
  Content *json.RawMessage "content"
}

func (self *Grapher) HandleClientBlob(blob []byte) (node AbstractNode, err os.Error) {
  // Try to decode it into a camli-store schema blob
  var schema clientSuperSchema
  err = json.Unmarshal(blob, &schema)
  if err != nil {
    log.Printf("Err: Malformed client schema blob: %v\n", err)
    return nil, err
  }

  switch schema.Type {
  case "permanode":
    node, err = self.CreatePermaBlob(schema.MimeType)
    if err != nil {
      return nil, err
    }
    _, err = self.CreateKeepBlob(node.BlobRef(), "")
    return
  case "keep":
    var permissionBlobRef string
    if schema.Permission != "" {
      data, err := self.gstore.GetOTNodeByBlobRef(schema.PermaNode, schema.Permission)
      if err != nil {
	log.Printf("Unable to find permission %v, %v", schema.PermaNode, schema.Permission)
	return nil, err
      }
      if k, ok := data["k"]; !ok && k.(int64) != OTNode_Permission {
	return nil, os.NewError("Not a permission node")
      }
      perm := &permissionNode{}
      perm.FromMap(schema.PermaNode, data)
      permissionBlobRef = perm.BlobRef()
    }
    node, err = self.CreateKeepBlob(schema.PermaNode, permissionBlobRef)
    if err != nil {
      return nil, err
    }
    return
  case "mutation":
    if schema.Operation == nil {
      return nil, os.NewError("Mutation is lacking an operation")
    }
    if schema.Entity == "" {
      return nil, os.NewError("Mutation is lacking an entity")
    }
    if schema.Field == "" {
      return nil, os.NewError("Mutation is lacking a field")
    }
    node, err = self.CreateMutationBlob(schema.PermaNode, schema.Entity, schema.Field, []byte(*schema.Operation), schema.ApplyAt)
    return
  case "delentity":
    if schema.Entity == "" {
      return nil, os.NewError("Mutation is lacking an entity")
    }
    node, err = self.CreateDeleteEntityBlob(schema.PermaNode, schema.Entity)
    return
  case "entity":
    if schema.MimeType == "" {
      return nil, os.NewError("Entity is lacking a mimetype")
    }
    node, err = self.CreateEntityBlob(schema.PermaNode, schema.MimeType, []byte(*schema.Content))
    return
  case "permission":
    var action int
    switch schema.Action {
    case "invite":
      action = PermAction_Invite
    case "expel":
      action = PermAction_Expel
    case "change":
      action = PermAction_Change
    default:
      err = os.NewError("Unknown action type in permission blob")
      return
    }
    node, err = self.CreatePermissionBlob(schema.PermaNode, schema.ApplyAt, schema.User, schema.Allow, schema.Deny, action)
    return
  default:
    log.Printf("Err: Unknown schema type: " + schema.Type)
  }
  return nil, os.NewError("Unknown schema type: " + schema.Type)
}

func domain(userid string) string {
  return userid[strings.Index(userid, "@") + 1:];
}

func newBlobRef(blob []byte) string {
  h := sha256.New()
  h.Write(blob)
  return string(hex.EncodeToString(h.Sum()))
}
