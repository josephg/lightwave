package lightwavegrapher

import (
  ot "lightwaveot"
  "log"
  "os"
  "json"
)

// -----------------------------------------------------
// The tree structure that the grapher is building up

const (
  OTNode_Keep = 1 + iota
  OTNode_Permission
  OTNode_Entity
  OTNode_DelEntity
  OTNode_Mutation
  OTNode_Perma
)

// All nodes must implement this interface
type AbstractNode interface {
  BlobRef() string
  Signer() string
  PermaBlobRef() string
  ToMap() map[string]interface{}
  FromMap(perma_blobref string, data map[string]interface{})
  // Returns 0 if no time has been specified
  Time() int64
}

// All nodes participating in Operational Transformation must implement this interface
type OTNode interface {
  AbstractNode
  Dependencies() []string
  SetSequenceNumber(seq int64)
  SequenceNumber() int64
}

const (
  PermAction_Invite = iota
  PermAction_Expel
  PermAction_Change
)

type EntityNode interface {
  OTNode
  Content() []byte
  MimeType() string
}

type entityNode struct {
  permaBlobRef string
  entityBlobRef string
  entitySigner string
  content []byte
  dependencies []string
  seqNumber int64
  mimeType string
}

func (self *entityNode) BlobRef() string {
  return self.entityBlobRef
}

func (self *entityNode) Signer() string {
  return self.entitySigner
}

func (self *entityNode) PermaBlobRef() string {
  return self.permaBlobRef
}

func (self *entityNode) Dependencies() []string {
  return self.dependencies
}

func (self *entityNode) SetSequenceNumber(seq int64) {
  self.seqNumber = seq
}

func (self *entityNode) SequenceNumber() int64 {
  return self.seqNumber
}

func (self *entityNode) Time() int64 {
  return 0
}

func (self *entityNode) Content() []byte {
  return self.content
}

func (self *entityNode) MimeType() string {
  return self.mimeType
}

func (self *entityNode) ToMap() map[string]interface{} {
  m := make(map[string]interface{})
  m["k"] = int64(OTNode_Entity)
  m["b"] = self.entityBlobRef
  m["s"] = self.entitySigner
  m["dep"] = self.dependencies
  m["seq"] = self.seqNumber
  m["c"] = self.content
  m["mt"] = self.mimeType;
  return m
}

func (self *entityNode) FromMap(permaBlobRef string, m map[string]interface{}) {
  self.permaBlobRef = permaBlobRef
  self.entityBlobRef = m["b"].(string)
  self.entitySigner = m["s"].(string)
  if d, ok := m["dep"]; ok {
    self.dependencies = d.([]string)
  }
  self.content = m["c"].([]byte)
  self.seqNumber = m["seq"].(int64)
  self.mimeType = m["mt"].(string)
}

type DelEntityNode interface {
  OTNode
  EntityBlobRef() string
}

type delEntityNode struct {
  permaBlobRef string
  delBlobRef string
  entityBlobRef string
  delSigner string
  dependencies []string
  seqNumber int64
}

func (self *delEntityNode) BlobRef() string {
  return self.delBlobRef
}

func (self *delEntityNode) Signer() string {
  return self.delSigner
}

func (self *delEntityNode) EntityBlobRef() string {
  return self.entityBlobRef
}

func (self *delEntityNode) PermaBlobRef() string {
  return self.permaBlobRef
}

func (self *delEntityNode) Dependencies() []string {
  return self.dependencies
}

func (self *delEntityNode) SetSequenceNumber(seq int64) {
  self.seqNumber = seq
}

func (self *delEntityNode) SequenceNumber() int64 {
  return self.seqNumber
}

func (self *delEntityNode) Time() int64 {
  return 0
}

func (self *delEntityNode) ToMap() map[string]interface{} {
  m := make(map[string]interface{})
  m["k"] = int64(OTNode_DelEntity)
  m["b"] = self.delBlobRef
  m["s"] = self.delSigner
  m["e"] = self.entityBlobRef
  m["dep"] = self.dependencies
  m["seq"] = self.seqNumber
  return m
}

func (self *delEntityNode) FromMap(permaBlobRef string, m map[string]interface{}) {
  self.permaBlobRef = permaBlobRef
  self.delBlobRef = m["b"].(string)
  self.delSigner = m["s"].(string)
  self.entityBlobRef = m["e"].(string)
  if d, ok := m["dep"]; ok {
    self.dependencies = d.([]string)
  }
  self.seqNumber = m["seq"].(int64)
}

type PermissionNode interface {
  OTNode  
  UserName() string
  AllowBits() int
  DenyBits() int
  Action() int
}

type permissionNode struct {
  ot.Permission
  permaBlobRef string
  permissionSigner string
  action int
  seqNumber int64
  dependencies []string
}

func (self *permissionNode) BlobRef() string {
  return self.ID
}

func (self *permissionNode) Signer() string {
  return self.permissionSigner
}

func (self *permissionNode) PermaBlobRef() string {
  return self.permaBlobRef
}

func (self *permissionNode) Dependencies() []string {
  return self.dependencies
}

func (self *permissionNode) SetSequenceNumber(seq int64) {
  self.seqNumber = seq
}

func (self *permissionNode) SequenceNumber() int64 {
  return self.seqNumber
}

func (self *permissionNode) Time() int64 {
  return 0
}

func (self *permissionNode) UserName() string {
  return self.User
}

func (self *permissionNode) AllowBits() int {
  return self.Allow
}

func (self *permissionNode) DenyBits() int {
  return self.Deny
}

func (self *permissionNode) Action() int {
  return self.action
}

func (self *permissionNode) ToMap() map[string]interface{} {
  m := make(map[string]interface{})
  m["k"] = int64(OTNode_Permission)
  m["b"] = self.ID
  m["s"] = self.permissionSigner
  m["ac"] = int64(self.action)
  m["u"] = self.User
  m["a"] = int64(self.Allow)
  m["d"] = int64(self.Deny)
  m["oa"] = int64(self.OriginalAllow)
  m["od"] = int64(self.OriginalDeny)
  ha := make([]int64, len(self.History))
  hd := make([]int64, len(self.History))
  hid := make([]string, len(self.History))
  for i, h := range self.History {
    ha[i] = int64(h.Allow)
    hd[i] = int64(h.Deny)
    hid[i] = h.ID
  }
  m["ha"] = ha
  m["hd"] = hd
  m["hid"] = hid
  m["dep"] = self.dependencies
  m["seq"] = self.seqNumber
  return m
}

func (self *permissionNode) FromMap(permaBlobRef string, m map[string]interface{}) {
  self.permaBlobRef = permaBlobRef
  self.ID = m["b"].(string)
  self.permissionSigner = m["s"].(string)
  self.action = int(m["ac"].(int64))
  self.User = m["u"].(string)
  self.Allow = int(m["a"].(int64))
  self.Deny = int(m["d"].(int64))
  self.OriginalAllow = int(m["oa"].(int64))
  self.OriginalDeny = int(m["od"].(int64))
  if _, ok := m["ha"]; ok {
    ha := m["ha"].([]int64)
    hd := m["hd"].([]int64)
    hid := m["hid"].([]string)
    for i := 0; i < len(ha); i++ {
      self.History = append(self.History, ot.PermissionHistory{ID: hid[i], Allow: int(ha[i]), Deny: int(hd[i])})
    }
  }
  if d, ok := m["dep"]; ok {
    self.dependencies = d.([]string)
  }
  self.seqNumber = m["seq"].(int64)
}

type MutationNode interface {
  OTNode
  Operation() interface{}
  SetOperation(op interface{})
  EntityBlobRef() string
  Field() string
}

type mutationNode struct {
  permaBlobRef string
  mutationBlobRef string
  mutationSigner string
  // This is either []byte or an already decoded operation, for example ot.Operation.
  operation interface{}
  entityBlobRef string
  dependencies []string
  seqNumber int64
  field string
  time int64
}

func (self *mutationNode) BlobRef() string {
  return self.mutationBlobRef
}

func (self *mutationNode) Signer() string {
  return self.mutationSigner
}

func (self *mutationNode) PermaBlobRef() string {
  return self.permaBlobRef
}

func (self *mutationNode) Dependencies() []string {
  return self.dependencies
}

func (self *mutationNode) Operation() interface{} {
  return self.operation
}

func (self *mutationNode) SetOperation(op interface{}) {
  self.operation = op
}

func (self *mutationNode) SetSequenceNumber(seq int64) {
  self.seqNumber = seq
}

func (self *mutationNode) SequenceNumber() int64 {
  return self.seqNumber
}

func (self *mutationNode) Time() int64 {
  return self.time
}

func (self *mutationNode) EntityBlobRef() string {
  return self.entityBlobRef
}

func (self *mutationNode) Field() string {
  return self.field
}

func (self *mutationNode) ToMap() map[string]interface{} {
  m := make(map[string]interface{})
  m["k"] = int64(OTNode_Mutation)
  m["b"] = self.mutationBlobRef
  m["s"] = self.mutationSigner
  switch self.operation.(type) {
  case []byte:
    m["op"] = self.operation.([]byte)
  case json.Marshaler:
    bytes, e := self.operation.(json.Marshaler).MarshalJSON()
    if e != nil {
      panic("Failed marshaling")
    }
    m["op"] = bytes
  default:
    panic("Cannot serialize")
  }
  m["e"] = self.entityBlobRef
  m["dep"] = self.dependencies
  m["seq"] = self.seqNumber
  m["f"] = self.field
  if self.time != 0 {
    m["tm"] = self.time;
  }
  return m
}

func (self *mutationNode) FromMap(permaBlobRef string, m map[string]interface{}) {
  self.permaBlobRef = permaBlobRef
  self.mutationBlobRef = m["b"].(string)
  self.mutationSigner = m["s"].(string)
  self.operation = m["op"].([]byte)
  if d, ok := m["dep"]; ok {
    self.dependencies = d.([]string)
  }
  self.entityBlobRef = m["e"].(string)
  self.seqNumber = m["seq"].(int64)
  self.field = m["f"].(string)
  if d, ok := m["tm"]; ok {
    self.time = d.(int64)
  }
}

type KeepNode interface {
  OTNode
}

type keepNode struct {
  permaBlobRef string
  keepBlobRef string
  keepSigner string
  permissionBlobRef string
  dependencies []string
  seqNumber int64
}

func (self *keepNode) BlobRef() string {
  return self.keepBlobRef
}

func (self *keepNode) Signer() string {
  return self.keepSigner
}

func (self *keepNode) PermaBlobRef() string {
  return self.permaBlobRef
}

func (self *keepNode) Dependencies() []string {
  return self.dependencies
}

func (self *keepNode) SetSequenceNumber(seq int64) {
  self.seqNumber = seq
}

func (self *keepNode) SequenceNumber() int64 {
  return self.seqNumber
}

func (self *keepNode) Time() int64 {
  return 0
}

func (self *keepNode) ToMap() map[string]interface{} {
  m := make(map[string]interface{})
  m["k"] = int64(OTNode_Keep)
  m["b"] = self.keepBlobRef
  m["s"] = self.keepSigner
  if self.permissionBlobRef != "" {
    m["p"] = self.permissionBlobRef
  }
  m["dep"] = self.dependencies
  m["seq"] = self.seqNumber
  return m
}

func (self *keepNode) FromMap(permaBlobRef string, m map[string]interface{}) {
  self.permaBlobRef = permaBlobRef
  self.keepBlobRef = m["b"].(string)
  self.keepSigner = m["s"].(string)
  if p, ok := m["p"]; ok {
    self.permissionBlobRef = p.(string)
  }
  if d, ok := m["dep"]; ok {
    self.dependencies = d.([]string)
  }
  self.seqNumber = m["seq"].(int64)
}

// -----------------------------------------------------------------
// permaNode

type PermaNode interface {
  AbstractNode
  MimeType() string
  Updates() map[string]int64
  Followers() []string
  Users() []string
  SequenceNumber() int64
}

type permaNode struct {
  grapher *Grapher
  signer string
  // The blobref of this node
  blobref string
  // The permission bits for all users
  permissions map[string]int
  // The key is a userid and the value is the last sequence number attributed to this user
  updates map[string]int64
  // The current frontier
  frontier ot.Frontier
  seqNumber int64
  mimeType string
}

func NewPermaNode(grapher *Grapher) *permaNode {
  return &permaNode{grapher: grapher, frontier: make(ot.Frontier), permissions: make(map[string]int), updates: make(map[string]int64) }
}

func (self *permaNode) ToMap() map[string]interface{} {
  m := make(map[string]interface{})
  m["k"] = int64(OTNode_Perma)
  m["b"] = self.blobref
  m["s"] = self.signer
  m["f"] = self.frontier.IDs()
  m["n"] = self.seqNumber
  p2 := []int64{}
  p1 := []string{}
  u := []int64{}
  for user, perm := range self.permissions {
    p1 = append(p1, user)
    p2 = append(p2, int64(perm))
    if seq, ok := self.updates[user]; ok {
      u  = append(u, seq)
    } else {
      u = append(u, -1)
    }
  }
  m["up"] = u
  m["p1"] = p1
  m["p2"] = p2
  m["mt"] = self.mimeType
  return m
}

func (self *permaNode) FromMap(permaBlobRef string, m map[string]interface{}) {
  self.blobref = m["b"].(string)
  self.signer = m["s"].(string)
  self.seqNumber = m["n"].(int64)
  if f, ok := m["f"]; ok {
    self.frontier.FromIDs(f.([]string))
  }
  p1 := m["p1"].([]string)
  p2 := m["p2"].([]int64)
  u := m["up"].([]int64)
  for i := 0; i < len(p1); i++ {
    self.permissions[p1[i]] = int(p2[i])
    self.updates[p1[i]] = int64(u[i])
  }
  self.mimeType = m["mt"].(string)
}

// abstractNode interface
func (self *permaNode) PermaBlobRef() string {
  return ""
}

// abstractNode interface
func (self *permaNode) BlobRef() string {
  return self.blobref
}

// abstractNode interface
func (self *permaNode) Signer() string {
  return self.signer
}

func (self *permaNode) MimeType() string {
  return self.mimeType
}

func (self *permaNode) Updates() map[string]int64 {
  return self.updates
}

func (self *permaNode) SequenceNumber() int64 {
  return self.seqNumber
}

func (self *permaNode) Time() int64 {
  return 0
}

func (self *permaNode) followersWithPermission(bits int) (users []string) {
  for userid, allowed := range self.permissions {
    if allowed & Perm_Keep != Perm_Keep {
      continue
    }
    if bits != 0 { // Need to check for special permission bits?
      if self.signer != userid { // The user is not the owner. Then he needs special permissions
	if allowed & bits != bits {
	  continue
	}
      }
    }
    users = append(users, userid)
  }
  return
}

func (self *permaNode) Followers() (users []string) {
  for userid, allowed := range self.permissions {
    if allowed & Perm_Keep != Perm_Keep {
      continue
    }
    users = append(users, userid)
  }
  return
}

// This includes all followers and user that have been invited but not committed to follow so far
func (self *permaNode) Users() (users []string) {
  for userid, allowed := range self.permissions {
    if allowed == 0 || allowed == Perm_Keep { // No permission at all (except havin created a keep)?
      continue
    }
    users = append(users, userid)
  }
  return
}

func (self *permaNode) hasKeep(userid string) bool {
  return self.hasPermission(userid, Perm_Keep)
}

func (self *permaNode) addKeep(userid string) {
  if self.Signer() == userid {
    return
  }
  bits, _ := self.permissions[userid]
  bits |= Perm_Keep
  self.permissions[userid] = bits
}

func (self *permaNode) hasPermission(userid string, mask int) (ok bool) {
  if self.Signer() == userid {
    return true
  }
  bits, ok := self.permissions[userid]
  if !ok { // The requested user is not a user of this permaNode
    return false
  }
  return bits & mask == mask
}

// If deps is not empty, then the node could not be applied because it depends on
// blobs that have not yet been applied.
func (self *permaNode) apply(newnode OTNode, transformer Transformer) (deps []string, err os.Error) {
  deps, err = self.grapher.gstore.HasOTNodes(self.BlobRef(), []string{newnode.BlobRef()})
  if len(deps) == 0 {
    log.Printf("ALREADY APPLIED")
    return nil, os.NewError("OTNode has already been applied")
  }
  deps, err = self.grapher.gstore.HasOTNodes(self.BlobRef(), newnode.Dependencies())
  if len(deps) > 0 {
    return deps, nil
  }
  if perm, ok := newnode.(*permissionNode); ok {
    err = self.applyPermission(perm)
  } else if mut, ok := newnode.(*mutationNode); ok {
    deps, err = self.grapher.gstore.HasOTNodes(self.BlobRef(), []string{mut.EntityBlobRef()})
    if len(deps) != 0 {
      log.Printf("Referenced entity is missing. It should have been in the dependencies")
      return nil, os.NewError("Mutation references an invalid entity")
    }
    err = self.applyMutation(mut, transformer)
  }

  self.frontier.AddBlob(newnode.BlobRef(), newnode.Dependencies())
  newnode.SetSequenceNumber(self.seqNumber)
  // Ignore if the user issued a keep node
  if _, ok := newnode.(*keepNode); !ok {
    self.updates[newnode.Signer()] = self.seqNumber
  }
  self.seqNumber++
  
  return nil, err
}

func (self *permaNode) applyPermission(newnode *permissionNode) (err os.Error) {
  // Find out how far back we have to go in history to find a common anchor point for transformation
  h := ot.NewHistoryGraph(self.frontier, newnode.Dependencies())
  reverse_permissions := []*permissionNode{}
  prune := map[string]bool{}
  // Need to rollback?
  if !h.Test() {
    // Go back in history until our history is equal to (or earlier than) that of 'mut'.
    // On the way remember which mutations of our history do not belong to the
    // history of 'mut' because these must be pruned.
    ch, err := self.grapher.getOTNodesDescending(self.BlobRef())
    if err != nil {
      return err
    }
    for history_node := range ch {
      if !h.SubstituteBlob(history_node.BlobRef(), history_node.Dependencies()) {
	prune[history_node.BlobRef()] = true
      }
      if x, ok := history_node.(*permissionNode); ok {
	reverse_permissions = append(reverse_permissions, x)
      }
      if h.Test() {
	break
      }
    }
  }

  // Reverse the mutation history, such that oldest are first in the list.
  // This is ugly but prepending in the above loops is too slow.
  permissions := make([]*permissionNode, len(reverse_permissions))
  for i := 0; i < len(permissions); i++ {
    permissions[i] = reverse_permissions[len(reverse_permissions) - 1 - i]
  }
  
  // Prune all mutations that have been applied locally but do not belong to the history of the new mutation
  pnodes, e := prunePermissionSeq(permissions, prune)
  if e != nil {
    log.Printf("Prune Error: %v\n", e)
    err = e
    return
  }
    
  // Transform 'newnode' to apply it locally
  pnodes = append(pnodes, newnode)
  for _, n := range permissions {
    if n.BlobRef() != pnodes[0].BlobRef() {
      pnodes, _, err = transformPermissionSeq(pnodes, n)
      if err != nil {
	log.Printf("TRANSFORM ERR: %v", err)
	return
      }
    } else {
      pnodes = pnodes[1:]
    }
  }
  *newnode = *pnodes[0]
    
  bits, ok := self.permissions[newnode.User]
  if !ok {
    bits = 0
  }
  bits, err = ot.ExecutePermission(bits, newnode.Permission)
  if err == nil {
    self.permissions[newnode.User] = bits
  }  
  return
}

func (self *permaNode) applyMutation(newnode *mutationNode, transformer Transformer) (err os.Error) {
  if transformer == nil {
    return
  }
  // Find out how far back we have to go in history to find a common anchor point for transformation
  h := ot.NewHistoryGraph(self.frontier, newnode.Dependencies())
  prune := map[string]bool{}
  rollback := int64(0)
  // Need to rollback?
  if !h.Test() {
    // Go back in history until our history is equal to (or earlier than) that of 'mut'.
    // On the way remember which mutations of our history do not belong to the
    // history of 'mut' because these must be pruned.
    ch, err := self.grapher.getOTNodesDescending(self.BlobRef())
    if err != nil {
      return err
    }
    for history_node := range ch {
      if !h.SubstituteBlob(history_node.BlobRef(), history_node.Dependencies()) {
	prune[history_node.BlobRef()] = true
      }
      rollback++
      if h.Test() {
	break
      }
    }
  }

  concurrent := []string{}
  for c, _ := range prune {
    concurrent = append(concurrent, c)
  }
  ch, err := self.grapher.getMutationsAscending(self.blobref, newnode.EntityBlobRef(), newnode.Field(), self.SequenceNumber() - rollback, self.SequenceNumber())
  if err != nil {
    return err
  }
  err = transformer.TransformMutation(newnode, ch, concurrent)
  if err != nil {
    return err
  }
  return
}

func (self *permaNode) transformLocalPermission(perm *permissionNode, applyAtSeqNumber int64) (tperm *permissionNode, appliedAtSeqNumber int64, err os.Error) {
  var reverse_permissions []*permissionNode
  i := self.SequenceNumber()
  appliedAtSeqNumber = i
  if i < applyAtSeqNumber {
    return nil, 0, os.NewError("Invalid sequence number")
  }
  ch, err := self.grapher.getOTNodesDescending(self.BlobRef())
  if err != nil {
    return nil, 0, err
  }
  for history_node := range ch {
    if i == applyAtSeqNumber {
      break
    }
    i--
    if x, ok := history_node.(*permissionNode); ok {
      reverse_permissions = append(reverse_permissions, x)
    }
  }
  if i != applyAtSeqNumber {
    return nil, 0, os.NewError("Invalid sequence number")
  }
  
  // Reverse the mutation history, such that oldest are first in the list.
  // This is ugly but prepending in the above loops is too slow.
  permissions := make([]*permissionNode, len(reverse_permissions))
  for i := 0; i < len(permissions); i++ {
    permissions[i] = reverse_permissions[len(reverse_permissions) - 1 - i]
  }
  
  _, tperm, err = transformPermissionSeq(permissions, perm)
  if err != nil {
    log.Printf("TRANSFORM ERR: %v", err)
    return
  }
  return
}

// ----------------------------------------------------------------
// Helper functions

func transformPermissionSeq(nodes []*permissionNode, node *permissionNode) (tnodes []*permissionNode, tnode *permissionNode, err os.Error) {
  tnode = node
  for _, n := range nodes {
    n, tnode, err = transformPermission(n, tnode)
    if err != nil {
      return
    }
    tnodes = append(tnodes, n)
  }
  return
}

func transformPermission(node1 *permissionNode, node2 *permissionNode) (tnode1, tnode2 *permissionNode, err os.Error) {
  p1 := *node1
  p2 := *node2
  p1.Permission, p2.Permission, err = ot.TransformPermission(node1.Permission, node2.Permission)
  tnode1 = &p1
  tnode2 = &p2
  return
}

func prunePermissionSeq(nodes []*permissionNode, prune map[string]bool) (result []*permissionNode, err os.Error) {
  for _, n := range nodes {
    // This mutation/permission is not to be pruned?
    if _, is_prune := prune[n.BlobRef()]; !is_prune {
      p := *n
      p.Permission, err = ot.PrunePermission(n.Permission, prune)
      result = append(result, &p)
      if err != nil {
	return
      }
    }
  }
  return
}
