package lightwavegrapher

import (
  ot "lightwaveot"
  "log"
  "os"
)

// -----------------------------------------------------
// The tree structure that the grapher is building up

// Abstract base class for nodes
type node struct {
  // BlobRef of the parent
  parent string
  signer string
  time int64
}

// The blobref of the parent node or the empty string
func (self *node) Parent() string {
  return self.parent
}

// The userid of the signer
func (self *node) Signer() string {
  return self.signer
}

func (self *node) Timestamp() int64 {
  return self.time
}

// All nodes must implement this interface
type abstractNode interface {
  BlobRef() string
  Parent() string
  Signer() string
  Timestamp() int64
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
  dependencies []string
  blobref string
  // TODO: Storing this here is a waste of memory
  mutation []byte
}

func (self *mutationNode) BlobRef() string {
  return self.blobref
}

func (self *mutationNode) Dependencies() []string {
  return self.dependencies
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

// -----------------------------------------------------------------
// permaNode

type permaNode struct {
  node
  // The blobref of this node
  blobref string
  // The keys are userids. The values are blobrefs of the keep-blob.
  // This map contains keeps of foreign users.
  // The keep of the local user is not stored here.
  keeps map[string]string
  // The keys are userids. The values are blobrefs of the keep-blob.
  pendingInvitations map[string]string
  // The permission bits for all users
  permissions map[string]int
  // An ordered list of applied blob references.
  // The most recent blob is at the end of the list.
  // These blobs may have been transformed before they have been applied  
  // TODO: This is a LARGE data structure. Do not keep it in memory ...
  appliedBlobs []string
  // The applied (and transformed) mutations
  // TODO: This is a LARGE data structure. Do not keep it in memory ...
  members map[string]otNode
  // The current frontier
  frontier ot.Frontier
}

func newPermaNode() *permaNode {
  return &permaNode{frontier: make(ot.Frontier), members: make(map[string]otNode), keeps:make(map[string]string), pendingInvitations: make(map[string]string), permissions: make(map[string]int) }
}

// abstractNode interface
func (self *permaNode) BlobRef() string {
  return self.blobref
}

func (self *permaNode) sequenceNumber() int {
  return len(self.appliedBlobs)
}

func (self *permaNode) followersWithPermission(bits int) (users []string) {
  for userid, _ := range self.keeps {
    if self.members != nil && bits != 0 { // Need to check for special permission bits?
      if self.signer != userid { // The user is not the owner. Then he needs special permissions
	allowed, ok := self.permissions[userid]
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

func (self *permaNode) followers() (users []string) {
  for userid, _ := range self.keeps {
    users = append(users, userid)
  }
  return
}

func (self *permaNode) hasKeep(userid string) bool {
  _, ok := self.keeps[userid]
  return ok
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

func (self *permaNode) hasApplied(blobref string) bool {
  if _, ok := self.members[blobref]; ok {
    return true
  }
  return false
}

// Implements the Builder interface
func (self *permaNode) historyNodes(reverse bool) <-chan interface{} {
  ch := make(chan interface{})
  if reverse {
    f := func() {
      for i := len(self.appliedBlobs) - 1; i >= 0; i-- {
	ch <- self.members[self.appliedBlobs[i]]
      }
      close(ch)
    }
    go f()
    return ch
  }
  f := func() {
    for _, id := range self.appliedBlobs {
      ch <- self.members[id]
    }
    close(ch)
  }
  go f()
  return ch
}

// Implements the Builder interface
func (self *permaNode) historyBlobRefs(reverse bool) <-chan string {
  ch := make(chan string)
  if reverse {
    f := func() {
      for i := len(self.appliedBlobs) - 1; i >= 0; i-- {
	ch <- self.appliedBlobs[i]
      }
      close(ch)
    }
    go f()
    return ch
  }
  f := func() {
    for _, id := range self.appliedBlobs {
      ch <- id
    }
    close(ch)
  }
  go f()
  return ch
}

// If deps is not empty, then the node could not be applied because it depends on
// blobs that have not yet been applied.
func (self *permaNode) apply(perma *permaNode, newnode otNode) (deps []string, rollback int, concurrent []string, err os.Error) {
  // The mutation has already been applied?
  if self.hasApplied(newnode.BlobRef()) {
    return
  }
  // Are all dependencies satisfied, i.e. are all mutations
  // on which mut depends already processed by the builder?
  unsatisfied := false
  for _, dep := range newnode.Dependencies() {
    if !self.hasApplied(dep) {
      unsatisfied = true
      deps = append(deps, dep)
    }
  }
  if unsatisfied {
    return deps, 0, nil, nil
  }

  // Find out how far back we have to go in history to find a common anchor point for transformation
  h := ot.NewHistoryGraph(self.frontier, newnode.Dependencies())
  reverse_permissions := []*permissionNode{}
  prune := map[string]bool{}
  // Need to rollback?
  if !h.Test() {
    // Go back in history until our history is equal to (or earlier than) that of 'mut'.
    // On the way remember which mutations of our history do not belong to the
    // history of 'mut' because these must be pruned.
    for x := range self.historyNodes(true) {
      history_node := x.(otNode)
      if !h.SubstituteBlob(history_node.BlobRef(), history_node.Dependencies()) {
	prune[history_node.BlobRef()] = true
      }
      if x, ok := history_node.(*permissionNode); ok {
	reverse_permissions = append(reverse_permissions, x)
      }
//      if _, ok := history_node.(*mutationNode); ok {
	rollback++
//      }
      if h.Test() {
	break
      }
    }
  }

  // PermissionNodes are handled by the permaNode directly via the code below.
  // MutationNodes are passed on to the Transformer.
  // KeepNodes and PermaNodes are handled directly by the Grapher since they need no
  // transformation.
  if _, ok := newnode.(*permissionNode); ok {
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
    
    // Transform 'mut' to apply it locally
    pnodes = append(pnodes, newnode.(*permissionNode))
    println(len(pnodes))
    for _, n := range permissions {
      if n.BlobRef() != pnodes[0].BlobRef() && ok {
	pnodes, _, err = transformPermissionSeq(pnodes, n)
	if err != nil {
	  log.Printf("TRANSFORM ERR: %v", err)
	  return
	}
      } else {
	pnodes = pnodes[1:]
      }
    }
    newnode = pnodes[0]
  }

  self.appliedBlobs = append(self.appliedBlobs, newnode.BlobRef())
  self.members[newnode.BlobRef()] = newnode
  self.frontier.AddBlob(newnode.BlobRef(), newnode.Dependencies())
  
  if perm, ok := newnode.(*permissionNode); ok {
    bits, ok := perma.permissions[perm.permission.User]
    if !ok {
      bits = 0
    }
    bits, err = ot.ExecutePermission(bits, perm.permission)
    if err == nil {
      perma.permissions[perm.permission.User] = bits
    }
  }
  
  // Send information about rollbacks and prunes to the composer
  for c, _ := range prune {
    concurrent = append(concurrent, c)
  }
  return
}

func (self *permaNode) transform(perm *permissionNode, applyAtSeqNumber int) (tperm *permissionNode, err os.Error) {
  var reverse_permissions []*permissionNode
  i := self.sequenceNumber()
  if i < applyAtSeqNumber {
    return nil, os.NewError("Invalid sequence number")
  }
  for x := range self.historyNodes(true) {
    if i == applyAtSeqNumber {
      break
    }
    history_node := x.(otNode)
    if x, ok := history_node.(*permissionNode); ok {
      reverse_permissions = append(reverse_permissions, x)
    }
  }
  if i != applyAtSeqNumber {
    return nil, os.NewError("Invalid sequence number")
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
  p1.permission, p2.permission, err = ot.TransformPermission(node1.permission, node2.permission)
  tnode1 = &p1
  tnode2 = &p2
  return
}

func prunePermissionSeq(nodes []*permissionNode, prune map[string]bool) (result []*permissionNode, err os.Error) {
  for _, n := range nodes {
    // This mutation/permission is not to be pruned?
    if _, is_prune := prune[n.BlobRef()]; !is_prune {
      p := *n
      p.permission, err = ot.PrunePermission(n.permission, prune)
      result = append(result, &p)
      if err != nil {
	return
      }
    }
  }
  return
}
