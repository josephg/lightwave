package lightwaveot

import (
  "os"
)

// A permission is a set of 31 permission bits.
// A permission is applied by first OR-ing the document-bits with the allow-bits.
// Then, AND the resulting bits with the deny-bits.
// If an allow-bit is 1 then the corresponding deny bit must be 1 as well.
// In other words, allow and deny must not contradict each other.
type Permission struct {
  ID string
  Dependencies []string
  // A 1 bit explicitly allows something
  Allow int "allow"
  // A 0 bit explicitly denies something
  Deny int "deny"
  User string "user"
  Domain string "domain"
  // This property is not serialized
  History []permissionHistory
  OriginalDeny int
  OriginalAllow int
}

type permissionHistory struct {
  id string
  deny int
  allow int
}

func (self *Permission) IsValid() bool {
  return self.Allow & self.Deny == self.Allow
}

func TransformPermission(p1, p2 Permission) (tp1, tp2 Permission, err os.Error) {
  tp1 = p1
  tp2 = p2
  if len(tp1.History) == 0 {
    tp1.OriginalAllow = p1.Allow
    tp1.OriginalDeny = p1.Deny
  }
  if len(tp2.History) == 0 {
    tp2.OriginalAllow = p2.Allow
    tp2.OriginalDeny = p2.Deny
  }
  
  // Permissions for different users?
  if p1.User != p2.User || p1.Domain != p2.Domain {
    return
  }
  
  // It is not possible that one permission explicitly allows something that is explicitly forbidden by the other one.
  if (0xfffffff ^ (p1.Allow ^ p1.Deny)) & (0xfffffff ^ (p2.Allow ^ p2.Deny)) & (p1.Allow ^ p2.Allow) != 0 {
    err = os.NewError("Permissions are not based on the same document version")
    return
  }
  if !p1.IsValid() || !p2.IsValid() {
    err = os.NewError("The permissions are not valid")
    return
  }
  
  tp1.Allow = (p1.Allow & p2.Deny) &^ p2.Allow
  tp1.Deny = p1.Deny | (0xfffffff ^ p2.Deny)
  tp2.Allow = (p2.Allow & p1.Deny) &^ p1.Allow
  tp2.Deny = p2.Deny | (0xfffffff ^ p1.Deny)
  // Record the transformation history to realize pruning later on
//  tp1.history = make([]permissionHistory, len(p1.history))
//  tp2.history = make([]permissionHistory, len(p2.history))
//  if len(p1.history) > 0 {
//    copy(tp1.history, p1.history)
//  }
  tp1.History = append(tp1.History, permissionHistory{id:p2.ID, deny:p2.Deny, allow:p2.Allow})
//  if len(p2.history) > 0 {
//    copy(tp2.history, p2.history)
//  }
  tp2.History = append(tp2.History, permissionHistory{id:p1.ID, deny:p1.Deny, allow:p1.Allow})
  return
}

func ComposePermission(p1, p2 Permission) (c Permission) {
  c.Deny = p1.Allow | p2.Allow | (p1.Deny & p2.Deny)
  c.Allow = (p1.Allow & p2.Deny) | (p2.Allow & p1.Deny)
  return
}

func PrunePermission(p Permission, prune map[string]bool) (tp Permission, err os.Error) {
  tp = p
  // Find out if any of the pruned permissions influenced 'p'.
  recompute := false
  for _, h := range p.History {
    if _, ok := prune[h.id]; ok {
      recompute = true
      break
    }
  }
  if !recompute {
    return
  }
  tp.History = []permissionHistory{}
  tp.Allow = tp.OriginalAllow
  tp.Deny = tp.OriginalDeny
  for _, h := range p.History {
    if _, ok := prune[h.id]; ok {
      continue
    }
    var x Permission
    x.User = p.User
    x.Domain = p.Domain
    x.Allow = h.allow
    x.Deny = h.deny
    x.ID = h.id
    tp, _, err = TransformPermission(tp, x)
    if err != nil {
      return
    }
  }  
  return
}
