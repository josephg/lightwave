package ot

import "errors"

// A permission is a set of 31 permission bits.
// A permission is applied by first OR-ing the document-bits with the allow-bits.
// Then, AND the resulting bits with the deny-bits.
// If an allow-bit is 1 then the corresponding deny bit must be 1 as well.
// In other words, allow and deny must not contradict each other.
type Permission struct {
  // This property is not serialized
  ID   string
  Deps []string
  // A 1 bit explicitly allows something
  Allow int
  // A 1 bit explicitly denies something
  Deny int
  User string
  // This property is required for pruning
  History []PermissionHistory
  // This property is required for pruning
  OriginalDeny int
  // This property is required for pruning
  OriginalAllow int
}

type PermissionHistory struct {
  ID    string "id"
  Deny  int    "d"
  Allow int    "a"
}

func (self *Permission) IsValid() bool {
  return self.Allow&self.Deny == 0
}

func TransformPermission(p1, p2 Permission) (tp1, tp2 Permission, err error) {
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
  if p1.User != p2.User {
    return
  }

  // It is not possible that one permission explicitly allows something that is explicitly forbidden by the other one.
  if (p1.Allow&p2.Deny) != 0 || (p2.Allow&p1.Deny) != 0 {
    err = errors.New("Permissions are not based on the same document version")
    return
  }
  if !p1.IsValid() || !p2.IsValid() {
    err = errors.New("The permissions are not valid")
    return
  }

  tp1.Allow = (p1.Allow &^ p2.Deny) &^ p2.Allow
  tp1.Deny = p1.Deny &^ p2.Deny
  tp2.Allow = (p2.Allow &^ p1.Deny) &^ p1.Allow
  tp2.Deny = p2.Deny &^ p1.Deny
  // Record the transformation history to realize pruning later on
  tp1.History = append(tp1.History, PermissionHistory{ID: p2.ID, Deny: p2.Deny, Allow: p2.Allow})
  tp2.History = append(tp2.History, PermissionHistory{ID: p1.ID, Deny: p1.Deny, Allow: p1.Allow})
  return
}

func ComposePermission(p1, p2 Permission) (c Permission) {
  c.Deny = (p1.Deny | p2.Deny) &^ (p1.Allow | p2.Allow)
  c.Allow = (p1.Allow &^ p2.Deny) | (p2.Allow &^ p1.Deny)
  return
}

func PrunePermission(p Permission, prune map[string]bool) (tp Permission, err error) {
  tp = p
  // Find out if any of the pruned permissions influenced 'p'.
  recompute := false
  for _, h := range p.History {
    if _, ok := prune[h.ID]; ok {
      recompute = true
      break
    }
  }
  if !recompute {
    return
  }
  tp.History = []PermissionHistory{}
  tp.Allow = tp.OriginalAllow
  tp.Deny = tp.OriginalDeny
  for _, h := range p.History {
    if _, ok := prune[h.ID]; ok {
      continue
    }
    var x Permission
    x.User = p.User
    x.Allow = h.Allow
    x.Deny = h.Deny
    x.ID = h.ID
    tp, _, err = TransformPermission(tp, x)
    if err != nil {
      return
    }
  }
  return
}

func ExecutePermission(bits int, perm Permission) (result int, err error) {
  // Check that all bits that are explicitly allowed are not set yet
  if bits&perm.Allow != 0 {
    err = errors.New("Permission is already granted")
    return
  }
  // Check that all bits that are explicitly denied are set currently
  if bits&perm.Deny != perm.Deny {
    err = errors.New("The permission has already been removed")
    return
  }

  result = (bits | perm.Allow) &^ perm.Deny
  return
}
