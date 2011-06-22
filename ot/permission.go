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
  // A 1 bit explicitly allows something
  Allow int "allow"
  // A 0 bit explicitly denies something
  Deny int "deny"
}

// They key is a username of the form "user@domain"
type Permissions map[string]Permission

func (self *Permission) IsValid() bool {
  return self.Allow & self.Deny == self.Allow
}

func (self *Permissions) IsValid() bool {
  for _, p := range *self {
    if !p.IsValid() {
      return false
    }
  }
  return true
}

func TransformPermission(p1, p2 Permission) (tp1, tp2 Permission, err os.Error) {
  // It is not possible that one permission explicitly allows something that is explicitly forbidden by the other one.
  if (0xfffffff ^ (p1.Allow ^ p1.Deny)) & (0xfffffff ^ (p2.Allow ^ p2.Deny)) & (p1.Allow ^ p2.Allow) != 0 {
    err = os.NewError("Permissions are not based on the same document version")
    return
  }
  if !p1.IsValid() || !p2.IsValid() {
    err = os.NewError("The permissions are not valid")
    return
  }
  
  tp1 = p1
  tp2 = p2
  tp1.Allow = (p1.Allow & p2.Deny) &^ p2.Allow
  tp1.Deny = p1.Deny | (0xfffffff ^ p2.Deny)
  tp2.Allow = (p2.Allow & p1.Deny) &^ p1.Allow
  tp2.Deny = p2.Deny | (0xfffffff ^ p1.Deny)
  return
}

func TransformPermissions(p1, p2 Permissions) (tp1, tp2 Permissions, err os.Error) {
  tp1 = make(Permissions)
  tp2 = make(Permissions)
  // If one user appears in both permission sets then these permissions must be transformed
  for user, x1 := range p1 {
    if x2, ok := p2[user]; ok {
      tp1[user], tp2[user], err = TransformPermission(x1, x2)
      if err != nil {
	return
      }
    }
  }
  return
}

func ComposePermission(p1, p2 Permission) (c Permission) {
  c.Deny = p1.Allow | p2.Allow | (p1.Deny & p2.Deny)
  c.Allow = (p1.Allow & p2.Deny) | (p2.Allow & p1.Deny)
  return
}

func ComposePermissions(p1, p2 Permissions) (c Permissions, err os.Error) {
  c = make(Permissions)
  for user, x1 := range p1 {
    if x2, ok := p2[user]; ok {
      c[user] = ComposePermission(x1, x2)
    } else {
      c[user] = x1
    }
  }
  for user, x2 := range p2 {
    if _, ok := p1[user]; !ok {
      c[user] = x2
    }
  }
  return
}

func PrunePermission(p1, p2 Permission) (tp1, tp2 Permission, err os.Error) {
  tp1 = p1
  tp2 = p2
  return
}
