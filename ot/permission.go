package lightwaveot

import (
  "os"
)

type Permission struct {
  User string "user"
  Domain string "domain"
  Allow int "allow"
  Deny int "deny"
}

func (self *Permission) IsValid() bool {
  return self.Allow & self.Deny == self.Allow
}

func TransformPermissions(p1, p2 Permission) (tp1, tp2 Permission, err os.Error) {
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
  if p1.User != p2.User || p1.Domain != p2.Domain {
    return
  }
  tp1.Allow = (p1.Allow & p2.Deny) &^ p2.Allow
  tp1.Deny = p1.Deny | (0xfffffff ^ p2.Deny)
  tp2.Allow = (p2.Allow & p1.Deny) &^ p1.Allow
  tp2.Deny = p2.Deny | (0xfffffff ^ p1.Deny)
  return
}

func PrunePermissions(p1, p2 Permission) (tp1, tp2 Permission, err os.Error) {
  tp1 = p1
  tp2 = p2
  return
}
