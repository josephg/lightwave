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
  // The user whom this permission is granted or denied
  User string "user"
  // The domain of the user whom this permission is granted or denied
  Domain string "domain"
  // A 1 bit explicitly allows something
  Allow int "allow"
  // A 0 bit explicitly denies something
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
