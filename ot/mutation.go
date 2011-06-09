package lightwaveot

import (
  "fmt"
)

const (
  NoOp = iota
  InsertOp    // Used in StringOp and ArrayOp
  DeleteOp    // Used in StringOp and ArrayOp
  SkipOp      // Used in StringOp and ArrayOp
  StringOp    // Used as root or in ArrayOp or in ObjectOp
  OverwriteOp // Used as root or in ArrayOp or in ObjectOp
  ArrayOp     // Used as root or in ArrayOp or in ObjectOp
  ObjectOp    // Used as root or in ArrayOp or in ObjectOp
)

type Operation struct {
  Kind int "k"
  Len int "l"                  // Usually 1, except for SkipOp, DeleteOp (or InsertOp if used in a string mutation)
  Operations []Operation "o" // Might be empty
  Value interface{} "v"        // A simple value, e.g. string or int or float etc. Might be nil
}

type Mutation struct {
  Operation Operation "o" // The root of a tree of operations
  ID string "id"            // A globally unique ID. This ID is used to break a tie if two operations are in conflict 
  Site string "site"        // A site identifier. This an unstructured string which is unique for a combination of user and session.
  Dependencies []string "d" // IDs of mutations on which this mutation depends on directly (indirect dependencies are not listed)
  DebugName string "n"      // For debugging only
}

func (self Mutation) String() string {
  return fmt.Sprintf("MUT %v {%v}", self.DebugName, self.Operation.String())
}

func (self Operation) String() string {
  switch self.Kind {
  case NoOp:
    return "nop"
  case InsertOp:
    if len(self.Value.(string)) == 0 && self.Len > 0 {      
      return fmt.Sprintf("t:%v", self.Len)
    }
    return fmt.Sprintf("i:%v", self.Value.(string))
  case DeleteOp:
    return fmt.Sprintf("d:%v", self.Len)
  case SkipOp:
    return fmt.Sprintf("s:%v", self.Len)
  case StringOp:
    return fmt.Sprintf("str:%v", self.Operations)
  case OverwriteOp:
    return fmt.Sprintf("set:%v", self.Value)
  case ArrayOp:
    return fmt.Sprintf("arr:%v", self.Operations)
  case ObjectOp:
    return fmt.Sprintf("obj:%v", self.Operations)
  default:
    panic("Unsupported op")
  }
  return ""
}
