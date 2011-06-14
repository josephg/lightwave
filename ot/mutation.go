package lightwaveot

import (
  "fmt"
)

const (
  NoOp = iota
  InsertOp    // Used in StringOp and ArrayOp and AttributeOp
  DeleteOp    // Used in StringOp and ArrayOp
  SkipOp      // Used in StringOp and ArrayOp and AttributeOp
  StringOp    // Used as root or in ArrayOp or in ObjectOp and AttributeOp
  ArrayOp     // Used as root or in ArrayOp or in ObjectOp and AttributeOp
  ObjectOp    // Used as root or in ArrayOp or in ObjectOp and AttributeOp
  AttributeOp // Used in ObjectOp
)

type Operation struct {
  Kind int "k"
  // Usually 1, except for SkipOp, DeleteOp (or InsertOp if used in a string mutation)
  Len int "l"
  // The sub-operations. For example a StringOp will store in this field all the insert, skip and delete
  // operations it wants to apply to a string.
  // This field is empty for InsertOp, DeleteOp, SkipOp and OverwriteOp.
  Operations []Operation "o"
  // A simple value, e.g. string or int or float etc.
  // This value is used in case of InsertOp and OverwriteOp.
  // In case of InsertOp it stores the string value to insert.
  // However, with InsertOp the Value might be an empty string while the Len field is larger than 0.
  // This indicates that the operation wants to insert a number of tombs as specified by the Len field. 
  Value interface{} "v"        
}

type Mutation struct {
  // The root of a tree of operations.
  // This is eithr StringOp, ArrayOp, ObjectOp or OverwriteOp.
  // Currently, only StringOp is supported.
  Operation Operation "o" 
  // A globally unique ID. This ID is used to break a tie if two operations are in conflict.
  ID string "id" 
  // A site identifier. This an unstructured string which is unique for a combination of user and session.
  // This string is used to break a tie if two operations are in conflict.
  Site string "site"
  // IDs of mutations on which this mutation depends directly (indirect dependencies are not listed)
  Dependencies []string "d"
  // This field has only a meaning for the local computer or in the case of a client/server protocol
  AppliedAt int
  // For debugging only
  DebugName string "n"      
}

func (self Mutation) String() string {
  return fmt.Sprintf("MUT %v {%v}", self.DebugName, self.Operation.String())
}

func (self Operation) String() string {
  switch self.Kind {
  case NoOp:
    return "nop"
  case InsertOp:
    if str, ok := self.Value.(string); ok && len(str) == 0 && self.Len > 0 {      
      return fmt.Sprintf("t:%v", self.Len)
    } else {
      return fmt.Sprintf("i:%v", self.Value)
    }
  case DeleteOp:
    return fmt.Sprintf("d:%v", self.Len)
  case SkipOp:
    return fmt.Sprintf("s:%v", self.Len)
  case StringOp:
    return fmt.Sprintf("str:%v", self.Operations)
  case ArrayOp:
    return fmt.Sprintf("arr:%v", self.Operations)
  case ObjectOp:
    return fmt.Sprintf("obj:%v", self.Operations)
  case AttributeOp:
    return fmt.Sprintf("attr[key:%v ops:%v]", self.Value.(string), self.Operations)    
  default:
    panic("Unsupported op")
  }
  return ""
}
