package lightwaveot

import (
  "json"
  "os"
  "crypto/sha256"
  "encoding/hex"
)

// {"site":"xxx", "op":{"$a":[ "Hello World", 100, 200, {"$s":5}, {"$d":3} ] } }
// {"site":"xxx", "op":{"$t":[ "Hello World", {"$s":5}, {"$d":3} ] } }
// {"site":"xxx", "op":{$o:{"k":"myattr", "v":0, "m":{"$t":[ {"$i":"Hello World"}, {"$s":5}, {"$d":3} ] } } } }
// {"site":"xxx", "op":{"myattr":{"v":0, "s":{"$t":[ {"$i":"Hello World"}, {"$s":5}, {"$d":3} ] } } } }
// {"site":"xxx", "op":{"myattr":{"v":1, "v":"Some constant"} } }
// {"site":"xxx", "op":{"myattr":{"d":1} } }

func DecodeMutation(blob []byte) (result Mutation, err os.Error) {
  // Decode JSON
  j := make(map[string]interface{})
  if err = json.Unmarshal(blob, &j); err != nil {
    return
  }
  // Site
  site, ok := j["site"]
  if !ok {
    err = os.NewError("JSON data is not a valid mutation: Missing 'site' property")
    return
  }
  if result.Site, ok = site.(string); !ok {
    err = os.NewError("JSON data is not a valid mutation: 'site' property must be a string")
    return
  }
  // Operation
  op, ok := j["op"]
  if !ok {
    err = os.NewError("JSON data is not a valid mutation: Missing 'op' property")
    return
  }
  if result.Operation, err = decodeOperation(op); err != nil {
    return
  }
  // Compute the hash and encode it has hex
  h := sha256.New()
  h.Write(blob)
  result.ID = hex.EncodeToString(h.Sum())
  return
}

func decodeOperation(operation interface{}) (result Operation, err os.Error) {
  op, ok := operation.(map[string]interface{})
  if !ok {
    result.Kind = InsertOp
    result.Len = 1
    result.Value = operation
    return
  }  
  // StringOp ?
  t, ok := op["$t"]
  if ok {
    arr, ok := t.([]interface{}) 
    if ok {
      result.Kind = StringOp
      result.Len = 1
      for _, a := range arr {
	var o Operation
	o, err = decodeOperation(a)
	if err != nil {
	  return
	}
	if o.Kind == InsertOp {
	  str, ok := o.Value.(string)
	  if !ok {
	    err = os.NewError("Can only insert strings inside text")
	    return
	  }
	  o.Len = len(str)
	}
	result.Operations = append(result.Operations, o)
      }
    } else {
      err = os.NewError("Malformed mutation")
    }
    return
  }
  s, ok := op["$s"]
  if ok {
    skip, ok := s.(float64)
    if ok {
      result.Kind = SkipOp
      result.Len = int(skip)
    } else {
      err = os.NewError("Malformed mutation")
    }
    return
  }
  d, ok := op["$d"]
  if ok {
    del, ok := d.(float64)
    if ok {
      result.Kind = DeleteOp
      result.Len = int(del)
    } else {
      err = os.NewError("Malformed mutation")
    }
    return
  }
  // TODO: Array
  // TODO ObjectOp ?
  result.Kind = InsertOp
  result.Len = 1
  result.Value = operation  
  return
}

func EncodeMutation(mut Mutation) (result []byte, err os.Error) {
  var op interface{}
  op, err = encodeOperation(mut.Operation)
  if err != nil {
    return
  }
  j := map[string]interface{}{ "site": mut.Site, "op": op }
  result, err = json.Marshal(j)
  return
}

func encodeOperation(op Operation) (result interface{}, err os.Error) {
  switch op.Kind {
  case InsertOp:
    result = op.Value
  case DeleteOp:
    result = map[string]interface{}{"$d": op.Len}
  case SkipOp:
    result = map[string]interface{}{"$s": op.Len}
  case StringOp:
    arr := []interface{}{}
    for _, o := range op.Operations {
      var x interface{}
      x, err = encodeOperation(o)
      if err != nil {
	return
      }
      arr = append(arr, x)
    }
    result = map[string]interface{}{"$t": arr}
  case ObjectOp:
    // TODO
  case AttributeOp:
    // TOOD
  case ArrayOp:
    // TODO
  default:
    return nil, os.NewError("Unknown operation kind")
  }
  return
}