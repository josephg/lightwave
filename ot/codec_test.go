package lightwaveot

import (
  "testing"
  "json"
)
  
func TestJsonCodec(t *testing.T) {
  m1 := []byte(`{"site":"xxx", "dep":[], "op":{"$t":[ "Hello World", {"$s":5}, {"$d":3} ] } }`)

  mut, err := DecodeMutation(m1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  m1b, _, err := EncodeMutation(mut, EncNormal)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  
  d1 := make(map[string]interface{})
  d2 := make(map[string]interface{})
  err = json.Unmarshal(m1, &d1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  err = json.Unmarshal(m1b, &d2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  
  if !compareJson(d1, d2) {
    t.Fatalf("Decoding and Encoding changed the mutation content:\n%v\n%v\n", string(m1), string(m1b))
    return
  }
}

func compareJson(val1, val2 interface{}) bool {
  if obj1, ok := val1.(map[string]interface{}); ok {
	obj2, ok := val2.(map[string]interface{})
	if !ok {
	  return false
	}
	if !compareJsonObject(obj1, obj2) {
	  return false
	}
	return true
  }
  if a1, ok := val1.([]interface{}); ok {
	a2, ok := val2.([]interface{})
	if !ok {
	  return false
	}	  
	if !compareJsonArray(a1, a2) {
	  return false
	}
	return true
  }
  if s1, ok := val1.(string); ok {
	s2, ok := val2.(string)
	if !ok {
	  return false
	}
	if s1 != s2 {
	  return false
	}
	return true
  }
  if s1, ok := val1.(bool); ok {
	s2, ok := val2.(bool)
	if !ok {
	  return false
	}
	if s1 != s2 {
	  return false
	}
	return true
  }
  if s1, ok := val1.(float64); ok {
	s2, ok := val2.(float64)
	if !ok {
	  return false
	}
	if s1 != s2 {
	  return false
	}
	return true
  }
  return false
}

func compareJsonArray(arr1, arr2 []interface{}) bool {
  if len(arr1) != len(arr2) {
	return false
  }
  for i, val1 := range arr1 {
	val2 := arr2[i]
	if !compareJson(val1, val2 ) {
	  return false
	}
  }
  return true
}

func compareJsonObject(obj1, obj2 map[string]interface{}) bool {
  for key, val1 := range obj1 {
	val2, ok := obj2[key]
	if !ok {
	  return false
	}
	if !compareJson(val1, val2 ) {
	  return false
	}
  }
  for key, _ := range obj2 {
	_, ok := obj1[key]
	if !ok {
	  return false
	}
  }
  return true
}
