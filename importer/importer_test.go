package lightwaveimporter

import (
  store "lightwavestore"
  . "lightwavegrapher"
  "testing"
  "fmt"
  "log"
  "os"
  "time"
)

var schema = &Schema{ FileSchemas: map[string]*FileSchema {
    "application/x-test-file": &FileSchema{ EntitySchemas: map[string]*EntitySchema {
	"application/x-test-entity": &EntitySchema { FieldSchemas: map[string]*FieldSchema {
	    "text": &FieldSchema{ Type: TypeString, ElementType: TypeNone, Transformation: TransformationString } } } } } } }
	  

func TestImporter(t *testing.T) {
  fed := &dummyFederation{}
  s := store.NewSimpleBlobStore()
  sg := NewSimpleGraphStore()
  grapher := NewGrapher("a@b", schema, s, sg, nil)
  s.AddListener(grapher)

}
