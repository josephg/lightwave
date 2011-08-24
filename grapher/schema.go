package lightwavegrapher

const (
  TypeNone = iota
  TypeInt64
  TypeFloat64
  TypeString
  TypeBool
  TypeBytes
  TypeEntityBlobRef
  TypePermaBlobRef
  TypeArray
  TypeMap
)

const (
  TransformationNone = iota
  TransformationMerge
  TransformationMax
  TransformationMin
)

type Schema struct {
  // The key is a file mime type
  FileSchemas map[string]*FileSchema
}

type FileSchema struct {
  // The key is an entity mime type
  EntitySchemas map[string]*EntitySchema
}

type EntitySchema struct {
  // The key is a field name
  FieldSchemas map[string]*FieldSchema
}

type FieldSchema struct {
  Type int
  ElementType int
  Transformation int
}
