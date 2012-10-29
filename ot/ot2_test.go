package ot

import (
  "testing"
)

func TestObjExec(t *testing.T) {
  o := NewSimpleObject()
  m1 := Mutation{ID: "m1", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: InsertOp, Len: 1, Operations: []Operation{
        Operation{Kind: StringOp, Operations: []Operation{
          Operation{Kind: InsertOp, Len: 11, Value: "Hello World"}}}}}}}}}}
  _, err := Execute(o, m1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val := o.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "Hello World" {
    t.Fatal("Object attribute has wrong value or version")
  }

  m2 := Mutation{ID: "m2", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: StringOp, Len: 1, Operations: []Operation{Operation{Kind: SkipOp, Len: 11}, Operation{Kind: InsertOp, Len: 3, Value: "!!!"}}}}}}}}
  _, err = Execute(o, m2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "Hello World!!!" {
    t.Fatal("Object attribute has wrong value or version")
  }

  m3 := Mutation{ID: "m3", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: InsertOp, Len: 1, Operations: []Operation{
        Operation{Kind: StringOp, Operations: []Operation{
          Operation{Kind: InsertOp, Len: 3, Value: "Old"}}}}},
      Operation{Kind: SkipOp, Len: 1}}}}}}
  _, err = Execute(o, m3)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o.Get("a1")
  if version != 1 || val.(*SimpleText).Text != "Hello World!!!" {
    t.Fatalf("Object attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  m4 := Mutation{ID: "m4", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: StringOp, Len: 1, Operations: []Operation{Operation{Kind: SkipOp, Len: 3}, Operation{Kind: InsertOp, Len: 3, Value: "???"}}}, Operation{Kind: SkipOp, Len: 1}}}}}}
  _, err = Execute(o, m4)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o.Get("a1")
  if version != 1 || val.(*SimpleText).Text != "Hello World!!!" {
    t.Fatalf("Object attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  m0 := Mutation{ID: "m5", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: InsertOp, Len: 1, Operations: []Operation{
        Operation{Kind: StringOp, Operations: []Operation{
          Operation{Kind: InsertOp, Len: 3, Value: "Neu"}}}}}}}}}}
  seq := []Mutation{m1, m2, m3, m4}
  tseq, tm0, err := TransformSeq(seq, m0)
  if err != nil {
    t.Fatal(err.String())
    return
  }

  _, err = Execute(o, tm0)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o.Get("a1")
  if version != 2 || val.(*SimpleText).Text != "Neu" {
    t.Fatalf("Object attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  o2 := NewSimpleObject()
  _, err = Execute(o2, m0)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  for _, m := range tseq {
    _, err = Execute(o2, m)
    if err != nil {
      t.Fatal(err.String())
      return
    }
  }
  version, val = o2.Get("a1")
  if version != 2 || val.(*SimpleText).Text != "Neu" {
    t.Fatalf("Object attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  // Test composition
  cseq := []Mutation{m1, m2, m3, m4}
  c, err := ComposeSeq(cseq)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  o = NewSimpleObject()
  _, err = Execute(o, c)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o.Get("a1")
  if version != 1 || val.(*SimpleText).Text != "Hello World!!!" {
    t.Fatalf("Object attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  cseq = []Mutation{m1, m2, m3, m4, tm0}
  c, err = ComposeSeq(cseq)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  o = NewSimpleObject()
  _, err = Execute(o, c)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o2.Get("a1")
  if version != 2 || val.(*SimpleText).Text != "Neu" {
    t.Fatalf("Object attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  cseq = []Mutation{m0}
  cseq = append(cseq, tseq...)
  c, err = ComposeSeq(cseq)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  o = NewSimpleObject()
  _, err = Execute(o, c)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o2.Get("a1")
  if version != 2 || val.(*SimpleText).Text != "Neu" {
    t.Fatalf("Object attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }
}

func TestObjTransform(t *testing.T) {
  m1 := Mutation{ID: "m1", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: InsertOp, Len: 1, Operations: []Operation{
        Operation{Kind: StringOp, Operations: []Operation{
          Operation{Kind: InsertOp, Len: 11, Value: "Hello World"}}}}}}}}}}
  m2 := Mutation{ID: "m2", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: InsertOp, Len: 1, Operations: []Operation{
        Operation{Kind: StringOp, Operations: []Operation{
          Operation{Kind: InsertOp, Len: 16, Value: "The other option"}}}}}}}}}}
  tm1, tm2, err := Transform(m1, m2)
  if err != nil {
    t.Fatal(err.String())
    return
  }

  o1 := NewSimpleObject()
  o2 := NewSimpleObject()
  _, err = Execute(o1, m1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o1, tm2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o2, m2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o2, tm1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val := o1.Get("a1")
  if version != 1 || val.(*SimpleText).Text != "The other option" {
    t.Fatalf("Object o1 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }
  version, val = o2.Get("a1")
  if version != 1 || val.(*SimpleText).Text != "The other option" {
    t.Fatalf("Object o2 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  // Compose
  o1 = NewSimpleObject()
  o2 = NewSimpleObject()
  c1, err := Compose(m1, tm2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  c2, err := Compose(m2, tm1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o1, c1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o2, c2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o1.Get("a1")
  if version != 1 || val.(*SimpleText).Text != "The other option" {
    t.Fatalf("Object o1 attribute has wrong value or version after composition: %v %v", version, val.(*SimpleText).Text)
  }
  version, val = o2.Get("a1")
  if version != 1 || val.(*SimpleText).Text != "The other option" {
    t.Fatalf("Object o2 attribute has wrong value or version after composition: %v %v", version, val.(*SimpleText).Text)
  }
}

func TestObjTransformAndCompose2(t *testing.T) {
  m1 := Mutation{ID: "m1", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: StringOp, Operations: []Operation{
        Operation{Kind: InsertOp, Len: 2, Value: "ab"}}}}}}}}
  m2 := Mutation{ID: "m2", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: StringOp, Operations: []Operation{
        Operation{Kind: InsertOp, Len: 2, Value: "xy"}}}}}}}}
  tm1, tm2, err := Transform(m1, m2)
  if err != nil {
    t.Fatal(err.String())
    return
  }

  o1 := NewSimpleObject()
  o1.Set("a1", 0, NewSimpleText(""))
  o2 := NewSimpleObject()
  o2.Set("a1", 0, NewSimpleText(""))
  _, err = Execute(o1, m1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o1, tm2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o2, m2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o2, tm1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val := o1.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "abxy" {
    t.Fatalf("Object o1 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }
  version, val = o2.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "abxy" {
    t.Fatalf("Object o2 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  // Compose
  o1 = NewSimpleObject()
  o1.Set("a1", 0, NewSimpleText(""))
  o2 = NewSimpleObject()
  o2.Set("a1", 0, NewSimpleText(""))
  c1, err := Compose(m1, tm2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  c2, err := Compose(m2, tm1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o1, c1)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  _, err = Execute(o2, c2)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o1.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "abxy" {
    t.Fatalf("Object o1 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }
  version, val = o2.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "abxy" {
    t.Fatalf("Object o2 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }
}

func TestObjPrune(t *testing.T) {
  m1 := Mutation{ID: "m1", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: StringOp, Operations: []Operation{
        Operation{Kind: InsertOp, Len: 2, Value: "ab"}}}}}}}}
  m2 := Mutation{ID: "m2", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: StringOp, Operations: []Operation{
        Operation{Kind: InsertOp, Len: 2, Value: "xy"}}}}}}}}

  tm1, tm2, err := Transform(m1, m2)
  if err != nil {
    t.Fatal(err.String())
    return
  }

  m, _, err := PruneMutation(tm2, m1)
  o1 := NewSimpleObject()
  o1.Set("a1", 0, NewSimpleText(""))
  _, err = Execute(o1, m)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val := o1.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "xy" {
    t.Fatalf("Object o1 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  m, _, err = PruneMutation(tm1, m2)
  o1 = NewSimpleObject()
  o1.Set("a1", 0, NewSimpleText(""))
  _, err = Execute(o1, m)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o1.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "ab" {
    t.Fatalf("Object o1 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }
}

func TestObjPrune2(t *testing.T) {
  m1 := Mutation{ID: "m1", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: InsertOp, Len: 1, Operations: []Operation{
        Operation{Kind: StringOp, Operations: []Operation{
          Operation{Kind: InsertOp, Len: 11, Value: "Hello World"}}}}}}}}}}
  m2 := Mutation{ID: "m2", Operation: Operation{Kind: ObjectOp, Len: 1, Operations: []Operation{
    Operation{Kind: AttributeOp, Value: "a1", Operations: []Operation{
      Operation{Kind: InsertOp, Len: 1, Operations: []Operation{
        Operation{Kind: StringOp, Operations: []Operation{
          Operation{Kind: InsertOp, Len: 16, Value: "The other option"}}}}}}}}}}

  tm1, tm2, err := Transform(m1, m2)
  if err != nil {
    t.Fatal(err.String())
    return
  }

  m, _, err := PruneMutation(tm2, m1)
  o1 := NewSimpleObject()
  _, err = Execute(o1, m)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val := o1.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "The other option" {
    t.Fatalf("Object o1 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }

  m, _, err = PruneMutation(tm1, m2)
  o1 = NewSimpleObject()
  _, err = Execute(o1, m)
  if err != nil {
    t.Fatal(err.String())
    return
  }
  version, val = o1.Get("a1")
  if version != 0 || val.(*SimpleText).Text != "Hello World" {
    t.Fatalf("Object o1 attribute has wrong value or version: %v %v", version, val.(*SimpleText).Text)
  }
}
