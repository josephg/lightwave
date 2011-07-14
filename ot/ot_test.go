package lightwaveot

import (
  "os"
  "testing"
  "rand"
  "fmt"
  "encoding/hex"
  "crypto/sha256"
  vec "container/vector"
)


// --------------------------------------------
// Hashes and hex-encoding helper functions

func hexHash(input string) string {
  return idToString(hash([]byte(input)))
}

func hash(input []byte) []byte {
  h := sha256.New()
  h.Write(input)
  return h.Sum()
}

func idToString(id []byte) string {
  return hex.EncodeToString(id)
}

// -------------------------------------------
// Iterator for permutations

// Can be used in a for-range clause to iterate over all possible permutations of n numbers
func Permutations(n int) (comm chan []int) {
  comm = make(chan []int)
  arr := make([]int, n)
  for i := 0; i < n; i++ {
    arr[i] = i
  }
  go permutations(0, arr, comm)
  return comm
}

func permutations(pos int, arr []int, comm chan []int) {
  if pos == len(arr) {
    result := make([]int, len(arr))
    copy(result, arr)
    comm <- result
    return
  }
  for i := pos; i < len(arr); i++ {
    tmp := arr[pos]
    arr[pos] = arr[i]
    arr[i] = tmp
    permutations(pos + 1, arr, comm)
    arr[i] = arr[pos]
    arr[pos] = tmp
  }
  if pos == 0 {
    close(comm)
  }
}

// -------------------------------------------
// Iterator for subsets

// Returns all subsets of the n numbers [0...n-1]
func Subsets(n int) (comm chan []int) {
  comm = make(chan []int)
  go subsets(0, n, []int{}, comm)
  return comm
}

func subsets(n int, max int, arr []int, comm chan []int) {
  if n == max {
    comm <- arr
    return
  }
  subsets(n + 1, max, arr, comm)
  arr2 := make([]int, len(arr) + 1)
  copy(arr2, arr)
  arr2[len(arr)] = n
  subsets(n + 1, max, arr2, comm)
  if n == 0 {
    close(comm)
  }
}

// -------------------------------------------
// Random operation generator

// n is the length of the document
func RandomOperations(n int) (ops []Operation) {
  i := 0
  for i <= n {
    r := rand.Float64()
    if r < 0.1 { // Insert a tomb?
      l := rand.Intn(3) + 1
      if len(ops) == 0 || ops[len(ops) - 1].Kind != InsertOp || len(ops[len(ops) - 1].Value.(string)) > 0 {
        ops = append(ops, Operation{Kind:InsertOp, Len: l, Value: ""})
      }
    } else if r < 0.3 { // Insert characters?
      data := fmt.Sprintf("_%v_", rand.Intn(100))
      if len(ops) == 0 || ops[len(ops) - 1].Kind != InsertOp || len(ops[len(ops) - 1].Value.(string)) == 0 {
        ops = append(ops, Operation{Kind:InsertOp, Len: len(data), Value: data})
      }
    }
    if i == n { // Allowed inserts at the end, but now it is time to quit the for loop
      break
    }
    incr := rand.Intn(n - i - 1) + 1
    r = rand.Float64()
    if r < 0.6 { // Skip?
      if len(ops) > 0 && ops[len(ops) - 1].Kind == SkipOp {
	ops[len(ops) - 1].Len += incr
      } else {
        ops = append(ops, Operation{Kind:SkipOp, Len: incr})
      }
    } else { // Delete
      if len(ops) > 0 && ops[len(ops) - 1].Kind == DeleteOp {
	ops[len(ops) - 1].Len += incr
      } else {
        ops = append(ops, Operation{Kind:DeleteOp, Len: incr})
      }
    }
    i += incr
  }
  return
}

// -------------------------------------------------
// Tests

func TestPruning(t *testing.T) {
  // Try many random operations
  for test := 0; test < 10000; test++ {
    // t.Logf("%v x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x-x\n", test)
    original := "abcdefghijk"
    all := []Mutation{}
    // Create concurrent mutations
    for i := 0; i < 4; i++ {
      name := fmt.Sprintf("m%v", i)
      all = append(all, Mutation{DebugName: name, ID: hexHash(name), Operation: Operation{Kind: StringOp, Operations: RandomOperations(len(original))}})
    }

    // Transform the mutations against each other
    seq := []Mutation{}
    for i := 0; i < len(all); i++ {
      _, x, err := TransformSeq(seq, all[i])
      if err != nil {
	t.Fatalf("ERR: %v, i=%v\n", err.String(), i)
      }      
      seq = append( seq, x )
    }
    
    // Transform the mutations but this time skip a subset of mutations
    for subset := range Subsets(len(seq)) {
      seq2 := []Mutation{}
      for i := 0; i < len(all); i++ {
	skip := false
	for j := 0; j < len(subset); j++ {
	  if subset[j] == i {
	    skip = true
	    continue
	  }
	}
	if skip {
	  continue
	}
	_, x, err := TransformSeq(seq2, all[i])
	if err != nil {
	  t.Fatal(err.String())
	}
	seq2 = append( seq2, x )
      }
      
      // println("------------- computing undo")
      // Undo mutation k in seq
      var undolist map[string]bool = make(map[string]bool)
      for j := 0; j < len(subset); j++ {
        undolist[all[subset[j]].ID] = true
      }
      seq3, err := PruneMutationSeq(seq, undolist)
      if err != nil {
	t.Fatalf("ERR: %v\n", err.String())
      }
      
      // println("------------- done undo")
      // fmt.Printf("SEQ: %v\nSEQ2: %v\nSEQ3: %v\nALL: %v\n", seq, seq2, seq3, all)
      // Check that seq2 and seq3 both generate the same document
      doc2 := NewSimpleText(original)
      for _, mut := range seq2 {
	result, err := Execute(doc2, mut)
	if err != nil {
	  t.Fatal(err.String())
	}
	doc2 = result.(*SimpleText)
      }

      doc3 := NewSimpleText(original)
      for _, mut := range seq3 {
	result, err := Execute(doc3, mut)
	if err != nil {
	  t.Fatal(err.String())
	}
	doc3 = result.(*SimpleText)
      }

      if doc2.Text != doc3.Text {
	t.Fatalf("Undo delivers different docs:\n\tdoc1: %v\n\tdoc2: %v\n", doc2.Text, doc3.Text)
      }
      // t.Logf("UNDO2 doc: %v\n", doc2.Text)
    }
  }
}

func TestPruningAndComposing(t *testing.T) {
  // Try many random operations
  for test := 0; test < 10000; test++ {
    original := "abcdefghijk"
    all := []Mutation{}
    // Create concurrent mutations
    for i := 0; i < 4; i++ {
      name := fmt.Sprintf("m%v", i)
      all = append(all, Mutation{DebugName: name, ID: hexHash(name), Operation: Operation{Kind: StringOp, Operations: RandomOperations(len(original))}})
    }

    // Transform the mutations against each other
    seq := []Mutation{}
    for i := 0; i < len(all); i++ {
      _, x, err := TransformSeq(seq, all[i])
      if err != nil {
	t.Fatalf("ERR: %v, i=%v\n", err.String(), i)
      }      
      seq = append( seq, x )
    }
    
    // Transform the mutations but this time skip mutation k
    for k := 0; k < len(all); k++ {
      seq2 := []Mutation{}
      for i := 0; i < len(all); i++ {
	if i == k {
	  continue
	}
	_, x, err := TransformSeq(seq2, all[i])
	if err != nil {
	  t.Fatal(err.String())
	}
	seq2 = append( seq2, x )
      }
      
      // Undo mutation k in seq
      seq3, err := PruneMutationSeq(seq, map[string]bool{seq[k].ID: true})
      if err != nil {
	t.Fatalf("ERR: %v, k=%v", err.String(), k)
      }
      
      // Check that seq2 and seq3 both generate the same document
      doc2 := NewSimpleText(original)
      for _, mut := range seq2 {
	result, err := Execute(doc2, mut)
	if err != nil {
	  t.Fatal(err.String())
	}
	doc2 = result.(*SimpleText)
      }

      doc3 := NewSimpleText(original)
      for _, mut := range seq3 {
	result, err := Execute(doc3, mut)
	if err != nil {
	  t.Fatal(err.String())
	}
	doc3 = result.(*SimpleText)
      }

      if doc2.Text != doc3.Text {
	t.Fatalf("Undo delivers different docs:\n\tdoc1: %v\n\tdoc2: %v\n", doc2.Text, doc3.Text)
      }
      
      _, x, err := TransformSeq(seq2, all[k])
      if err != nil {
	t.Fatal(err.String())
      }
      seq2 = append(seq2, x)
      
      _, x, err = TransformSeq(seq3, all[k])
      if err != nil {
	t.Fatal(err.String())
      }
      seq3 = append(seq3, x)
      
      result, err := Execute(doc2, seq2[len(seq2)-1])
      if err != nil {
	t.Fatal(err.String())
      }
      doc2 = result.(*SimpleText)
      
      result, err = Execute(doc3, seq3[len(seq3)-1])
      if err != nil {
	t.Fatal(err.String())
      }
      doc3 = result.(*SimpleText)

      if doc2.Text != doc3.Text {
	t.Fatal(fmt.Sprintf("Undo delivers different docs after all applications:\n\tdoc1: %v\n\tdoc2: %v\n", doc2.Text, doc3.Text))
      }

      comp1, err := ComposeSeq(seq)
      if err != nil {
	t.Fatal(err.String())
      }

      comp2, err := ComposeSeq(seq2)
      if err != nil {
	t.Fatal(err.String())
      }
      
      comp3, err := ComposeSeq(seq3)
      if err != nil {
	t.Fatal(err.String())
      }

      cdoc1 := NewSimpleText(original)
      result, err = Execute(cdoc1, comp1)
      if err != nil {
	t.Fatal(err.String())
      }
      cdoc1 = result.(*SimpleText)

      cdoc2 := NewSimpleText(original)
      result, err = Execute(cdoc2, comp2)
      if err != nil {
	t.Fatal(err.String())
      }
      cdoc2 = result.(*SimpleText)
      
      cdoc3 := NewSimpleText(original)
      result, err = Execute(cdoc3, comp3)
      if err != nil {
	t.Fatal(err.String())
      }
      cdoc3 = result.(*SimpleText)

      if cdoc1.Text != cdoc2.Text {
	t.Fatal("cdoc1 != cdoc2")
      }

      if cdoc2.Text != cdoc3.Text {
	t.Fatal("cdoc2 != cdoc3")
      }
    }
  }
}

func TestTombStream(t *testing.T) {
  var v vec.IntVector
  ts := NewTombStream(&v)
  ts.InsertChars(7)
  if v.Len() != 1 || v.At(0) != 7 {
    t.Fatal("Error in TombStream 1")
  }
  
  ts = NewTombStream(&v)
  ts.InsertChars(1)
  chars, err1 := ts.Skip(2)
  if err1 != nil {
    t.Fatal("TombStream reached end too soon")
  }
  if chars != 2 {
    t.Fatal("Invalid number of chars")
  }
  ts.InsertChars(1)
  chars, err1 = ts.Skip(5)
  if err1 != nil {
    t.Fatal("TombStream reached end too soon")
  }
  if chars != 5 {
    t.Fatal("Invalid number of chars")
  }
  ts.InsertChars(1)
  _, err1 = ts.Skip(1)
  if err1 == nil {
    t.Fatal("TombStream did not detect end")
  }
  if v.Len() != 1 || v.At(0) != 10 {
    t.Fatal("Error in TombStream 2")
  }
  
  ts = NewTombStream(&v)
  chars, _ = ts.Skip(3)
  if chars != 3 {
    t.Fatal("Invalid number of chars")
  }
  n, err1 := ts.Bury(2)
  if err1 != nil || n != 2 {
    t.Fatal("Cannot bury")
  }
  if v.Len() != 3 || v.At(0) != 3 || v.At(1) != -2 || v.At(2) != 5 {
    t.Fatal("Error in TombStream 3")
  }
  
  ts = NewTombStream(&v)
  chars, _ = ts.Skip(5)
  if chars != 3 {
    t.Fatal("Invalid number of chars")
  }
  n, err2 := ts.Bury(2)
  if err2 != nil || n != 2 {
    t.Fatal("Cannot bury")
  }
  if v.Len() != 3 || v.At(0) != 3 || v.At(1) != -4 || v.At(2) != 3 {
    t.Fatal("Error in TombStream 4")
  }

  ts = NewTombStream(&v)
  chars, _ = ts.Skip(2)
  if chars != 2 {
    t.Fatal("Invalid number of chars")
  }
  n, err3 := ts.Bury(1)
  if err3 != nil || n != 1 {
    t.Fatal("Cannot bury")
  }
  if v.Len() != 3 || v.At(0) != 2 || v.At(1) != -5 || v.At(2) != 3 {
    t.Fatal("Error in TombStream 5")
  }

  ts = NewTombStream(&v)
  chars, _ = ts.Skip(1)
  if chars != 1 {
    t.Fatal("Invalid number of chars")
  }
  n, err4 := ts.Bury(7)
  if err4 != nil || n != 2 {
    t.Fatal("Cannot bury")
  }
  chars, _ = ts.Skip(2)
  if chars != 2 {
    t.Fatal("Invalid number of chars")
  }
  if v.Len() != 3 || v.At(0) != 1 || v.At(1) != -7 || v.At(2) != 2 {
    t.Fatal("Error in TombStream 6")
  }

  ts = NewTombStream(&v)
  chars, _ = ts.Skip(9)
  if chars != 2 {
    t.Fatal("Invalid number of chars")
  }
  ts.InsertTombs(3)  // Middle of char
  chars, _ = ts.Skip(1)
  if chars != 1 {
    t.Fatal("Invalid number of chars")
  }
  if v.Len() != 5 || v.At(0) != 1 || v.At(1) != -7 || v.At(2) != 1 || v.At(3) != -3 || v.At(4) != 1 {
    t.Fatal("Error in TombStream 7")
  }

  ts = NewTombStream(&v)
  ts.InsertTombs(2)  // Beginning of seq
  if v.Len() != 6 || v.At(0) != -2 || v.At(1) != 1 || v.At(2) != -7 || v.At(3) != 1 || v.At(4) != -3 || v.At(5) != 1 {
    t.Fatal("Error in TombStream 8")
  }

  ts = NewTombStream(&v)
  chars, _ = ts.Skip(15)
  if chars != 3 {
    t.Fatal("Invalid number of chars")
  }
  ts.InsertTombs(1) // End of seq
  if v.Len() != 7 || v.At(0) != -2 || v.At(1) != 1 || v.At(2) != -7 || v.At(3) != 1 || v.At(4) != -3 || v.At(5) != 1 || v.At(6) != -1 {
    t.Fatal("Error in TombStream 9")
  }

  ts = NewTombStream(&v)
  chars, _ = ts.Skip(2)
  if chars != 0 {
    t.Fatal("Invalid number of chars")
  }
  ts.InsertTombs(1) // End of tomb
  if v.Len() != 7 || v.At(0) != -3 || v.At(1) != 1 || v.At(2) != -7 || v.At(3) != 1 || v.At(4) != -3 || v.At(5) != 1 || v.At(6) != -1 {
    t.Fatal("Error in TombStream 10")
  }

  ts = NewTombStream(&v)
  chars, _ = ts.Skip(4)
  if chars != 1 {
    t.Fatal("Invalid number of chars")
  }
  ts.InsertTombs(1) // Beginning of tomb
  if v.Len() != 7 || v.At(0) != -3 || v.At(1) != 1 || v.At(2) != -8 || v.At(3) != 1 || v.At(4) != -3 || v.At(5) != 1 || v.At(6) != -1 {
    t.Fatal("Error in TombStream 11")
  }

  ts = NewTombStream(&v)
  chars, _ = ts.Skip(1)
  if chars != 0 {
    t.Fatal("Invalid number of chars")
  }
  ts.InsertTombs(1) // Midle of tomb
  if v.Len() != 7 || v.At(0) != -4 || v.At(1) != 1 || v.At(2) != -8 || v.At(3) != 1 || v.At(4) != -3 || v.At(5) != 1 || v.At(6) != -1 {
    t.Fatal("Error in TombStream 11")
  }
}

func TestTransform(t *testing.T) {
  // Try many random operations
  for test := 0; test < 10000; test++ {
    original := "abcdefghijk"
    all := []Mutation{}
    // Create concurrent mutations
    for i := 0; i < 4; i++ {
      all = append(all, Mutation{ID: fmt.Sprintf("m%v", i), Operation: Operation{Kind: StringOp, Operations: RandomOperations(len(original))}})
    }

    // Execute the four operations in any possible order
    counter := 0
    prev := ""
    for perm := range Permutations(len(all)) {
      doc := NewSimpleText(original)
      var err os.Error
      var applied []Mutation
      for i := 0; i < len(perm); i++ {
	mut := all[perm[i]]
	// Transform against all applied ops
	for _, appliedmut := range applied {
	  _, mut, err = Transform(appliedmut, mut)
          if err != nil {
	    t.Fatal(err.String())
          }
	}
	applied = append(applied, mut)
	var result interface{}
	result, err = Execute(doc, mut)
	doc = result.(*SimpleText)
      }
      if counter == 0 {
	prev = doc.Text
      } else if prev != doc.Text {
	t.Fatal("Different docs")
      }
      counter++
    }
  }
}
