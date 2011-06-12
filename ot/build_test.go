package lightwaveot

import (
  "testing"
  "rand"
)

func TestHistoryGraph(t *testing.T) {
  m7a := Mutation{ID:"m7a"}
  m7b := Mutation{ID:"m7b"}
  m8a := Mutation{ID:"m8a"}
  m8b := Mutation{ID:"m8b"}
  m9a := Mutation{ID:"m9a"}
  m9b := Mutation{ID:"m9b"}
  m7 := Mutation{ID:"m7", Dependencies:[]string{"m7a","m7b"}}
  m8 := Mutation{ID:"m8", Dependencies:[]string{"m8a","m8b"}}
  m9 := Mutation{ID:"m9", Dependencies:[]string{"m9a","m9b"}}
  m11 := Mutation{ID:"m11", Dependencies:[]string{"m9b"}}
  m5 := Mutation{ID:"m5", Dependencies:[]string{"m7","m8"}}
  m6 := Mutation{ID:"m6", Dependencies:[]string{"m8","m9","m11"}}
  m10 := Mutation{ID:"m10", Dependencies:[]string{"m9b"}}
  m1 := Mutation{ID:"m1", Dependencies:[]string{"m5"}}
  m2 := Mutation{ID:"m2", Dependencies:[]string{"m5"}}
  m3 := Mutation{ID:"m3", Dependencies:[]string{"m5", "m6"}}
  m4 := Mutation{ID:"m4", Dependencies:[]string{"m6", "m10"}}
  h := newHistoryGraph(map[string]bool{"m1":false, "m2":false, "m3":false, "m4":false}, []string{"m5","m9"})
  muts := []Mutation{m1, m2, m3, m4, m5, m6, m10, m7, m8, m9, m11, m7a, m7b, m8a, m8b, m9a, m9b}
  rollback := 0
  for i := 0; i < len(muts) && !h.Test(); i++ {
    h.Substitute(muts[i])
    rollback++
  }
  if rollback != 11 {
    t.Fatal("Rollback error")
  }
}

func TestBuild(t *testing.T) {
  m7a := Mutation{ID:"m7a"}
  m7b := Mutation{ID:"m7b"}
  m8a := Mutation{ID:"m8a"}
  m8b := Mutation{ID:"m8b"}
  m9a := Mutation{ID:"m9a"}
  m9b := Mutation{ID:"m9b"}
  m7 := Mutation{ID:"m7", Dependencies:[]string{"m7a","m7b"}}
  m8 := Mutation{ID:"m8", Dependencies:[]string{"m8a","m8b"}}
  m9 := Mutation{ID:"m9", Dependencies:[]string{"m9a","m9b"}}
  m11 := Mutation{ID:"m11", Dependencies:[]string{"m9b"}}
  m5 := Mutation{ID:"m5", Dependencies:[]string{"m7","m8"}}
  m6 := Mutation{ID:"m6", Dependencies:[]string{"m8","m9","m11"}}
  m10 := Mutation{ID:"m10", Dependencies:[]string{"m9b"}}
  m1 := Mutation{ID:"m1", Dependencies:[]string{"m5"}}
  m2 := Mutation{ID:"m2", Dependencies:[]string{"m5"}}
  m3 := Mutation{ID:"m3", Dependencies:[]string{"m5", "m6"}}
  m4 := Mutation{ID:"m4", Dependencies:[]string{"m6", "m10"}}
  muts := []Mutation{m1, m2, m3, m4, m5, m6, m10, m7, m8, m9, m11, m7a, m7b, m8a, m8b, m9a, m9b}
  
  // Try to apply the mutations in all possible permutations
  for i := 0; i < 10000; i++ {
    perm := rand.Perm(len(muts))
    b := NewSimpleBuilder()
    for i := 0; i < len(muts); i++ {
      mut := muts[perm[i]]
      _, err := Build(b, mut)
      if err != nil {
	t.Fatal(err.String())
	return
      }
    }
  
    if len(muts) != len(b.AppliedMutationIDs()) {
      t.Fatal("Not all mutations have been applied")
    }
  }
}
