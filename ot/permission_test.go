package lightwaveot

import (
  "testing"
)
  
func TestPermission(t *testing.T) {
  allow1 := []int{1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0}
  deny1  := []int{1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0}
  allow2 := []int{1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0}
  deny2  := []int{1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0}
  allow3 := []int{0, -1, 1, -2, -1, -1, -1, -1, 0, -1, 0, 0, -2, -1, 0, 0}
  deny3  := []int{1, -1, 1, -2, -1, -1, -1, -1, 1, -1, 1, 1, -2, -1, 0, 1}
  
  for i := 0; i < 16; i++ {
    p1 := Permission{Allow:allow1[i], Deny:0xffffffe | deny1[i]}
    p2 := Permission{Allow:allow2[i], Deny:0xffffffe | deny2[i]}
    p3, _, err := TransformPermission(p1, p2)
    if allow3[i] < 0 {
      if err == nil {
	t.Fatal("Expected transformation error")
      }
    } else if err != nil {
      t.Fatalf("%v: %v\n", i, err.String())
    } else {
      if p3.Allow != allow3[i] || p3.Deny != (0xffffffe | deny3[i]) {
	t.Fatalf("Wrong result %v: %v %v %v\n", i, p1, p2, p3)
      }
    }
    _, p3, err = TransformPermission(p2, p1)
    if allow3[i] < 0 {
      if err == nil {
	t.Fatal("Expected transformation error")
      }
    } else if err != nil {
      t.Fatalf("%v: %v\n", i, err.String())
    } else {
      if p3.Allow != allow3[i] || p3.Deny != (0xffffffe | deny3[i]) {
	t.Fatalf("Wrong result2 %v: %v %v %v\n", i, p1, p2, p3)
      }
    }
  }
}

func TestPermissionCompose(t *testing.T) {
  allow1 := []int{0, 0, 1, 0, 1, 0, 0}
  deny1  := []int{1, 0, 1, 0, 1, 1, 1}
  allow2 := []int{0, 1, 0, 0, 0, 1, 0}
  deny2  := []int{1, 1, 0, 1, 1, 1, 0}
  allow3 := []int{0, 0, 0, 0, 1, 1, 0}
  deny3  := []int{1, 1, 1, 0, 1, 1, 0}

  for i := 0; i < 7; i++ {
    p1 := Permission{Allow:allow1[i], Deny:0xffffffe | deny1[i]}
    p2 := Permission{Allow:allow2[i], Deny:0xffffffe | deny2[i]}
    p3 := ComposePermission(p1, p2)
    if p3.Allow != allow3[i] || p3.Deny != (0xffffffe | deny3[i]) {
      t.Fatal("Wrong composition result")
    }
  }
}

func TestPermissionPrune(t *testing.T) {
  p1 := Permission{ID:"p1", Allow:1, Deny: ^0}
  p2 := Permission{ID:"p2", Allow:0, Deny: ^1}
  p3 := Permission{ID:"p3", Allow:1, Deny: ^0}
  tp, _, err := TransformPermission(p3, p1)
  if err != nil {
    t.Fatal(err.String())
  }
  tp, _, err = TransformPermission(tp, p2)
  if err != nil {
    t.Fatal(err.String())
  }
  if tp.Allow != 0 || tp.Deny != ^0 {
    t.Fatal("Not the expected transformation before pruning")
  }
  if len(tp.History) != 2 || tp.History[0].id != "p1" || tp.History[1].id != "p2" {
    t.Fatal("History is wrong 1")
  }
  pr, err := PrunePermission(tp, map[string]bool{"p2":true})
  if err != nil {
    t.Fatalf("Prune 1: %v\n", err.String())
  }
  if pr.Allow != 0 || pr.Deny != ^0 {
    t.Fatal("Not the expected result of pruning: 1")
  }
  if tp.OriginalAllow != 1 || tp.OriginalDeny != ^0 {
    t.Fatalf("Original is wrong: %v %v", tp.OriginalAllow, tp.OriginalDeny)
  }
  if len(tp.History) != 2 || tp.History[0].id != "p1" || tp.History[1].id != "p2" {
    t.Fatal("History is wrong 2")
  }
  pr, err = PrunePermission(tp, map[string]bool{"p1":true, "p2":true})
  if err != nil {
    t.Fatalf("Prune 2: %v\n", err.String())
  }
  if pr.Allow != 1 || pr.Deny != ^0 {
    t.Fatalf("Not the expected result of pruning: 2: %x %x", pr.Allow, pr.Deny)
  }
}