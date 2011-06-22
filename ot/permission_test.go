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