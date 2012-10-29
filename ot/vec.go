package ot

type IntVector []int

func (s *IntVector) Len() int {
  return len(*s)
}

func (s *IntVector) Push(val int) {
  *s = append(*s, val)
}

func (s *IntVector) At(i int) int {
  return (*s)[i]
}

func (s *IntVector) Set(i int, v int) {
  (*s)[i] = v
}

func (s *IntVector) Insert(i int, v int) {
  *s = append((*s)[:i], append([]int{v}, (*s)[i:]...)...)
}

func (s *IntVector) Delete(i int) {
  *s = append((*s)[:i], (*s)[i+1:]...)
}

func (s *IntVector) Copy() (b []int) {
  b = make([]int, len(*s))
  copy(b, *s)
  return
}

type Vector []interface{}

func (s *Vector) Insert(i int, v interface{}) {
  *s = append((*s)[:i], append([]interface{}{v}, (*s)[i:]...)...)
}

func (s *Vector) Cut(i, j int) {
  *s = append((*s)[:i], (*s)[j:]...)
}


