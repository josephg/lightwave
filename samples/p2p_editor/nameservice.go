package main

import (
  "os"
)

type dummyNameService struct {
}

func (self *dummyNameService) Lookup(identity string) (addr string, err os.Error) {
  switch identity {
  case "fed1.com":
    return ":8181", nil
  case "fed2.com":
    return ":8282", nil
  case "fed3.com":
    return ":8383", nil
  case "fed4.com":
    return ":8484", nil
  case "fed5.com":
    return ":8585", nil
  }
  return "", os.NewError("Unknown host")
}
