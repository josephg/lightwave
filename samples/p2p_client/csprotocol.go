package main

import (
  . "lightwave/ot"
  "log"
  "net"
  "net/textproto"
  "bufio"
)

type CSProtocol struct {
  indexer *Indexer
  laddr string
  conn net.Conn
  sendChan chan []byte
}

func NewCSProtocol(laddr string, indexer *Indexer) *CSProtocol {
  cs := &CSProtocol{laddr: laddr, indexer: indexer, sendChan: make(chan []byte, 1000)}
  return cs
}

func (self *CSProtocol) Dial() (err error) {
  self.conn, err = net.Dial("tcp", self.laddr)
  if err != nil {
    return err
  }
  go self.read()
  go self.write()
  return
}

func (self *CSProtocol) read() {
  r := textproto.NewReader(bufio.NewReader(self.conn))
  for {
    blob, err := r.ReadLineBytes()
    if err != nil {
      log.Printf("CS-READ: %v\n", err)
      self.closeConn()
      return
    }
    mut, err := DecodeMutation(blob)
    if err != nil {
      log.Printf("CS-DECODE ERROR: %v\n", err)
      self.closeConn()
      return
    }      
    err = self.indexer.HandleServerMutation(mut)
    if err != nil {
      log.Printf("CS-APPLY: %v\n", err)
      self.closeConn()
      return
    }
  }
}

func (self *CSProtocol) write() {
  // Wait for further messages and send them
  for data := range self.sendChan {
    data = append(data, 10)
    n, err := self.conn.Write(data)
    if err != nil || n != len(data) {
      self.closeConn()
      continue
    }
  }
}

func (self *CSProtocol) closeConn() {
  self.conn.Close()
  self.conn = nil
}

func (self *CSProtocol) SendMutation(mut Mutation) {
  blob, _, err := EncodeMutation(mut, EncExcludeDependencies)
  if err != nil {
    panic("FAILED encoding a mutation")
  }
  self.sendChan <- blob
}
