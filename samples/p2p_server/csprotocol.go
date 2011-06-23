package main

import (
  . "lightwaveot"
  . "lightwavestore"
  "log"
  "net"
  "net/textproto"
  "bufio"
)

type CSProtocol struct {
  store BlobStore
  indexer *Indexer
  laddr string
  conns map[int]*csconn
  connCounter int

}

type csconn struct {
  connection net.Conn
  sendChan chan []byte
  ID int
}

func NewCSProtocol(store BlobStore, indexer *Indexer, laddr string) *CSProtocol {
  cs := &CSProtocol{store:store, indexer: indexer, laddr: laddr, conns: make(map[int]*csconn)}
  indexer.AddListener(cs)
  return cs
}

func (self *CSProtocol) Listen() {
  l, err := net.Listen("tcp", self.laddr)
  if err != nil {
    return
  }
  for {
    c, err := l.Accept()
    if err != nil {
      log.Printf("ACCEPT: %v", err)
      continue
    }
    self.newConn(c)
  }
}

func (self *CSProtocol) newConn(c net.Conn) {
  x := &csconn{c, make(chan []byte, 1000), self.connCounter}
  // TODO: Use a mutex
  self.conns[self.connCounter] = x
  self.connCounter++
  go self.read(x)
  go self.write(x)
}

func (self *CSProtocol) read(c *csconn) {
  r := textproto.NewReader(bufio.NewReader(c.connection))
  for {
    blob, err := r.ReadLineBytes()
    if err != nil {
      log.Printf("CS-READ: %v\n", err)
      self.closeConn(c)
      return
    }
    mut, err := DecodeMutation(blob)
    if err != nil {
      log.Printf("CS-DECODE ERROR: %v\n", err)
      self.closeConn(c)
      return
    }      
    err = self.indexer.HandleClientMutation(mut)
    if err != nil {
      log.Printf("CS-APPLY: %v\n", err)
      self.closeConn(c)
      return
    }
  }
}

func (self *CSProtocol) write(c *csconn) {
  // First, send everything that has been applied by the indexer
  for mut := range self.indexer.History(false) {
    blob, _, err := EncodeMutation(mut,EncExcludeDependencies)
    if err != nil {
      panic("FAILED encoding a mutation")
    }
    blob = append(blob, 10)
    n, err := c.connection.Write(blob)
    if err != nil || n != len(blob) {
      self.closeConn(c)
      continue
    }
  }
  // Wait for further messages and send them
  for data := range c.sendChan {
    data = append(data, 10)
    n, err := c.connection.Write(data)
    if err != nil || n != len(data) {
      self.closeConn(c)
      continue
    }
  }
}

func (self *CSProtocol) closeConn(c *csconn) {
  c.connection.Close()
  // TODO: Use a mutex
  self.conns[c.ID] = nil, false
}

func (self *CSProtocol) HandleMutation(mut Mutation) {
  blob, _, err := EncodeMutation(mut, EncExcludeDependencies)
  if err != nil {
    panic("FAILED encoding a mutation")
  }
  for _, conn := range self.conns {
    conn.sendChan <- blob
  }
}
