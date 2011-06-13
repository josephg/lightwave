package main

import (
  "net"
  "bufio"
  "net/textproto"
  "log"
  "os"
)

type Federation struct {
  laddr string
  store *Store
  conns map[int]*conn
  connCounter int
  // For debugging only
  suspend bool
  queue [][]byte
  // A queue of blob refs
  refqueue []string
}

type conn struct {
  connection net.Conn
  sendChan chan []byte
  ID int
}

func NewFederation(laddr string, store *Store) (network *Federation) {
  network = &Federation{laddr: laddr, store: store, conns: make(map[int]*conn)}
  return
}

func (self *Federation) Listen() {
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

func (self *Federation) Dial(laddr string) (err os.Error) {
  c, err := net.Dial("tcp", laddr)
  if err != nil {
    return err
  }
  self.newConn(c)
  return nil
}

func (self *Federation) newConn(c net.Conn) {
  x := &conn{c, make(chan []byte, 1000), self.connCounter}
  // TODO: Use a mutex
  self.conns[self.connCounter] = x
  self.connCounter++
  go self.read(x)
  go self.write(x)
}

func (self *Federation) ForwardBlob(blob []byte, blobref string) {
  // DEBUG
  if self.suspend {
    self.queue = append(self.queue, blob)
    self.refqueue = append(self.refqueue, blobref)
    return
  }
  // END DEBUG
  for _, conn := range self.conns {
    conn.sendChan <- blob
  }
}

// DEBUG
func (self *Federation) Suspend() {
  self.suspend = true
}

// DEBUG
func (self *Federation) Resume() {
  if !self.suspend {
    return
  }
  self.suspend = false
  i := 0
  for _, blob := range self.queue {
    self.ForwardBlob(blob, self.refqueue[i])
    i++
  }
  self.queue = nil
  self.refqueue = nil
}

func (self *Federation) read(c *conn) {
  r := textproto.NewReader(bufio.NewReader(c.connection))
  for {
    blob, err := r.ReadLineBytes()
    if err != nil {
      log.Printf("READ: %v\n", err)
      self.closeConn(c)
      return
    }
//    log.Printf("read blob\n")
    self.store.StoreBlob(blob, "")
  }
}

func (self *Federation) write(c *conn) {
  // First, send everything that is in the store
  for _, blob := range self.store.Enumerate() {
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

func (self *Federation) closeConn(c *conn) {
  c.connection.Close()
  // TODO: Use a mutex
  self.conns[c.ID] = nil, false
}
