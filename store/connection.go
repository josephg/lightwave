package lightwavestore

import (
  "net"
  "log"
  "json"
  "os"
  "sync"
)

type Connection struct {
  // The userID of the remote server
  userID string
  replication *Replication
  conn net.Conn
  enc *json.Encoder
  dec *json.Decoder
  mutex sync.Mutex
  errChannel chan<- os.Error
  receivedBlobs [100]string
  receivedBlobsIndex int
}

func newConnection(conn net.Conn, replication *Replication, errChannel chan<- os.Error) *Connection {
  c := &Connection{conn: conn, replication: replication, enc: json.NewEncoder(conn), dec: json.NewDecoder(conn)}
  go c.read()
  return c
}

func (self *Connection) addReceivedBlock(blobref string) {
  self.receivedBlobs[self.receivedBlobsIndex] = blobref
  self.receivedBlobsIndex = (self.receivedBlobsIndex + 1) % len(self.receivedBlobs)
}

func (self *Connection) hasReceivedBlob(blobref string) bool {
  for _, b := range self.receivedBlobs {
    if b == blobref {
      return true
    }
  }
  return false
}

// Sends a message that does not require a response
func (self *Connection) Send(cmd string, data interface{}) (err os.Error) {
  if cmd == "" {
    return os.NewError("Must specify a cmd")
  }
  var msg Message
  msg.Cmd = cmd
  err = msg.EncodePayload(data)
  if err != nil {
    log.Printf("Encoding failed: %v\n", err)
    return
  }
  self.mutex.Lock()
  defer self.mutex.Unlock()
  if msg.Payload == nil {
    m := messageWithoutPayload{msg.Cmd}
    err = self.enc.Encode(m)
  } else {
    err = self.enc.Encode(msg)
  }
  if err != nil && self.errChannel != nil {
    self.errChannel <- err
  }
  return
}

func (self *Connection) read() {
  for {
    var msg Message
    err := self.dec.Decode(&msg)
    msg.connection = self
    if err != nil {
      log.Printf("ERR READ JSON: %v\n", err)
      if self.errChannel != nil {
	self.errChannel <- err
      }
      self.Close()
      return
    }
    self.replication.HandleMessage(msg)
  }
}

func (self *Connection) Close() {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  if self.conn == nil {
    return
  }
  self.conn.Close()
  self.conn = nil
  self.replication.unregisterConnection(self)
}
