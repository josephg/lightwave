package lightwavefed

import (
  "net"
  "log"
  "json"
  "os"
  "sync"
)


type Connection struct {
  // The identity of the remote server
  identity string
  fed *Federation
  conn net.Conn
  enc *json.Encoder
  dec *json.Decoder
  mutex sync.Mutex
}

func newConnection(conn net.Conn, fed *Federation) *Connection {
  c := &Connection{conn: conn, fed: fed, enc: json.NewEncoder(conn), dec: json.NewDecoder(conn)}
  go c.read()
  return c
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
    return self.enc.Encode(m)
  }
  return self.enc.Encode(msg)
}

func (self *Connection) read() {
  for {
    var msg Message
    err := self.dec.Decode(&msg)
    msg.connection = self
    if err != nil {
      log.Printf("ERR READ JSON: %v\n", err)
      self.Close()
      return
    }
    self.fed.HandleMessage(msg)
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
  self.fed.unregisterConnection(self)
}
