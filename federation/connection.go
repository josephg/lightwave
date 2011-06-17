package lightwavefed

import (
  "net"
  "log"
  "json"
  "os"
  "sync"
)

type Connection struct {
  fed *Federation
  conn net.Conn
  enc *json.Encoder
  dec *json.Decoder
  id int
  mutex sync.Mutex
  respHandler map[int]ResponseHandler 
}

func newConnection(conn net.Conn, fed *Federation) *Connection {
  c := &Connection{conn: conn, fed: fed, respHandler: make(map[int]ResponseHandler), enc: json.NewEncoder(conn), dec: json.NewDecoder(conn)}
  go c.read()
  return c
}

func Dial(raddr string, fed *Federation) (conn *Connection, err os.Error) {
  c, err := net.Dial("tcp", raddr)
  if err != nil {
    return nil, err
  }
  return newConnection(c, fed), nil
}

func Listen(addr string, fed *Federation) (err os.Error) {
  l, err := net.Listen("tcp", addr)
  if err != nil {
    return
  }
  for {
    c, err := l.Accept()
    if err != nil {
      log.Printf("ERR ACCEPT: %v", err)
      continue
    }
    newConnection(c, fed)
  }
  return
}

// Sends a message that does not require a response
func (self *Connection) SendAsync(msg *Message) os.Error {
  if msg.Cmd == "" {
    return os.NewError("Must specify a cmd")
  }
  if msg.Status != 0 {
    return os.NewError("Must not specify a status in a request")
  }
  self.mutex.Lock()
  defer self.mutex.Unlock()
  self.id++
  msg.ID = self.id
  return self.enc.Encode(msg)
}

type ResponseHandler func(response *Message)

func (self *Connection) SendRequestAsync(msg *Message, handler ResponseHandler) os.Error {
  if msg.Cmd == "" {
    return os.NewError("Must specify a cmd in a request")
  }
  if msg.Status != 0 {
    return os.NewError("Must not specify a status in a request")
  }
  self.mutex.Lock()
  defer self.mutex.Unlock()
  self.id++
  msg.ID = self.id
  self.respHandler[msg.ID] = handler
  return self.enc.Encode(msg)
}

// Sends a message with the 'cmd' field set to the remote computer.
// The parameter 'data' specifies a data structure that is marshaled (to JSON) and sent as
// the payload of the message. Passing nil means that no payload is sent.
// If the response message carries a payload, it is unmarshalled into the data structure provided by 'resp'. Passing nil means that any payload being returned is ignored.
// The function returns the status code it receives in the response message or an error code
// if sending or receiving failed locally.
// The return value 'err' is !=nil if sending, receiving or marshalling/unmarshalling fails.
func (self *Connection) SendRequest(cmd string, data interface{}, resp interface{}) (status int, err os.Error) {
  // Create a channel to be notified when the response comes int
  ch := make(chan *Message)
  handler := func(r *Message) {
    ch <- r
    close(ch)
  }
  // Build the message
  msg := &Message{Cmd: cmd}
  // if data != nil {
    bytes, err := json.Marshal(data)
    if err != nil {
      return 0, err
    }
    p := json.RawMessage(bytes)
    msg.Payload = &p
  // }
    
  // Send the message
  err = self.SendRequestAsync(msg, handler)
  if err != nil {
    return
  }
  // Wait for the response
  r := <-ch
  if resp != nil && r.Payload != nil && r.Status == 200 {
    err = json.Unmarshal(*r.Payload, resp)
  }
  return r.Status, err
}

func (self *Connection) sendResponse(response *Message) os.Error {
  if response.Cmd != "" {
    return os.NewError("Must not specify a cmd in a response")
  }
  if response.Status <= 0 {
    return os.NewError("Must specify a status >0 in a response")
  }
  self.mutex.Lock()
  defer self.mutex.Unlock()
  return self.enc.Encode(response)
}


func (self *Connection) read() {
  for {
    var msg Message
    err := self.dec.Decode(&msg)
    if err != nil {
      log.Printf("ERR READ JSON: %v\n", err)
      self.Close()
      return
    }
    if msg.Status != 0 && msg.ID != 0 { // This is a response
      self.mutex.Lock()
      if handler, ok := self.respHandler[msg.ID]; ok {
	self.respHandler[msg.ID] = nil, false
	self.mutex.Unlock()
	handler(&msg)
      } else {
	self.mutex.Unlock()
      }
    } else if msg.Cmd != "" { // This is a request
      status := 404
      var data interface{}
      self.mutex.Lock()      
      handler := self.fed.Handler(msg.Cmd)
      self.mutex.Unlock()      
      if handler != nil {
	status, data = handler(&msg)
      }
      // log.Printf("Sending %v %v\n", status, data)
      var msg2 Message
      msg2.ID = msg.ID
      msg2.Status = status
      bytes, err := json.Marshal(data)
      if err != nil {
	panic("Failed to marshal response")
      }
      p := json.RawMessage(bytes)
      msg2.Payload = &p
      self.sendResponse(&msg2)
    }
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
}
