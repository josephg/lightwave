package store

import (
  "encoding/json"
  "log"
  "net"
  "sync"
  "time"
)

const (
  connClient    = 1
  connServer    = 2
  connStreaming = 4
)

const (
  ReconnectDelay = 20
)

type Replication struct {
  userID string
  mutex  sync.Mutex
  // The value in the map denotes the state of the connection as specified
  // by the conn* constants.
  connections map[*Connection]int
  store       BlobStore
  // The empty string or the network address of a master
  masterAddr string
  laddr      string
}

func NewReplication(userID string, store BlobStore, laddr string, masterAddr string) *Replication {
  rep := &Replication{store: store, connections: make(map[*Connection]int), userID: userID, masterAddr: masterAddr, laddr: laddr}
  store.AddListener(rep)
  if masterAddr != "" {
    go rep.dialMaster(masterAddr)
  }
  return rep
}

func (self *Replication) registerConnection(conn *Connection, kind int) {
  self.mutex.Lock()
  self.connections[conn] = kind
  self.mutex.Unlock()
}

func (self *Replication) unregisterConnection(conn *Connection) {
  self.mutex.Lock()
  delete(self.connections, conn)
  self.mutex.Unlock()
}

func (self *Replication) Listen() (err error) {
  l, err := net.Listen("tcp", self.laddr)
  if err != nil {
    return
  }
  for {
    c, err := l.Accept()
    if err != nil {
      log.Printf("ERR ACCEPT: %v", err)
      continue
    }
    conn := newConnection(c, self, nil)
    self.registerConnection(conn, connServer)
    conn.Send("HELO", self.userID)
    // This tells the other side to start sending BLOBs as they come in
    conn.Send("OPEN", nil)
  }
  return
}

// Creates a connection to another peer
func (self *Replication) dialMaster(raddr string) (err error) {
  ch := make(chan error)
  for {
    c, err := net.Dial("tcp", raddr)
    if err != nil {
      log.Printf("Failed connecting to %v, will retry ...\n", raddr)
      time.Sleep(1000000000 * ReconnectDelay)
      continue
    }
    log.Printf("Connection established")
    conn := newConnection(c, self, ch)
    self.registerConnection(conn, connClient)
    conn.Send("HELO", self.userID)
    // This tells the other side to start sending BLOBs as they come in
    conn.Send("OPEN", nil)
    // This initiates the syncing
    conn.Send("THASH", nil)
    // Wait for some error
    <-ch
    log.Printf("Connection is broken. Will retry ...\n")
    time.Sleep(1000000000 * ReconnectDelay)
  }
  return
}

// Called from the store when a new blob has been stored
func (self *Replication) HandleBlob(blob []byte, blobref string) error {
  for connection, flags := range self.connections {
    if flags&connStreaming == connStreaming {
      // Do not send the blob on the same connection on which it has been received
      if !connection.hasReceivedBlob(blobref) {
        connection.Send("BLOB", json.RawMessage(blob))
      }
    }
  }
  return nil
}

func (self *Replication) HandleMessage(msg Message) {
  if msg.Cmd != "HELO" && msg.connection.userID == "" {
    log.Printf("ERR: Missing HELO")
    return
  }
  switch msg.Cmd {
  case "OPEN":
    self.openHandler(msg)
  case "CLOSE":
    self.closeHandler(msg)
  case "THASH":
    self.treeHashHandler(msg)
  case "TCHLD":
    self.treeHashChildrenHandler(msg)
  case "GET":
    self.getHandler(msg)
  case "GETN":
    self.getnHandler(msg)
  case "GETNX":
    self.getnxHandler(msg)
  case "BLOB":
    self.blobHandler(msg)
  case "HELO":
    self.heloHandler(msg)
  default:
    log.Printf("Unknown command: %v\n", msg.Cmd)
    msg.connection.Send("ERR", msg.Cmd)
  }
}

// Handles the 'HELO' command
func (self *Replication) heloHandler(msg Message) {
  if msg.connection.userID != "" {
    log.Printf("Error: Second HELO is being sent")
  }
  var userID string
  if msg.DecodePayload(&userID) != nil {
    log.Printf("Error in HELO request")
    return
  }
  if userID != self.userID {
    log.Printf("Error: syncing replicas owned by multiple users is not allowed")
    msg.connection.Close()
    return
  }
  msg.connection.userID = userID
}

// Handles the 'BLOB' command
func (self *Replication) blobHandler(msg Message) {
  if msg.Payload == nil {
    log.Printf("Blob message without payload detected")
    return
  }
  blob := []byte(*msg.Payload)
  blobref := NewBlobRef(blob)
  msg.connection.addReceivedBlock(blobref)
  self.store.StoreBlob(blob, blobref)
}

// Handles the 'OPEN' command
func (self *Replication) openHandler(msg Message) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  i, ok := self.connections[msg.connection]
  if !ok {
    panic("Unknown connection")
  }
  self.connections[msg.connection] = i | connStreaming
}

// Handles the 'CLOSE' command
func (self *Replication) closeHandler(msg Message) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  i, ok := self.connections[msg.connection]
  if !ok {
    panic("Unknown connection")
  }
  self.connections[msg.connection] = i &^ connStreaming
}

// Handles the 'GET' command
func (self *Replication) getHandler(msg Message) {
  var blobref string
  if msg.DecodePayload(&blobref) != nil {
    log.Printf("Error in GET request")
    return
  }
  blob, err := self.store.GetBlob(blobref)
  if err != nil {
    log.Printf("Error while talking to store: %v\n", err)
    return
  }
  // TODO: If this is not a JSON blob ...
  err = msg.connection.Send("BLOB", json.RawMessage(blob))
  if err != nil {
    log.Printf("Error while sending %v\n", err)
  }
}

// Handles the 'GETN' command
func (self *Replication) getnHandler(msg Message) {
  var prefix string
  if msg.DecodePayload(&prefix) != nil {
    log.Printf("Error in GETN request")
    return
  }
  self.getnHandlerIntern(prefix, msg.connection)
}

func (self *Replication) getnHandlerIntern(prefix string, conn *Connection) {
  channel, err := self.store.GetBlobs(prefix)
  if err != nil {
    log.Printf("Error while talking to store: %v\n", err)
    return
  }
  for blob := range channel {
    log.Printf("sendblob %v\n", blob.BlobRef)
    conn.Send("BLOB", json.RawMessage(blob.Data))
  }
}

// Handles the 'GETNX' command
func (self *Replication) getnxHandler(msg Message) {
  query := &struct {
    Prefix string   "prefix"
    Except []string "except"
  }{}
  if msg.DecodePayload(query) != nil {
    log.Printf("Error in GETNX request")
    return
  }
  except := make(map[string]bool)
  for _, e := range query.Except {
    except[e] = true
  }
  go self.getnxHandlerIntern(query.Prefix, except, msg.connection)
}

func (self *Replication) getnxHandlerIntern(prefix string, except map[string]bool, conn *Connection) {
  channel, err := self.store.GetBlobs(prefix)
  if err != nil {
    log.Printf("Error while talking to store: %v\n", err)
    return
  }
  for blob := range channel {
    if _, ok := except[blob.BlobRef]; ok {
      continue
    }
    conn.Send("BLOB", json.RawMessage(blob.Data))
  }
}

// Handles the 'THASH' command
func (self *Replication) treeHashHandler(msg Message) {
  // This is a request?
  if msg.Payload == nil || len(*msg.Payload) == 0 {
    msg.connection.Send("THASH", self.store.HashTree().Hash())
  } else {
    var hash string
    if msg.DecodePayload(&hash) != nil {
      log.Printf("Error in THASH response")
      return
    }
    // Both computers agree on the root hash? -> Done
    if hash == self.store.HashTree().Hash() {
      return
    }
    msg.connection.Send("TCHLD", "")
  }
}

// Handles the 'TCHLD' command
func (self *Replication) treeHashChildrenHandler(msg Message) {
  req := struct {
    Prefix   string   "prefix"
    Kind     int      "kind"
    Children []string "chld"
  }{}
  var prefix string
  // This is a request?
  if msg.DecodePayload(&prefix) == nil {
    req.Kind, req.Children, _ = self.store.HashTree().Children(prefix)
    req.Prefix = prefix
    msg.connection.Send("TCHLD", &req)
    return
  }
  if msg.DecodePayload(&req) != nil {
    log.Printf("Error in TCHL message")
    return
  }

  kind1, children1, prefix := req.Kind, req.Children, req.Prefix
  kind2, children2, err := self.store.HashTree().Children(prefix)
  if kind1 == HashTree_NIL || kind2 == HashTree_NIL || err != nil {
    log.Printf("Comparison of hash trees failed: prefix=%v, kind1=%v, kind2=%v, err=%v\n", prefix, kind1, kind2, err)
    return
  }

  // Turn a list of strings into a map of strings for further efficient processing
  map1 := map[string]bool{}
  for _, ch := range children1 {
    map1[ch] = true
  }
  map2 := map[string]bool{}
  for _, ch := range children2 {
    map2[ch] = true
  }

  if kind1 == HashTree_IDs && kind2 == HashTree_IDs {
    // Both returned hashes. Compare the two sets of hashes
    for key, _ := range map1 {
      if _, ok := map2[key]; !ok {
        msg.connection.Send("GET", key)
      }
    }
    for key, _ := range map2 {
      if _, ok := map1[key]; !ok {
        blob, err := self.store.GetBlob(key)
        if err != nil {
          log.Printf("Retrieving block %v failed\n", key)
        } else {
          msg.connection.Send("BLOB", json.RawMessage(blob))
        }
      }
    }
  } else if kind1 == HashTree_InnerNodes && kind2 == HashTree_InnerNodes {
    // Both returned subtree nodes? Recursion into the sub tree nodes
    for i := 0; i < HashTree_NodeDegree; i++ {
      if children1[i] == children2[i] {
        continue
      }
      p := prefix + string(hextable[i])
      if children1[i] == "" {
        self.getnHandlerIntern(p, msg.connection)
      } else if children2[i] == "" {
        // Get all blobs with this prefix
        msg.connection.Send("GETN", p)
      } else {
        // Recursion
        msg.connection.Send("TCHLD", p)
      }
    }
  } else if kind1 == HashTree_InnerNodes && kind2 == HashTree_IDs {
    for i := 0; i < HashTree_NodeDegree; i++ {
      // Get all blox with this prefix (except those in map1) from the other side
      p := prefix + string(hextable[i])
      lst := []string{}
      for key, _ := range map1 {
        lst = append(lst, key)
      }
      r := struct {
        Prefix string   "prefix"
        Except []string "except"
      }{p, lst}
      msg.connection.Send("GETNX", &r)
    }
  } else {
    for i := 0; i < HashTree_NodeDegree; i++ {
      // Send all blobs with this prefix (except those in map2) to the other side
      p := prefix + string(hextable[i])
      self.getnxHandlerIntern(p, map2, msg.connection)
    }
  }
}
