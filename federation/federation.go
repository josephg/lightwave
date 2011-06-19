package lightwavefed

import (
  "os"
  "sync"
  "json"
  "net"
  "log"
)

const (
  connClient = 1
  connServer = 2
  connStreaming = 4
)

type Federation struct {
  identity string
  mutex sync.Mutex
  connections map[*Connection]int
  store BlobStore
  ns NameService
}

type BlobStore interface {
  StoreBlob(blob []byte, blobref string)
  AddListener(listener BlobStoreListener)
  HashTree() HashTree
  GetBlob(blobref string) (blob []byte, err os.Error)
  GetBlobs(prefix string) (channel <-chan Blob, err os.Error)
}

type BlobStoreListener interface {
  HandleBlob(blob []byte, blobref string)
}

type Blob struct {
  Data []byte
  BlobRef string
}

type NameService interface {
  // Returns a string of the form "hostname:port", "Ip-address:port" or ":port".
  // Real life applications will use DNS A-records + default ports or DNS SRV-records
  // to perform the lookup. For demos we can hardcode it.
  Lookup(identity string) (addr string, err os.Error)
}

func NewFederation(identity string, ns NameService, store BlobStore) *Federation {
  fed := &Federation{store: store, connections: make(map[*Connection]int), identity: identity, ns: ns}
  store.AddListener(fed)
  return fed
}

func (self *Federation) registerConnection(conn *Connection, kind int) {
  self.mutex.Lock()
  self.connections[conn] = kind
  self.mutex.Unlock()
}

func (self *Federation) unregisterConnection(conn *Connection) {
  self.mutex.Lock()
  self.connections[conn] = 0, false
  self.mutex.Unlock()
}

func (self *Federation) Listen() (err os.Error) {
  addr, err := self.ns.Lookup(self.identity)
  if err != nil {
    log.Printf("Failed to lookup my own address")
    return err
  }
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
    conn := newConnection(c, self)
    self.registerConnection(conn, connServer)
    conn.Send("HELO", self.identity)
    // This tells the other side to start sending BLOBs as they come in
    conn.Send("OPEN", nil)
  }
  return
}

// Creates a connection to another peer
func (self *Federation) Dial(identity string) (err os.Error) {
  raddr, err := self.ns.Lookup(identity)
  if err != nil {
    return err
  }
  c, err := net.Dial("tcp", raddr)
  if err != nil {
    return err
  }
  conn := newConnection(c, self)
  self.registerConnection(conn, connClient)
  conn.Send("HELO", self.identity)
  // This tells the other side to start sending BLOBs as they come in
  conn.Send("OPEN", nil)
  // This initiates the syncing
  conn.Send("THASH", nil)
  return
}

// Called from the store when a new blob has been stored
func (self *Federation) HandleBlob(blob []byte, blobref string) {
  for connection, flags := range self.connections {
    if flags & connStreaming == connStreaming {
      connection.Send("BLOB", json.RawMessage(blob))
    }
  }
}

func (self *Federation) HandleMessage(msg Message) {
  if msg.Cmd != "HELO" && msg.connection.identity == "" {
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
func (self *Federation) heloHandler(msg Message) {
  if msg.connection.identity != "" {
    log.Printf("Error: Second HELO is being sent")
  }
  var identity string
  if msg.DecodePayload(&identity) != nil {
    log.Printf("Error in HELO request")
    return
  }
  msg.connection.identity = identity
}

// Handles the 'BLOB' command
func (self *Federation) blobHandler(msg Message) {
  if msg.Payload == nil {
    log.Printf("Blob message without payload detected")
    return
  }
  self.store.StoreBlob([]byte(*msg.Payload), "")
}

// Handles the 'OPEN' command
func (self *Federation) openHandler(msg Message) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  i, ok := self.connections[msg.connection]
  if !ok {
    panic("Unknown connection")
  }
  self.connections[msg.connection] = i | connStreaming
}

// Handles the 'CLOSE' command
func (self *Federation) closeHandler(msg Message) {
  self.mutex.Lock()
  defer self.mutex.Unlock()
  i, ok := self.connections[msg.connection]
  if !ok {
    panic("Unknown connection")
  }
  self.connections[msg.connection] = i &^ connStreaming
}

// Handles the 'GET' command
func (self *Federation) getHandler(msg Message) {
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
func (self *Federation) getnHandler(msg Message) {
  var prefix string
  if msg.DecodePayload(&prefix) != nil {
    log.Printf("Error in GETN request")
    return
  }
  self.getnHandlerIntern(prefix, msg.connection)
}

func (self *Federation) getnHandlerIntern(prefix string, conn *Connection) {
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
func (self *Federation) getnxHandler(msg Message) {
  query := &struct{Prefix string "prefix"; Except []string "except"}{}
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

func (self *Federation) getnxHandlerIntern(prefix string, except map[string]bool, conn *Connection) {
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
func (self *Federation) treeHashHandler(msg Message) {
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
func (self *Federation) treeHashChildrenHandler(msg Message) {
  req := struct{Prefix string "prefix"; Kind int "kind"; Children []string "chld"}{}
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
    for i := 0; i < hashTreeNodeDegree; i++ {
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
    for i := 0; i < hashTreeNodeDegree; i++ {
      // Get all blox with this prefix (except those in map1) from the other side
      p := prefix + string(hextable[i])
      lst := []string{}
      for key, _ := range map1 {
	lst = append(lst, key)
      }
      r := struct{Prefix string "prefix"; Except []string "except"}{p, lst}
      msg.connection.Send("GETNX", &r)
    }
  } else {
    for i := 0; i < hashTreeNodeDegree; i++ {
      // Send all blobs with this prefix (except those in map2) to the other side
      p := prefix + string(hextable[i])
      self.getnxHandlerIntern(p, map2, msg.connection)
    }  
  }
}
