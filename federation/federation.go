package lightwavefed

import (
  idx "lightwaveidx"
  store "lightwavestore"
  "sync"
  "os"
  "log"
  vec "container/vector"
  "strings"
  "http"
  "io/ioutil"
  "io"
  "bytes"
  "json"
)

const (
  connClient = 1
  connServer = 2
  connStreaming = 4
)

type Federation struct {
  addr string
  mutex sync.Mutex
  store store.BlobStore
  ns store.NameService
  indexer *idx.Indexer
  queues map[string]*queue
}

type queueEntry struct {
  users []string
  blobref string
}

func NewFederation(addr string, ns store.NameService, store store.BlobStore, indexer *idx.Indexer) *Federation {
  fed := &Federation{ns: ns, store: store, indexer: indexer, addr: addr, queues: make(map[string]*queue)}
  return fed
}

func (self *Federation) getQueue(domain string) chan<- queueEntry {
  self.mutex.Lock()
  q, ok := self.queues[domain]
  if !ok {
    ch := make(chan queueEntry)
    q = newQueue(self, domain, ch)
    self.queues[domain] = q
  }
  self.mutex.Unlock()
  return q.channel
}

func (self *Federation) Forward(blobref string, users []string) {  
  log.Printf("Forwarding %v to %v\n", blobref, users)
  // Determine the servers that have to be informed
  urls := make(map[string]vec.StringVector)
  for _, user := range users {
    rawurl, err := self.ns.Lookup(user)
    if err != nil {
      log.Print("Unknown user", err)
      continue
    }
    _, err = http.ParseURL(rawurl)
    if err != nil {
      log.Printf("Malformed URL: %v\n", rawurl)
      continue
    }
    urlList, _ := urls[rawurl]
    urlList.Push(user[strings.Index(user, "@") + 1:])
    urls[rawurl] = urlList
  }
  
  for url, urlUsers := range urls {
    q := self.getQueue(url)
    q <- queueEntry{urlUsers, blobref}
  }
}

func (self *Federation) Listen() (err os.Error) {
  mux := http.NewServeMux()
  f := func(w http.ResponseWriter, req *http.Request) {
    self.handleRequest(w, req)
  }
  mux.HandleFunc("/fed", f)

  // Accept incoming connections
  server := &http.Server{Addr: self.addr, Handler: mux}
  err = server.ListenAndServe()
  if err != nil {
    log.Printf("ListenAndServe: %v\n", err.String())
  }
  return
}

func (self *Federation) handleRequest(w http.ResponseWriter, req *http.Request) {
  switch req.Method {
  case "POST", "PUT":
    blob, err := ioutil.ReadAll(req.Body)
    if err != nil {
      log.Printf("Error reading request body")
      return
    }
    req.Body.Close()
    log.Printf("Received blob via federation: %v\n", string(blob))
    self.store.StoreBlob(blob, "")
    w.WriteHeader(200)
  case "GET":
    values := req.URL.Query()
    //
    // GET /fed?blobref=xyz
    //
    if blobref := values.Get("blobref"); blobref != "" {
      blob, err := self.store.GetBlob(blobref)
      if err != nil {
	log.Printf("Failed retrieving blob\n")
	// TODO: Better error message
	w.WriteHeader(500)
	return
      }
      w.Header().Add("Content-type", "application/octet-stream")
      buf := bytes.NewBuffer(blob)
      written, err := io.Copy(w, buf)
      if written != int64(len(blob)) || err != nil {
	log.Printf("Failed sending blob\n")
	return
      }
    //
    // GET /fed?frontier=xyz
    //
    } else if blobref = values.Get("frontier"); blobref != "" {
      perma, err := self.indexer.PermaNode(blobref)
      if perma == nil || err != nil {
	log.Printf("Failed retrieving perma node")
	w.WriteHeader(500)
	return
      }
      var result []byte
      if perma.OT() == nil {
	result = []byte("[]")
      } else {
	var err os.Error
	result, err = json.Marshal(perma.OT().Frontier().IDs())
	if err != nil {
	  log.Printf("Failed retrieving the frontier")
	  return
	}
      }
      w.Header().Add("Content-type", "application/json")
      buf := bytes.NewBuffer(result)
      written, err := io.Copy(w, buf)
      if written != int64(len(result)) || err != nil {
	log.Printf("Failed sending result\n")
	return
      }
    } else {
      log.Printf("Malformed get request\n")
      // TODO: Better error message
      w.WriteHeader(500)
    }
  default:
    w.WriteHeader(500)
    // TODO: give error message
  }
}