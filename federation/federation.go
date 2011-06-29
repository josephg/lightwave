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
  userID string
  addr string
  mutex sync.Mutex
  store store.BlobStore
  ns store.NameService
  indexer *idx.Indexer
  queues map[string]*queue
}

func NewFederation(userid, addr string, ns store.NameService, store store.BlobStore) *Federation {
  fed := &Federation{userID: userid, ns: ns, store: store, addr: addr, queues: make(map[string]*queue)}
  return fed
}

func (self *Federation) SetIndexer(indexer *idx.Indexer) {
  self.indexer = indexer
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
    if user == self.userID {
      continue
    }
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

// Downloads a permanode and all blobs up-to and including the frontier blobs.
func (self *Federation) DownloadBlobsRecursively(rawurl string, blobrefs []string) (err os.Error) {
  for i := 0; i < len(blobrefs); i++ {
    blobref := blobrefs[i]
    dependencies, err := self.DownloadBlob(rawurl, blobref)
    if err != nil {
      return err
    }
    blobrefs = append(blobrefs, dependencies...)
  }
  return
}

type depSchema struct {
  Dependencies []string "dep"
}

func (self *Federation) DownloadBlob(rawurl, blobref string) (dependencies []string, err os.Error) {
  // Get the blob
  var client http.Client
  req, err := client.Get(rawurl + "?blobref=" + http.URLEscape(blobref))
  if err != nil {
    return nil, err
  }
  // TODO: Improve for large files
  blob, err := ioutil.ReadAll(req.Body)
  if err != nil {
    log.Printf("Error reading request body")
    return nil, err
  }
  req.Body.Close()
  self.store.StoreBlob(blob, "")
  // Check whether the retrieved blob is a schema blob
  mimetype := idx.MimeType(blob)
  if mimetype == "application/x-lightwave-schema" {
    var schema depSchema
    err = json.Unmarshal(blob, &schema)
    if err != nil {
      log.Printf("Malformed schema blob: %v\n", err)
      return nil, err
    }
    dependencies = schema.Dependencies
  }
  return
}

func (self *Federation) DownloadFrontier(rawurl string, blobref string) (frontier []string, err os.Error) {
  // Get the blob
  var client http.Client
  req, err := client.Get(rawurl + "?frontier=" + http.URLEscape(blobref))
  if err != nil {
    return nil, err
  }
  // Process the returned value
  blob, err := ioutil.ReadAll(req.Body)
  if err != nil {
    log.Printf("Error reading request body")
    return nil, err
  }
  req.Body.Close()
  err = json.Unmarshal(blob, &frontier)
  if err != nil {
    log.Printf("Malformed frontier response: %v\n", err)
    return nil, err
  }
  return
}

type invitationSchema struct {
  User string "user"
  Signer string "signer"
  PermaNode string "perma"
  Dependencies []string "dep"
}

func (self *Federation) AcceptInvitation(blobref string) (err os.Error) {
  // Load the invitation from the store
  blob, err := self.store.GetBlob(blobref)
  if err != nil {
    return err
  }
  var schema invitationSchema
  err = json.Unmarshal(blob, &schema)
  if err != nil {
    return err
  }
  
  // Find the web server of this user
  var rawurl string
  rawurl, err = self.ns.Lookup(schema.Signer)
  if err != nil {
    return
  }

  // Get the frontier
  // frontier, err := self.DownloadFrontier(rawurl, schema.PermaNode)
  // if err != nil {
  //   return err
  // }
  
  // Download the perma node and all its member blobs
  // blobrefs := []string{schema.PermaNode}
  // blobrefs = append(blobrefs, frontier...)
  // err = self.DownloadBlobsRecursively(rawurl, frontier) 
  // if err != nil {
  //  return err
  // }

  err = self.DownloadBlobsRecursively(rawurl, schema.Dependencies) 
  if err != nil {
    return err
  }

  // Create a keep on the permaNode.
  keepJson := map[string]interface{}{ "signer": self.userID, "permission": blobref, "perma":schema.PermaNode, "dep": []string{blobref}}
  keepBlob, err := json.Marshal(keepJson)
  if err != nil {
    panic(err.String())
  }
  keepBlob = append([]byte(`{"type":"keep",`), keepBlob[1:]...)
  log.Printf("Storing keep %v\n", string(keepBlob))
  self.store.StoreBlob(keepBlob, "")
  return
}
