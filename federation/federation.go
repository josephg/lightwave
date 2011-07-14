package lightwavefed

import (
  grapher "lightwavegrapher"
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

type NameService interface {
  // Returns a string of the form "hostname:port", "Ip-address:port" or ":port".
  // Real life applications will use DNS A-records + default ports or DNS SRV-records
  // to perform the lookup. For demos we can hardcode it.
  Lookup(userID string) (addr string, err os.Error)
}

type Federation struct {
  userID string
  addr string
  mutex sync.Mutex
  store store.BlobStore
  ns NameService
  grapher *grapher.Grapher
  queues map[string]*queue
}

func NewFederation(userid, addr string, ns NameService, store store.BlobStore) *Federation {
  fed := &Federation{userID: userid, ns: ns, store: store, addr: addr, queues: make(map[string]*queue)}
  return fed
}

func (self *Federation) SetGrapher(grapher *grapher.Grapher) {
  self.grapher = grapher
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

  if len(urls) > 0 {
    log.Printf("Forwarding %v to %v\n", blobref, users)
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
      frontier, err := self.grapher.Frontier(blobref)
      if err != nil {
	log.Printf("Failed retrieving the frontier")
	return
      }
      result, err := json.Marshal(frontier)
      if err != nil {
	log.Printf("Failed marshaling the frontier")
	return
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

// TODO: Use the permanode blobref instead
func (self *Federation) DownloadPermaNode(permission_blobref string) os.Error {
  // Load the invitation from the store
  blob, err := self.store.GetBlob(permission_blobref)
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
    return err
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

  // TODO: Do not download blobs which are already available
  err = self.downloadBlobsRecursively(rawurl, schema.Dependencies) 
  if err != nil {
    return err
  }
  return nil
}

// Downloads a permanode and all blobs up-to and including the frontier blobs.
func (self *Federation) downloadBlobsRecursively(rawurl string, blobrefs []string) (err os.Error) {
  for i := 0; i < len(blobrefs); i++ {
    blobref := blobrefs[i]
    dependencies, err := self.downloadBlob(rawurl, blobref)
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

func (self *Federation) downloadBlob(rawurl, blobref string) (dependencies []string, err os.Error) {
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
  log.Printf("Downloaded %v\n", string(blob))
  req.Body.Close()
  self.store.StoreBlob(blob, "")
  // Check whether the retrieved blob is a schema blob
  mimetype := grapher.MimeType(blob)
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

func (self *Federation) downloadFrontier(rawurl string, blobref string) (frontier []string, err os.Error) {
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
