package lightwave

import (
  "log"
  "fmt"
  "os"
  "io/ioutil"
  "appengine"
  "appengine/channel"
  "appengine/user"
  "appengine/datastore"
  "appengine/mail"
  "appengine/memcache"
  "json"
  "http"
  "bytes"
  "template"
  "strconv"
  "time"
  "rand"
  "strings"
  "crypto/sha256"
  grapher "lightwavegrapher"
  tf "lightwavetransformer"
)

var (
  frontPageTmpl    *template.Template
  frontPageTmplErr os.Error
)

type channelStruct struct {
  Token string
  UserID string
  UserEmail string
  SessionID string
  OpenPermas []string
}

var schema *grapher.Schema

func init() {
  rand.Seed(time.Nanoseconds())

  // TODO: This should end up in a configuration file
  schema = &grapher.Schema{ FileSchemas: map[string]*grapher.FileSchema {
      "application/x-lightwave-book": &grapher.FileSchema{ EntitySchemas: map[string]*grapher.EntitySchema {
	  "application/x-lightwave-entity-chapter": &grapher.EntitySchema { FieldSchemas: map[string]*grapher.FieldSchema {
	      "after": &grapher.FieldSchema{ Type: grapher.TypeEntityBlobRef, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone },
	      "title": &grapher.FieldSchema{ Type: grapher.TypeString, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone },
	      "color": &grapher.FieldSchema{ Type: grapher.TypeInt64, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone } } },
	  "application/x-lightwave-entity-page": &grapher.EntitySchema { FieldSchemas: map[string]*grapher.FieldSchema {
	      "after": &grapher.FieldSchema{ Type: grapher.TypeEntityBlobRef, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone },
	      "title": &grapher.FieldSchema{ Type: grapher.TypeString, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone },
	      "chapter": &grapher.FieldSchema{ Type: grapher.TypeEntityBlobRef, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone },
	      "page": &grapher.FieldSchema{ Type: grapher.TypePermaBlobRef, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone } } } } },
      "application/x-lightwave-page": &grapher.FileSchema{ EntitySchemas: map[string]*grapher.EntitySchema {
	  "application/x-lightwave-entity-content": &grapher.EntitySchema { FieldSchemas: map[string]*grapher.FieldSchema {
	      "layout": &grapher.FieldSchema{ Type: grapher.TypeString, ElementType: grapher.TypeNone, Transformation: grapher.TransformationNone },
	      "text": &grapher.FieldSchema{ Type: grapher.TypeString, ElementType: grapher.TypeNone, Transformation: grapher.TransformationString } } } } } } }

  frontPageTmpl = template.New(nil)
  frontPageTmpl.SetDelims("{{", "}}")
  if err := frontPageTmpl.ParseFile("notebook.html"); err != nil {
    frontPageTmplErr = fmt.Errorf("tmpl.ParseFile failed: %v", err)
    return
  }

  http.HandleFunc("/", handleFrontPage)
  http.HandleFunc("/private/submit", handleSubmit)
  http.HandleFunc("/private/open", handleOpen)
  http.HandleFunc("/private/close", handleClose)
  http.HandleFunc("/private/listpermas", handleListPermas)
  http.HandleFunc("/private/listinbox", handleListInbox)
  http.HandleFunc("/private/listunread", handleListUnread)
  http.HandleFunc("/private/invitebymail", handleInviteByMail)
  http.HandleFunc("/private/inboxitem", handleInboxItem)
  http.HandleFunc("/private/markasread", handleMarkAsRead)

  http.HandleFunc("/internal/notify", handleDelayedNotify)

  http.HandleFunc("/_ah/channel/connected/", handleConnect)
  http.HandleFunc("/_ah/channel/disconnected/", handleDisconnect)
}

func handleFrontPage(w http.ResponseWriter, r *http.Request) {
  if r.URL.Path != "/" {
    http.Error(w, "404 Not Found", http.StatusNotFound)
    return
  }
  c := appengine.NewContext(r)
  u := user.Current(c)
  // User logged in?
  if u == nil {
    url, _ := user.LoginURL(c, "/")
    fmt.Fprintf(w, `<a href="%s">Sign in or register</a>`, url)
    return
  }

  s := newStore(c)

  // New User?
  usr, err := hasUser(c, u.Id)
  if err != nil {
    http.Error(w, "Couldn't search for user", http.StatusInternalServerError)
    c.Errorf("HasUser: %v", err)
    return
  }
  
  // New user?
  if usr == nil {
    usr, err = s.CreateUser()
    if err != nil {
      http.Error(w, "Couldn't create user", http.StatusInternalServerError)
      c.Errorf("CreateUser: %v", err)
      return
    }
    _, err := createBook(c);
    if err != nil {
      http.Error(w, "Couldn't create book", http.StatusInternalServerError)
      c.Errorf("CreateBook: %v", err)
      return
    }
  }
  
  if frontPageTmplErr != nil {
    w.WriteHeader(http.StatusInternalServerError) // 500
    fmt.Fprintf(w, "Page template is bad: %v", frontPageTmplErr)
    return
  }

  // TODO: This does not allow for multiple sessions
  session := fmt.Sprintf("s%v", rand.Int31())
  tok, err := channel.Create(c, u.Id + "/" + session)
  if err != nil {
    http.Error(w, "Couldn't create Channel", http.StatusInternalServerError)
    c.Errorf("channel.Create: %v", err)
    return
  }

  logout_url, _ := user.LogoutURL(c, "/")

  var ch channelStruct
  ch.UserID = u.Id
  ch.UserEmail = u.Email
  ch.Token = tok
  ch.SessionID = session
  _, err = datastore.Put(c, datastore.NewKey("channel", u.Id + "/" + session, 0, nil), &ch)
  if err != nil {
    return
  }

  b := new(bytes.Buffer)
  data := map[string]interface{}{ "userid":  u.Email, "logout": logout_url, "token": tok, "session": session }
  if err := frontPageTmpl.Execute(b, data); err != nil {
    w.WriteHeader(http.StatusInternalServerError) // 500
    fmt.Fprintf(w, "tmpl.Execute failed: %v", err)
    return
  }

  // Cookie
  cookie := &http.Cookie{Path:"/", Name:"Session", Value: session, Expires: *time.SecondsToUTC(time.UTC().Seconds() + 60 * 60 * 24)}
  http.SetCookie(w, cookie)

  w.Header().Set("Content-Length", strconv.Itoa(b.Len()))
  b.WriteTo(w)
}

func handleConnect(w http.ResponseWriter, r *http.Request) {
  u := r.FormValue("from")
  log.Printf("CONNECT '%v' and %v", u, r.Form)
}

func handleDisconnect(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := r.FormValue("from")
  log.Printf("DISCONNECT '%v' and %v", u, r.Form)
  if err := datastore.Delete(c, datastore.NewKey("channel", u, 0, nil)); err != nil {
    log.Printf("Err: %v", err)
    return
  }
}

type openCloseRequest struct {
  Session string "session"
  Perma string "perma"
  From int64 "from"
  MarkAsRead bool "markasread"
}

func handleOpen(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }
  jreq, err := ioutil.ReadAll(r.Body)
  if err != nil {
    http.Error(w, "Error reading request body", http.StatusInternalServerError)
    return
  }
  r.Body.Close()
  // Parse request
  var req openCloseRequest
  err = json.Unmarshal(jreq, &req)
  if err != nil {
    sendError(w, r, "Malformed JSON")
    return
  }
  // Load the channel infos
  var ch channelStruct
  if err = datastore.Get(c, datastore.NewKey("channel", u.Id + "/" + req.Session, 0, nil), &ch); err != nil {
    sendError(w, r, "Unknown channel")
    return
  }
  // Check
  if len(ch.OpenPermas) >= 10 {
    sendError(w, r, "Too many open channels")
    return    
  }
  is_open := false
  for _, p := range ch.OpenPermas {
    if p == req.Perma {
      is_open = true
      break
    }
  }
  var perma grapher.PermaNode
  if !is_open {
    // Update channel infos
    ch.OpenPermas = append(ch.OpenPermas, req.Perma)
    _, err = datastore.Put(c, datastore.NewKey("channel", u.Id + "/" + req.Session, 0, nil), &ch)
    if err != nil {
      sendError(w, r, "Internal server error")
    }
    // Repeat all blobs from this document.  
    s := newStore(c)
    g := grapher.NewGrapher(u.Email, schema, s, s, nil)
    s.SetGrapher(g)
    ch := newChannelAPI(c, s, req.Session, true, g)
    perma, err = g.Repeat(req.Perma, req.From)
    if err != nil {
      sendError(w, r, "Failed opening")
    }
    fmt.Fprintf(w, `{"ok":true, "blobs":[%v]}`, strings.Join(ch.messageBuffer, ","))
  } else {
    fmt.Fprint(w, `{"ok":true, "blobs":[]}`)
  }

  if req.MarkAsRead {
    if perma == nil {
      s := newStore(c)
      data, err := s.GetPermaNode(req.Perma)
      if err != nil {
	log.Printf("Err: Failed reading permanode")
	return
      }
      perma = grapher.NewPermaNode(nil)
      perma.FromMap(req.Perma, data)
    }
    markAsRead(c, perma.BlobRef(), perma.SequenceNumber() - 1)
  }
}

func handleClose(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }
  jreq, err := ioutil.ReadAll(r.Body)
  if err != nil {
    http.Error(w, "Error reading request body", http.StatusInternalServerError)
    return
  }
  r.Body.Close()
  // Parse request
  var req openCloseRequest
  err = json.Unmarshal(jreq, &req)
  if err != nil {
    sendError(w, r, "Malformed JSON")
    return
  }
  // Load the channel infos
  var ch channelStruct
  if err = datastore.Get(c, datastore.NewKey("channel", u.Id + "/" + req.Session, 0, nil), &ch); err != nil {
    sendError(w, r, "Unknown channel")
    return
  }
  ok := false
  permas := []string{}
  for _, p := range ch.OpenPermas {
    if p == req.Perma {
      ok = true
      continue
    }
    permas = append(permas, p)
  }
  if !ok {
    sendError(w, r, "Was not open")
    return
  }
  // Update channel infos
  ch.OpenPermas = permas
  _, err = datastore.Put(c, datastore.NewKey("channel", u.Id + "/" + req.Session, 0, nil), &ch)
  if err != nil {
    sendError(w, r, "Internal server error")
  }
  // Done
  fmt.Fprint(w, `{"ok":true}`)
}

func handleSubmit(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }

  // HACK: Cookies are broken on AppEngine
  sessionid := r.FormValue("session")
  /*
  cookie, err := r.Cookie("Session")
  if err != nil {
    sendError(w, r, "No session cookie")
    return
  } */   

  /*
  cookie := getSessionCookie(r)
  if cookie == nil {
    sendError(w, r, "No session cookie")
    return
  }
  sessionid := cookie.Value
  */

  blob, err := ioutil.ReadAll(r.Body)
  if err != nil {
    sendError(w, r, "Error reading request body")
    return
  }
  r.Body.Close()

  s := newStore(c)
  g := grapher.NewGrapher(u.Email, schema, s, s, nil)
  s.SetGrapher(g)
  tf.NewTransformer(g)
  newChannelAPI(c, s, sessionid, false, g)
  
//  log.Printf("Received: %v", string(blob))
  node, e := g.HandleClientBlob(blob)
  if e != nil {
    fmt.Fprintf(w, `{"ok":false, "error":"%v"}`, e.String())
    return
  }

  if perm, ok := node.(grapher.PermissionNode); ok {
    // Is the user who was granted permissions registered locally?
    usr, err := hasUserName(c, perm.UserName())
    if err != nil {
      log.Printf("Err in HasUserName: %v", err)
    }
    knownuser := "true"
    if usr == "" {
      knownuser = "false"
    }
    fmt.Fprintf(w, `{"ok":true, "blobref":"%v", "seq":%v, "knownuser":%v}`, perm.BlobRef(), perm.SequenceNumber(), knownuser )
  } else if otnode, ok := node.(grapher.OTNode); ok {
    fmt.Fprintf(w, `{"ok":true, "blobref":"%v", "seq":%v}`, otnode.BlobRef(), otnode.SequenceNumber())
  } else if perma, ok := node.(grapher.PermaNode); ok {
    // Add to the inbox of the user who created the permanode
    b := inboxStruct{LastSeq: 0, Archived: true}
    parent := datastore.NewKey("user", u.Id, 0, nil)
    _, err = datastore.Put(c, datastore.NewKey("inbox", perma.BlobRef(), 0, parent), &b)
    fmt.Fprintf(w, `{"ok":true, "blobref":"%v"}`, node.BlobRef())
  } else {
    fmt.Fprintf(w, `{"ok":true, "blobref":"%v"}`, node.BlobRef())
  }
}

func sendError(w http.ResponseWriter, r *http.Request, msg string) {
  log.Printf("Err: %v", msg)
  fmt.Fprintf(w, `{"ok":false, "error":"%v"}`, msg)
}

/*
func getSessionCookie(r *http.Request) *http.Cookie {
  for _, c := range r.Cookie {
    if c.Name == "Session" {
      return c
    }
  }
  return nil
}
*/

func handleListPermas(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }

  s := newStore(c)
  permas, err := s.ListPermas(r.FormValue("mimetype"))
  if err != nil {
    sendError(w, r, err.String())
    return
  }
  
  j := map[string]interface{}{"ok":true, "permas":permas}
  msg, err := json.Marshal(j)
  if err != nil {
    panic("Cannot serialize")
  }
  fmt.Fprint(w, string(msg))
}

func userShortName(email string) string {
  i := strings.Index(email, "@")
  return strings.ToUpper(email[0:1]) + email[1:i]
}

func handleListInbox(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }

  s := newStore(c)
  inbox, err := s.ListInbox(false)
  if err != nil {
    sendError(w, r, err.String())
    return
  }

  // Read the updates for all items in the inbox
  for _, entry := range inbox {
    fillInboxItem(s, entry["perma"].(string), entry["seq"].(int64), entry)
  }
  
  j := map[string]interface{}{"ok":true, "items":inbox}
  msg, err := json.Marshal(j)
  if err != nil {
    panic("Cannot serialize")
  }
  fmt.Fprint(w, string(msg))
}

type inviteByMail struct {
  Content string "content"
  UserName string "user"
}

func handleInviteByMail(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }

  blob, err := ioutil.ReadAll(r.Body)
  if err != nil {
    sendError(w, r, "Error reading request body")
    return
  }
  r.Body.Close()

  var req inviteByMail
  err = json.Unmarshal(blob, &req)
  if err != nil {
    sendError(w, r, "Malformed request body: " + err.String())
    return
  }
  
  msg := &mail.Message{ 
    Sender:  u.Email,
    To:      []string{req.UserName},
    Subject: "Invitation to LightWave",
    Body:    req.Content,
  }
  if err := mail.Send(c, msg); err != nil {
    sendError(w, r, "Could not send mail");
    c.Errorf("Couldn't send email: %v", err)
  }
}

func handleDelayedNotify(w http.ResponseWriter, r *http.Request) {
  log.Printf("Task started")
  c := appengine.NewContext(r)
  // Read the form data
  perma_blobref := r.FormValue("perma")
  // Allow for further notification tasks to be enqueued
  memcache.Delete(c, "notify-" + perma_blobref)
  // Find out about the users of this permanode
  s := newStore(c)
  data, err := s.GetPermaNode(perma_blobref)
  if err != nil {
    http.Error(w, "Err: Failed reading permanode", http.StatusInternalServerError)
    return
  }
  perma := grapher.NewPermaNode(nil)
  perma.FromMap(perma_blobref, data)
  message := fmt.Sprintf(`{"type":"notification", "perma":"%v", "lastseq":%v}`, perma_blobref, perma.SequenceNumber() - 1);
  // For all users find all channels on which they are listening
  for _, user := range perma.Users() {
    // Do we know this user?
    userid, err := hasUserName(c, user)
    if err != nil {
      log.Printf("Err: Unknown user %v", user)
      continue
    }
    // Mark that there is an unread file
    err = addUnread(c, userid, perma_blobref)
    if err != nil {
      log.Printf("Err writing unread: %v", err)
    }
    // Notify browser instances where this user is logged in
    var channels []channelStruct
    query := datastore.NewQuery("channel").Filter("UserEmail =", user)
    for it := query.Run(c) ; ; {
      var data channelStruct
      _, e := it.Next(&data)
      if e == datastore.Done {
	break
      }
      if e != nil {
	log.Printf("Err: in query: %v",e)
	break
      }
      channels = append(channels, data)
    }
    for _, ch := range channels {
      log.Printf("Sending to %v", ch.UserID + "/" + ch.SessionID)
      err := channel.Send(c, ch.UserID + "/" + ch.SessionID, message)
      if err != nil {
	log.Printf("Failed sending to channel %v", ch.UserID + "/" + ch.SessionID)
      }
    }
  }
}

func handleInboxItem(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  s := newStore(c)
  perma_blobref := r.FormValue("perma")
  item, err := s.GetInboxItem(perma_blobref)
  if err != nil {
    http.Error(w, "Could not get inbox item", http.StatusInternalServerError)
    c.Errorf("handleInboxItem: %v", err)
    return
  }
  entry := make(map[string]interface{})
  entry["perma"] = perma_blobref
  entry["seq"] = item.LastSeq
  err = fillInboxItem(s, perma_blobref, item.LastSeq, entry)
  if err != nil {
    fmt.Fprintf(w, `{"ok":false, "error":%v}`, err.String())
    return
  }
 
  info, _ := json.Marshal(entry)
  fmt.Fprintf(w, `{"ok":true, "item":%v}`, string(info))
}

func handleMarkAsRead(w http.ResponseWriter, r *http.Request) {
  // TODO: In a transaction first read it, build max, then write it back
  c := appengine.NewContext(r)
  perma_blobref := r.FormValue("perma")
  seq, err := strconv.Atoi64(r.FormValue("seq"))
  if err != nil {
    http.Error(w, "Expected a seq parameter", http.StatusInternalServerError)
    c.Errorf("handleInboxItem: %v", err)
    return
  }
  err = markAsRead(c, perma_blobref, seq)
  if err != nil {
    fmt.Fprintf(w, `{"ok":false, "error":"Failed accessing database"}`)
  } else {
    fmt.Fprintf(w, `{"ok":true}`)
  }
}

func markAsRead(c appengine.Context, perma_blobref string, seq int64) os.Error {
  s := newStore(c)
  return s.MarkInboxItemAsRead(perma_blobref, seq)
}

type UnreadStruct struct {
  BloomFilter []byte
}

func handleListUnread(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }

  s := newStore(c)
  inbox, err := s.ListInbox(true)
  if err != nil {
    sendError(w, r, err.String())
    return
  }

  // Read the bloom filter
  parent := datastore.NewKey("user", u.Id, 0, nil)
  key := datastore.NewKey("unread", "unread", 0, parent)
  filter := NewBloomFilter(sha256.New())
  var b UnreadStruct
  err = datastore.Get(c, key, &b)
  if err == datastore.ErrNoSuchEntity {
    log.Printf("No filter found")
    // Do nothing
  } else if err != nil {
    sendError(w, r, "Failed reading unread-bloom-filter")
    return    
  } else {
    filter.Load(b.BloomFilter)
  }
  
  // Read the updates for all items in the inbox
  unread := map[string]interface{}{}
  for _, entry := range inbox {
    perma_blobref := entry["perma"].(string)
    lastSeq := entry["seq"].(int64)
    log.Printf("Testing %v for unread", perma_blobref)
    if !filter.Has([]byte(perma_blobref)) {
      log.Printf("   not in filter")
      continue
    }
    data, err := s.GetPermaNode(perma_blobref)
    if err != nil {
      log.Printf("ERR: Failed reading permanode")
      continue
    }
    perma := grapher.NewPermaNode(nil)
    perma.FromMap(perma_blobref, data)
    updates := perma.Updates()
    var authors int64 = 0
    for _, user := range perma.Users() {
      if seq, ok := updates[user]; ok {
	if seq > lastSeq {
	  authors++
	}
      }
    }
    if authors > 0 {
      unread[perma_blobref] = lastSeq
    }
  }
  
  j := map[string]interface{}{"ok":true, "unread":unread}
  msg, err := json.Marshal(j)
  if err != nil {
    panic("Cannot serialize")
  }
  fmt.Fprint(w, string(msg))
}

// A helper function to produce information about inbox items
func fillInboxItem(s *store, perma_blobref string, lastSeq int64, entry map[string]interface{}) (err os.Error) {
  followers := []string{}
  authors := []string{}
  latestauthors := []string{}
  data, err := s.GetPermaNode(perma_blobref)
  if err != nil {
    log.Printf("ERR: Failed reading permanode")
    return err
  }
  perma := grapher.NewPermaNode(nil)
  perma.FromMap(perma_blobref, data);
  updates := perma.Updates()
  for _, user := range perma.Followers() {
    if seq, ok := updates[user]; ok {
      if seq > lastSeq {
	latestauthors = append(latestauthors, userShortName(user))
      } else if seq >= 0 {
	authors = append(authors, userShortName(user))
      } else {
	followers = append(followers, userShortName(user))
      }
    } else {
      followers = append(followers, userShortName(user))
    }
  }
  entry["followers"] = followers
  entry["authors"] = authors
  entry["latestauthors"] = latestauthors
  entry["digest"] = "Untitled Page" // TODO
  return nil
}

func addUnread(c appengine.Context, userid string, perma_blobref string) os.Error {
  // TODO: Execute in a transaction
  filter := NewBloomFilter(sha256.New())
  parent := datastore.NewKey("user", userid, 0, nil)
  key := datastore.NewKey("unread", "unread", 0, parent)
  var b UnreadStruct
  err := datastore.Get(c, key, &b)
  if err == datastore.ErrNoSuchEntity {
    // Do nothing
  } else if err != nil {
    return err
  } else {
    filter.Load(b.BloomFilter)
  }
  if filter.Has([]byte(perma_blobref)) {
    return nil
  }
  log.Printf("Writing unread %v for user %v", perma_blobref, userid);
  filter.Add([]byte(perma_blobref))
  b.BloomFilter = filter.Bytes()
   _, err = datastore.Put(c, key, &b)
  return err
}

func createBook(c appengine.Context) (perma_blobref string, err os.Error) {
  u := user.Current(c)
  s := newStore(c)
  g := grapher.NewGrapher(u.Email, schema, s, s, nil)
  s.SetGrapher(g)

  blob := []byte(`{"type":"permanode", "mimetype":"application/x-lightwave-book"}`) 
  var node grapher.AbstractNode
  node, err = g.HandleClientBlob(blob)
  if err == nil {
    perma_blobref = node.BlobRef();
  }
  return 
}

func addToInbox(c appengine.Context, username string, perma_blobref string, seq int64) (err os.Error) {
  // Does this user have an inbox on this machine?
  userid, err := hasUserName(c, username);
  if err != nil || userid == "" {
    return err
  }
  
  log.Printf("INBOX for user %v", username)
  
  // Store it
  b := inboxStruct{LastSeq: seq}
  parent := datastore.NewKey("user", userid, 0, nil)
  _, err = datastore.Put(c, datastore.NewKey("inbox", perma_blobref, 0, parent), &b)
  return err
}

type userStruct struct {
  UserEmail string
}

func hasUser(c appengine.Context, userid string) (usr *userStruct, err os.Error) {
  key := datastore.NewKey("user", userid, 0, nil)
  usr = &userStruct{}
  if err = datastore.Get(c, key, usr); err != nil {
    usr = nil;
    if err == datastore.ErrNoSuchEntity || err == datastore.ErrInvalidEntityType {
      err = nil
    }
    return
  }
  return
}

func hasUserName(c appengine.Context, username string) (userid string, err os.Error) {
  query := datastore.NewQuery("user").Filter("UserEmail =", username).KeysOnly()
  it := query.Run(c)
  key, err := it.Next(nil)
  if err == datastore.Done {
    return "", nil
  }
  if err != nil {
    log.Printf("Err: in query: %v", err)
    return "", err
  }
  return key.StringID(), nil
}
