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
  "json"
  "http"
  "bytes"
  "template"
  "strconv"
  "time"
  "rand"
  "strings"
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
  UserName string
  SessionID string
  OpenPermas []string
}

func init() {
  rand.Seed(time.Nanoseconds())
  
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
  usr, err := s.HasUser(u.Id)
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
    _, err := createBook(r);
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
  ch.UserName = u.String()
  ch.Token = tok
  ch.SessionID = session
  _, err = datastore.Put(c, datastore.NewKey("channel", u.Id + "/" + session, 0, nil), &ch)
  if err != nil {
    return
  }

  b := new(bytes.Buffer)
  data := map[string]interface{}{ "userid":  u.String(), "logout": logout_url, "token": tok, "session": session }
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
  if !is_open {
    // Update channel infos
    ch.OpenPermas = append(ch.OpenPermas, req.Perma)
    _, err = datastore.Put(c, datastore.NewKey("channel", u.Id + "/" + req.Session, 0, nil), &ch)
    if err != nil {
      sendError(w, r, "Internal server error")
    }
    // Repeat all blobs from this document.  
    s := newStore(c)
    g := grapher.NewGrapher(u.String(), s, s, nil)
    s.SetGrapher(g)
    ch := newChannelAPI(c, s, req.Session, true, g)
    g.Repeat(req.Perma, req.From)
    fmt.Fprintf(w, `{"ok":true, "blobs":[%v]}`, strings.Join(ch.messageBuffer, ","))
    return
  }
  // Done
  fmt.Fprint(w, `{"ok":true}`)
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
  g := grapher.NewGrapher(u.String(), s, s, nil)
  s.SetGrapher(g)
  tf.NewTransformer(g)
  newChannelAPI(c, s, sessionid, false, g)
  
//  log.Printf("Received: %v", string(blob))
  blobref, seqNumber, e := g.HandleClientBlob(blob)
  if e != nil {
    fmt.Fprintf(w, `{"ok":false, "error":"%v"}`, e.String())
    return
  }

  if seqNumber != -1 && blobref != "" {
    fmt.Fprintf(w, `{"ok":true, "blobref":"%v", "seq":%v}`, blobref, seqNumber)
  } else if seqNumber != -1 {
    fmt.Fprintf(w, `{"ok":true, "seq":%v}`, seqNumber)
  } else {
    fmt.Fprintf(w, `{"ok":true, "blobref":"%v"}`, blobref)
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

func handleListInbox(w http.ResponseWriter, r *http.Request) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  if u == nil {
    sendError(w, r, "No user attached to the request")
    return
  }

  s := newStore(c)
  inbox, err := s.ListInbox()
  if err != nil {
    sendError(w, r, err.String())
    return
  }
  
  j := map[string]interface{}{"ok":true, "permas":inbox}
  msg, err := json.Marshal(j)
  if err != nil {
    panic("Cannot serialize")
  }
  fmt.Fprint(w, string(msg))
}

func createBook(r *http.Request) (perma_blobref string, err os.Error) {
  c := appengine.NewContext(r)
  u := user.Current(c)
  s := newStore(c)
  g := grapher.NewGrapher(u.String(), s, s, nil)
  s.SetGrapher(g)

  blob := []byte(`{"type":"permanode", "mimetype":"application/x-lightwave-book"}`) 
  perma_blobref, _, err = g.HandleClientBlob(blob)
  return
}
