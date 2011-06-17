package lightwavefed

type Federation struct {
  reqHandler map[string]RequestHandler
}

func NewFederation() *Federation {
  return &Federation{reqHandler: make(map[string]RequestHandler)}
}

type RequestHandler func(request *Message) (status int, data interface{})

// Registers a handler to deal with messages depding on their 'cmd' field.
func (self *Federation) Handle(cmd string, handler RequestHandler) {
  self.reqHandler[cmd] = handler
}

func (self *Federation) Handler(cmd string) (handler RequestHandler) {
  handler, ok := self.reqHandler[cmd]
  if !ok {
    return nil
  }
  return handler
}

