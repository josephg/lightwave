package lightwavefed

import (
  "json"
  "os"
)

type Message struct {
  ID int "id"
  Cmd string "cmd"
  Status int "stat"
  Payload *json.RawMessage "data"
}

// 'data' is a pointer to some data structure.
// The function decodes the payload into this data structure.
func (self *Message) DecodePayload(data interface{}) os.Error {
  if self.Payload == nil {
    return os.NewError("No payload in message")
  }
  return json.Unmarshal(*self.Payload, data)
}

func (self *Message) EncodePayload(data interface{}) os.Error {
  bytes, err := json.Marshal(data)
  if err != nil {
    return err
  }
  p := json.RawMessage(bytes)
  self.Payload = &p
  return nil
}

