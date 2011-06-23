package lightwavestore

import (
  "json"
  "os"
)

type Message struct {
  Cmd string "cmd"
  Payload *json.RawMessage "data"
  connection *Connection
}

type messageWithoutPayload struct {
  Cmd string "cmd"
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
  if data == nil {
    self.Payload = nil
    return nil
  }
  if raw, ok := data.(json.RawMessage); ok {
    self.Payload = &raw
    return nil
  }
  bytes, err := json.Marshal(data)
  if err != nil {
    return err
  }
  p := json.RawMessage(bytes)
  self.Payload = &p
  return nil
}

