package lightwavefed

import (
  "json"
)

type Message struct {
  ID int "id"
  Cmd string "cmd"
  Status int "stat"
  Payload *json.RawMessage "data"
}
