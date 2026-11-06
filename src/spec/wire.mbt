// The JSON form of a message and of an outcome.
//
// The keys are the published protocol's, and the two instants are ISO 8601 text
// with an offset, which is what every implementation of that protocol writes.
// Inside this library time is milliseconds; the conversion lives here and
// nowhere else.

///|
/// An instant as the protocol writes it: ISO 8601, milliseconds, UTC.
fn instant_text(at : Int64) -> String {
  let seconds = if at >= 0L { at / 1000L } else { (at - 999L) / 1000L }
  let nano = ((at - seconds * 1000L) * 1000000L).to_int()
  @moondate.Moment::of_epoch(seconds, nano~, zone=@moondate.Utc).isoformat(
    fraction=3,
    offset=true,
  )
}

///|
/// An instant read back from the protocol's text. A number is accepted too: a
/// producer that writes epoch milliseconds is common enough, and reading it as a
/// date would be wrong by three orders of magnitude.
fn instant_value(
  json : Json,
  path : @json.JsonPath,
) -> Int64 raise @json.JsonDecodeError {
  match json {
    String(text) => instant_of_text(text, path)
    Number(value, ..) => value.to_int64()
    _ => raise @json.JsonDecodeError((path, "expected an instant"))
  }
}

///|
fn instant_of_text(
  text : String,
  path : @json.JsonPath,
) -> Int64 raise @json.JsonDecodeError {
  let moment = @moondate.parse(text) catch {
    _ => raise @json.JsonDecodeError((path, "not an ISO 8601 instant: " + text))
  }
  match moment.timestamp() {
    Some(seconds) => (seconds * 1000.0).round().to_int64()
    None => raise @json.JsonDecodeError((path, "instant out of range: " + text))
  }
}

///|
fn object_value(
  json : Json,
  path : @json.JsonPath,
) -> Map[String, Json] raise @json.JsonDecodeError {
  match json {
    Object(fields) => fields
    _ => raise @json.JsonDecodeError((path, "expected an object"))
  }
}

///|
fn text_value(
  fields : Map[String, Json],
  key : String,
  path : @json.JsonPath,
) -> String raise @json.JsonDecodeError {
  match fields.get(key) {
    Some(String(text)) => text
    Some(_) =>
      raise @json.JsonDecodeError((path.add_key(key), "expected a string"))
    None => raise @json.JsonDecodeError((path, "missing key " + key))
  }
}

///|
fn int_value(
  fields : Map[String, Json],
  key : String,
  path : @json.JsonPath,
) -> Int32 raise @json.JsonDecodeError {
  match fields.get(key) {
    Some(Number(value, ..)) => value.to_int()
    Some(_) =>
      raise @json.JsonDecodeError((path.add_key(key), "expected a number"))
    None => raise @json.JsonDecodeError((path, "missing key " + key))
  }
}

///|
/// The instant under `key`, or `None` when it is absent or null.
fn instant_field(
  fields : Map[String, Json],
  key : String,
  path : @json.JsonPath,
) -> Int64? raise @json.JsonDecodeError {
  match fields.get(key) {
    None | Some(Null) => None
    Some(value) => Some(instant_value(value, path.add_key(key)))
  }
}

///|
pub impl ToJson for Message with fn to_json(self) {
  let out = Map([])
  out["id"] = Json::string(self.id)
  out["task"] = Json::string(self.name)
  out["args"] = self.args
  out["kwargs"] = Json::object(Map([]))
  out["retries"] = Json::number(self.retries.to_double())
  out["queue"] = Json::string(self.queue)
  out["priority"] = Json::number(self.priority.to_double())
  out["utc"] = Json::boolean(true)
  match self.eta {
    Some(at) => out["eta"] = Json::string(instant_text(at))
    None => ()
  }
  match self.expires {
    Some(at) => out["expires"] = Json::string(instant_text(at))
    None => ()
  }
  if self.chain.length() > 0 {
    out["chain"] = Json::array(self.chain.map(fn(m) { Json(m) }))
  }
  match self.chord {
    Some(chord) => out["chord"] = Json(chord)
    None => ()
  }
  Json::object(out)
}

///|
pub extend Message with @json.ToJson::{to_json}

///|
pub impl FromJson for Message with fn from_json(json, path) {
  Message::read(object_value(json, path), path)
}

///|
pub extend Message with @json.FromJson::{from_json}

///|
pub impl ToJson for Chord with fn to_json(self) {
  let out = Map([])
  out["key"] = Json::string(self.key)
  out["index"] = Json::number(self.index.to_double())
  out["total"] = Json::number(self.total.to_double())
  out["ids"] = Json::array(self.ids.map(fn(id) { Json::string(id) }))
  out["callback"] = Json(self.callback)
  Json::object(out)
}

///|
pub extend Chord with @json.ToJson::{to_json}

///|
pub impl FromJson for Chord with fn from_json(json, path) {
  let fields = object_value(json, path)
  let ids = match fields.get("ids") {
    Some(Array(items)) =>
      items.map(fn(item) raise @json.JsonDecodeError {
        match item {
          String(id) => id
          _ =>
            raise @json.JsonDecodeError(
              (path.add_key("ids"), "expected strings"),
            )
        }
      })
    _ => raise @json.JsonDecodeError((path, "missing key ids"))
  }
  let callback = match fields.get("callback") {
    Some(value) => @json.from_json(value, path=path.add_key("callback"))
    None => raise @json.JsonDecodeError((path, "missing key callback"))
  }
  {
    key: text_value(fields, "key", path),
    index: int_value(fields, "index", path),
    total: int_value(fields, "total", path),
    ids,
    callback,
  }
}

///|
pub extend Chord with @json.FromJson::{from_json}

///|
/// The message a JSON object describes. Split from the `FromJson` impl so a
/// chord can decode its callback without going through a trait method.
fn Message::read(
  fields : Map[String, Json],
  path : @json.JsonPath,
) -> Message raise @json.JsonDecodeError {
  let chain = match fields.get("chain") {
    None | Some(Null) => []
    Some(Array(items)) => {
      let decoded = []
      let mut index : Int32 = 0
      for item in items {
        decoded.push(
          @json.from_json(item, path=path.add_key("chain").add_index(index)),
        )
        index = index + 1
      }
      decoded
    }
    Some(_) => raise @json.JsonDecodeError((path, "chain is not a list"))
  }
  let chord = match fields.get("chord") {
    None | Some(Null) => None
    Some(value) => Some(@json.from_json(value, path=path.add_key("chord")))
  }
  {
    id: text_value(fields, "id", path),
    name: text_value(fields, "task", path),
    args: fields.get("args").unwrap_or(Json::null()),
    queue: match fields.get("queue") {
      Some(String(queue)) => queue
      _ => "default"
    },
    priority: match fields.get("priority") {
      Some(Number(value, ..)) => value.to_int()
      _ => 0
    },
    eta: instant_field(fields, "eta", path),
    expires: instant_field(fields, "expires", path),
    retries: match fields.get("retries") {
      Some(Number(value, ..)) => value.to_int()
      _ => 0
    },
    chain,
    chord,
  }
}

///|
/// A message read from its JSON text. The `FromJson` impl is the same reading;
/// this is the entry a broker uses, so a bad payload is refused in this
/// library's terms rather than in the JSON module's.
pub fn Message::parse(text : StringView) -> Message raise Refused {
  let json = @json.parse(text) catch {
    error => raise Refused::Malformed(got=error.to_string())
  }
  read_message(json) catch {
    error => raise Refused::Malformed(got=error.to_string())
  }
}

///|
/// A message read at the root of a document, so the default path can be used.
fn read_message(json : Json) -> Message raise @json.JsonDecodeError {
  @json.from_json(json)
}

///|
/// The JSON text of a message, which is what a broker stores.
pub fn Message::text(self : Message) -> String {
  Json(self).stringify()
}

///|
pub impl ToJson for Outcome with fn to_json(self) {
  let out = Map([])
  out["state"] = Json::string(self.state.code())
  match self.result {
    Some(value) => out["result"] = value
    None => ()
  }
  match self.error {
    Some(error) => out["error"] = Json::string(error)
    None => ()
  }
  out["retries"] = Json::number(self.retries.to_double())
  out["at"] = Json::string(instant_text(self.at))
  Json::object(out)
}

///|
pub extend Outcome with @json.ToJson::{to_json}

///|
pub impl FromJson for Outcome with fn from_json(json, path) {
  let fields = object_value(json, path)
  let state = State::of(text_value(fields, "state", path)) catch {
    _ => raise @json.JsonDecodeError((path.add_key("state"), "unknown state"))
  }
  let result = match fields.get("result") {
    None | Some(Null) => None
    Some(value) => Some(value)
  }
  let error = match fields.get("error") {
    Some(String(message)) => Some(message)
    _ => None
  }
  {
    state,
    result,
    error,
    retries: match fields.get("retries") {
      Some(Number(value, ..)) => value.to_int()
      _ => 0
    },
    at: instant_value(
      fields.get("at").unwrap_or(Json::null()),
      path.add_key("at"),
    ),
  }
}

///|
pub extend Outcome with @json.FromJson::{from_json}
