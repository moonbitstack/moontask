// The management application: routes, requests and the events it sends back.

///|
/// A management interface over one app.
///
/// The app is the only thing it needs to send and revoke; the queues it reports
/// come from the broker's `Inspector`, which a broker may or may not implement,
/// and a page shows what it can when it is absent.
pub struct Admin {
  app : @moontask.App
  inspector : &@tspec.Inspector?
  /// How many count frames the event stream sends before it ends. 0 streams
  /// until the server closes the connection.
  frames : Int32
  /// How long between those frames, in milliseconds.
  interval : Int64
}

///|
/// A management interface over `app`, reporting the queues of `inspector` when
/// one is given.
///
/// `frames` and `interval` shape the live stream: a page that stays open wants
/// an endless one, and a test wants a short one that finishes.
pub fn Admin::new(
  app : @moontask.App,
  inspector? : &@tspec.Inspector,
  frames~ : Int32 = 0,
  interval~ : Int64 = 1000L,
) -> Admin {
  { app, inspector, frames, interval, }
}

///|
/// The application, as one ASGI callable.
///
/// `GET /` is the page, `GET /api/tasks`, `GET /api/queues` and
/// `GET /api/results/{id}` are the reports, `POST /api/tasks/{name}` sends a run,
/// `POST /api/results/{id}/revoke` cancels one, and `GET /api/events` is the
/// server-sent event stream of queue counts.
pub fn Admin::to_asgi(self : Admin) -> @moonasgi.AsgiApp {
  async fn(scope, receive, send) {
    match scope {
      @spec.Scope::Http(http) =>
        self.handle_http(http, receive, send) catch {
          error => {
            let body = Json::object({ "error": Json::string(error.to_string()) })
            self.send_json(send, 500, body)
          }
        }
      @spec.Scope::Lifespan(_) =>
        // The application holds no state of its own to open or close.
        ()
      @spec.Scope::WebSocket(_) =>
        self.send_json(
          send,
          400,
          Json::object({ "error": Json::string("websocket is not served here") }),
        )
    }
  }
}

///|
async fn Admin::handle_http(
  self : Admin,
  scope : @spec.HttpScope,
  receive : async () -> @spec.Event,
  send : async (@spec.Event) -> Unit,
) -> Unit {
  let path = scope.path
  let verb = scope.http_method
  match (verb, path) {
    ("GET", "/") => self.send_html(send, page())
    ("GET", "/api/tasks") => self.send_json(send, 200, self.tasks_json())
    ("GET", "/api/queues") => self.send_json(send, 200, self.queues_json())
    ("GET", "/api/events") => self.stream_events(send)
    ("POST", _) => {
      let body = read_body(receive)
      if path.has_prefix("/api/results/") && path.has_suffix("/revoke") {
        let id = between(path, "/api/results/", "/revoke")
        self.revoke(id) catch {
          @tspec.Refused::Missing(_) =>
            self.send_json(
              send,
              409,
              Json::object({
                "error": Json::string("this app keeps no results"),
              }),
            )
          error => raise error
        }
        self.send_json(send, 200, Json::object({ "revoked": Json::string(id) }))
      } else if path.has_prefix("/api/tasks/") {
        let name = after(path, "/api/tasks/")
        let args = @json.parse(body) catch {
          _ => raise @tspec.Refused::Malformed(got=body)
        }
        let handle = self.app.send(name, args) catch {
          @tspec.Refused::Unknown(_) => {
            self.send_json(
              send,
              404,
              Json::object({
                "error": Json::string("no task registered as " + name),
              }),
            )
            return
          }
          error => raise error
        }
        self.send_json(
          send,
          202,
          Json::object({ "id": Json::string(handle.ids()[0]) }),
        )
      } else {
        self.send_json(
          send,
          404,
          Json::object({ "error": Json::string("no such route") }),
        )
      }
    }
    ("GET", _) =>
      if path.has_prefix("/api/results/") {
        let id = after(path, "/api/results/")
        self.send_json(send, 200, self.result_json(id))
      } else {
        self.send_json(
          send,
          404,
          Json::object({ "error": Json::string("no such route") }),
        )
      }
    (_, _) =>
      self.send_json(
        send,
        405,
        Json::object({ "error": Json::string("method not allowed") }),
      )
  }
}

///|
/// The registered tasks, with the policy each runs under.
fn Admin::tasks_json(self : Admin) -> Json {
  let tasks = []
  for name in self.app.task_names() {
    match self.app.policy_of(name) {
      Some(policy) =>
        tasks.push(
          Json::object({
            "name": Json::string(name),
            "queue": Json::string(policy.queue),
            "retries": Json::number(policy.retries.to_double()),
            "timeout": match policy.timeout {
              Some(limit) => Json::number(limit.to_double())
              None => Json::null()
            },
            "acks_late": Json::boolean(policy.acks_late),
            "rate": match policy.rate {
              Some(rate) =>
                Json::object({
                  "count": Json::number(rate.count.to_double()),
                  "per": Json::number(rate.per.to_double()),
                })
              None => Json::null()
            },
          }),
        )
      None => ()
    }
  }
  Json::array(tasks)
}

///|
/// What each queue holds, or an empty list when the broker cannot say.
async fn Admin::queues_json(self : Admin) -> Json raise @tspec.Refused {
  let inspector = match self.inspector {
    Some(inspector) => inspector
    None => return Json::array([])
  }
  let queues = []
  for info in inspector.queues() {
    queues.push(
      Json::object({
        "name": Json::string(info.name),
        "ready": Json::number(info.ready.to_double()),
        "delayed": Json::number(info.delayed.to_double()),
        "unacked": Json::number(info.unacked.to_double()),
      }),
    )
  }
  Json::array(queues)
}

///|
/// The result of one run: its state and the value or the error.
async fn Admin::result_json(
  self : Admin,
  id : String,
) -> Json raise @tspec.Refused {
  let backend = match self.app.backend_of() {
    Some(backend) => backend
    None => raise @tspec.Refused::Missing(kind="backend")
  }
  match backend.fetch(id) {
    Some(outcome) =>
      Json::object({
        "id": Json::string(id),
        "state": Json::string(outcome.state.code()),
        "result": outcome.result.unwrap_or(Json::null()),
        "error": match outcome.error {
          Some(error) => Json::string(error)
          None => Json::null()
        },
        "retries": Json::number(outcome.retries.to_double()),
        "at": Json::number(outcome.at.to_double()),
      })
    None =>
      Json::object({ "id": Json::string(id), "state": Json::string("PENDING") })
  }
}

///|
/// Cancel one run.
async fn Admin::revoke(self : Admin, id : String) -> Unit raise @tspec.Refused {
  let backend = match self.app.backend_of() {
    Some(backend) => backend
    None => raise @tspec.Refused::Missing(kind="backend")
  }
  let outcome : @tspec.Outcome = {
    state: @tspec.Revoked,
    result: None,
    error: None,
    retries: 0,
    at: self.app.now(),
  }
  backend.store(id, outcome)
}

///|
/// Send a frame of queue counts every `interval`, `frames` times.
async fn Admin::stream_events(
  self : Admin,
  send : async (@spec.Event) -> Unit,
) -> Unit {
  send(
    @spec.Event::HttpResponseStart(
      status=200,
      headers=[
        ("content-type", "text/event-stream"),
        ("cache-control", "no-cache"),
        ("connection", "keep-alive"),
      ],
      trailers=false,
    ),
  )
  let mut sent = 0
  while self.frames <= 0 || sent < self.frames {
    let counts = self.queues_json()
    let frame = "data: " + frame_text(counts) + "\n\n"
    let stopped = try {
      send(
        @spec.Event::HttpResponseBody(body=@utf8.encode(frame), more_body=true),
      )
      false
    } catch {
      _ => true
    }
    if stopped {
      return
    }
    sent = sent + 1
    @wait.nap(self.interval.to_int())
  }
  send(@spec.Event::HttpResponseBody(body=b"", more_body=false))
}

///|
fn frame_text(counts : Json) -> String {
  counts.stringify()
}

///|
async fn Admin::send_json(
  self : Admin,
  send : async (@spec.Event) -> Unit,
  status : Int32,
  body : Json,
) -> Unit {
  ignore(self)
  send(
    @spec.Event::HttpResponseStart(
      status~,
      headers=[("content-type", "application/json; charset=utf-8")],
      trailers=false,
    ),
  )
  send(
    @spec.Event::HttpResponseBody(
      body=@utf8.encode(body.stringify()),
      more_body=false,
    ),
  )
}

///|
async fn Admin::send_html(
  self : Admin,
  send : async (@spec.Event) -> Unit,
  body : String,
) -> Unit {
  ignore(self)
  send(
    @spec.Event::HttpResponseStart(
      status=200,
      headers=[("content-type", "text/html; charset=utf-8")],
      trailers=false,
    ),
  )
  send(@spec.Event::HttpResponseBody(body=@utf8.encode(body), more_body=false))
}

///|
/// The whole request body, however many chunks it came in.
async fn read_body(receive : async () -> @spec.Event) -> String {
  let builder = StringBuilder()
  let mut more = true
  while more {
    let event = receive()
    match event {
      @spec.Event::HttpRequest(body~, more_body~) => {
        builder.write_string(@utf8.decode_lossy(body[:]))
        more = more_body
      }
      @spec.Event::HttpDisconnect => break
      _ => break
    }
  }
  builder.to_string()
}

///|
fn after(path : String, prefix : String) -> String {
  path.unsafe_substring(start=prefix.length(), end=path.length())
}

///|
fn between(path : String, prefix : String, suffix : String) -> String {
  let rest = after(path, prefix)
  rest.unsafe_substring(start=0, end=rest.length() - suffix.length())
}
