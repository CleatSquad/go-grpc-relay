# go-grpc-relay

Generic transport-and-dispatch plumbing for the "local Go relay" pattern: a
process in a language without a performant native gRPC/HTTP2 client (e.g.
PHP) talks plain HTTP/1.1 + raw protobuf over a local Unix domain socket, and
a companion Go process makes the real gRPC/HTTP2 call on its behalf. This
avoids parsing gRPC framing (trailers, HTTP/2) in userland — that cost moves
into a native Go gRPC client with its own persistent channel.

The relay package handles only the generic transport layer:

- HTTP routing for `POST /rpc/{Service}/{Method}`, with raw protobuf request
  and response bodies (no JSON envelope).
- A `Registry` mapping short service/method names to gRPC dispatch info
  (unary via `conn.Invoke`, server-streaming via `conn.NewStream`) — no
  generated typed client needed.
- A length-prefixed binary frame format for streaming responses (protobuf
  payloads can contain arbitrary bytes, so a line delimiter would not work).
- JSON error envelopes for HTTP-level failures.
- A gRPC health check handler backed by the standard
  `grpc.health.v1.Health` service.
- Graceful lifecycle for a UDS-listening HTTP server (SIGINT/SIGTERM
  shutdown).

Building the actual service registry — which RPCs exist and how to construct
their request/response messages — is inherently specific to each relay and
is left to the caller's own `main()`.

## Install

```bash
go get github.com/CleatSquad/go-grpc-relay
```

## Usage

```go
registry := relay.Registry{
    "FooService/Bar": {
        FullMethod: "/example.v1.FooService/Bar",
        NewReq:     func() proto.Message { return &examplev1.BarRequest{} },
        NewResp:    func() proto.Message { return &examplev1.BarResponse{} },
    },
}

mux := http.NewServeMux()
mux.HandleFunc("/rpc/", relay.HandleRPC(conn, registry, nil))
mux.HandleFunc("/health", relay.HandleHealth(conn))

log.Fatal(relay.Serve("[my-relay]", "/tmp/my-relay.sock", mux))
```

## License

MIT — see [LICENSE](LICENSE).
