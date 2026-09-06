# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-09-06

### Added

- `Registry`/`HandlerEntry`: short-name RPC registry dispatching unary and
  server-streaming calls without a generated typed client.
- `HandleRPC`: HTTP handler for `POST /rpc/{Service}/{Method}` with raw
  binary protobuf request/response bodies and a `X-Timeout-Ms` deadline
  override.
- `WriteFrame`/`FrameTypeData`/`FrameTypeError`: length-prefixed binary
  framing for streamed responses.
- `HandleHealth`: HTTP health endpoint backed by the standard
  `grpc.health.v1.Health` service.
- `WriteJSONError`/`JSONErrorResponse`: JSON error envelope for HTTP-level
  failures.
- `Serve`: UDS-listening HTTP server with graceful SIGINT/SIGTERM shutdown.
