// Package relay implements the generic half of the "local Go relay" pattern:
// a process in a language without a performant native gRPC/HTTP2 client
// (e.g. PHP) talks plain HTTP/1.1 + raw protobuf over a local Unix domain
// socket, and a companion Go process makes the real gRPC/HTTP2 call on its
// behalf. This avoids parsing gRPC framing (trailers, HTTP/2) in userland
// via cURL — that cost moves into a native Go gRPC client with its own
// persistent channel.
//
// Only the transport-and-dispatch plumbing lives here: HTTP routing, path
// parsing, JSON error envelopes, streaming frames, UDS listener lifecycle.
// The service-specific registry (which RPCs exist, their request/response
// types) is built by each relay's own main() and passed in — that part is
// inherently different per service and does not belong in a shared package.
package relay

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func withOutgoingMetadata(ctx context.Context, md map[string]string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.New(md))
}

// HandlerEntry describes one RPC: its full gRPC method name and how to
// build empty request/response messages for it. Dispatch itself goes
// through conn.Invoke (unary) or conn.NewStream (server-streaming), so a
// relay never needs a generated typed client — this generic path is
// exactly what a typed client does under the hood.
type HandlerEntry struct {
	FullMethod string
	NewReq     func() proto.Message
	NewResp    func() proto.Message
	IsStream   bool
}

// Registry maps "{Service}/{Method}" (short service name, no package
// prefix) to its handler entry.
type Registry map[string]HandlerEntry

// NormalizeService strips any dotted package prefix, keeping only the last
// segment: "example.v1.FooService" -> "FooService".
func NormalizeService(service string) string {
	parts := strings.Split(service, ".")
	return parts[len(parts)-1]
}

// Get looks up a handler by (possibly dotted) service name and method.
func (r Registry) Get(service, method string) (HandlerEntry, bool) {
	entry, ok := r[NormalizeService(service)+"/"+method]
	return entry, ok
}

// ParseRPCPath extracts {Service} and {Method} from "/rpc/{Service}/{Method}".
func ParseRPCPath(path string) (service, method string, ok bool) {
	trimmed := strings.TrimPrefix(path, "/rpc/")
	if trimmed == path {
		return "", "", false
	}
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// Server-streaming frame format on the wire: 1-byte frame type, then a
// 4-byte big-endian length, then that many bytes of payload. Binary
// protobuf can contain '\n', so this needs an explicit length prefix
// instead of a line delimiter.
const (
	FrameTypeData  byte = 0
	FrameTypeError byte = 1
)

// StreamContentType is the Content-Type advertised for server-streaming
// responses (see WriteFrame). It identifies the framed-stream wire format,
// not any particular consumer service.
const StreamContentType = "application/x-rpc-stream"

func WriteFrame(w io.Writer, frameType byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = frameType
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

type JSONErrorResponse struct {
	Error JSONErrorDetail `json:"error"`
}

type JSONErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func WriteJSONError(w http.ResponseWriter, statusCode int, code string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(JSONErrorResponse{Error: JSONErrorDetail{Code: code, Message: message}})
}

// HandleHealth checks real connectivity to the backend's gRPC health
// service, not just "this relay process is up" (a static 200 would always
// say UP even if the backend is unreachable, defeating the point).
func HandleHealth(conn *grpc.ClientConn) http.HandlerFunc {
	client := healthpb.NewHealthClient(conn)

	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		w.Header().Set("Content-Type", "application/json")

		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "DOWN", "error": status.Convert(err).Message()})
			return
		}

		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "DOWN", "backend": resp.GetStatus().String()})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP"})
	}
}

// MetadataFromRequest extracts outgoing gRPC metadata (e.g. an
// authenticated user id) from the incoming HTTP request. Return nil/empty
// for relays that have nothing to forward.
type MetadataFromRequest func(*http.Request) map[string]string

// HandleRPC expects POST /rpc/{Service}/{Method}, with the request payload
// as a raw binary protobuf body (Content-Type: application/x-protobuf) — no
// JSON envelope. X-Timeout-Ms overrides the per-call deadline.
func HandleRPC(conn *grpc.ClientConn, registry Registry, metadataFn MetadataFromRequest) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			WriteJSONError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST is supported")
			return
		}

		service, method, ok := ParseRPCPath(r.URL.Path)
		if !ok {
			WriteJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "Expected path /rpc/{Service}/{Method}")
			return
		}

		entry, ok := registry.Get(service, method)
		if !ok {
			WriteJSONError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("Unknown RPC method: %s/%s", service, method))
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			WriteJSONError(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("Failed to read body: %v", err))
			return
		}
		defer r.Body.Close()

		timeout := 170 * time.Second
		if ms, err := strconv.ParseInt(r.Header.Get("X-Timeout-Ms"), 10, 64); err == nil && ms > 0 {
			timeout = time.Duration(ms) * time.Millisecond
		}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		if metadataFn != nil {
			if md := metadataFn(r); len(md) > 0 {
				ctx = withOutgoingMetadata(ctx, md)
			}
		}

		reqMsg := entry.NewReq()
		if len(body) > 0 {
			if err := proto.Unmarshal(body, reqMsg); err != nil {
				WriteJSONError(w, http.StatusBadRequest, "INVALID_PAYLOAD", fmt.Sprintf("Failed to unmarshal payload: %v", err))
				return
			}
		}

		if !entry.IsStream {
			respMsg := entry.NewResp()
			if err := conn.Invoke(ctx, entry.FullMethod, reqMsg, respMsg); err != nil {
				st := status.Convert(err)
				WriteJSONError(w, http.StatusBadGateway, st.Code().String(), st.Message())
				return
			}

			respBytes, err := proto.Marshal(respMsg)
			if err != nil {
				WriteJSONError(w, http.StatusInternalServerError, "INTERNAL", fmt.Sprintf("Failed to marshal response: %v", err))
				return
			}

			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
			return
		}

		serveStream(w, ctx, conn, entry, reqMsg)
	}
}

func serveStream(w http.ResponseWriter, ctx context.Context, conn *grpc.ClientConn, entry HandlerEntry, reqMsg proto.Message) {
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, entry.FullMethod)
	if err != nil {
		st := status.Convert(err)
		WriteJSONError(w, http.StatusBadGateway, st.Code().String(), st.Message())
		return
	}

	if err := stream.SendMsg(reqMsg); err != nil {
		st := status.Convert(err)
		WriteJSONError(w, http.StatusBadGateway, st.Code().String(), fmt.Sprintf("Failed to send stream request: %v", st.Message()))
		return
	}
	if err := stream.CloseSend(); err != nil {
		st := status.Convert(err)
		WriteJSONError(w, http.StatusBadGateway, st.Code().String(), fmt.Sprintf("Failed to close stream send: %v", st.Message()))
		return
	}

	w.Header().Set("Content-Type", StreamContentType)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	flusher, isFlusher := w.(http.Flusher)

	for {
		eventMsg := entry.NewResp()
		err := stream.RecvMsg(eventMsg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			st := status.Convert(err)
			errBytes, _ := json.Marshal(JSONErrorDetail{Code: st.Code().String(), Message: st.Message()})
			_ = WriteFrame(w, FrameTypeError, errBytes)
			if isFlusher {
				flusher.Flush()
			}
			break
		}

		eventBytes, err := proto.Marshal(eventMsg)
		if err != nil {
			log.Printf("[grpc-relay] Failed to marshal event: %v", err)
			break
		}

		if err := WriteFrame(w, FrameTypeData, eventBytes); err != nil {
			break
		}
		if isFlusher {
			flusher.Flush()
		}
	}
}

// Serve starts an HTTP server on a Unix domain socket, blocking until it
// receives SIGINT/SIGTERM (graceful shutdown) or the listener errors out.
// logPrefix tags log lines (e.g. "[my-relay]").
func Serve(logPrefix, socketPath string, mux *http.ServeMux) error {
	_ = os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on unix socket %s: %w", socketPath, err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)

	if err := os.Chmod(socketPath, 0o777); err != nil {
		log.Printf("%s Warning: chmod 0777 on socket failed: %v", logPrefix, err)
	}

	server := &http.Server{Handler: mux}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Printf("%s Shutting down...", logPrefix)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = os.Remove(socketPath)
	}()

	log.Printf("%s Listening on unix socket %s", logPrefix, socketPath)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server error: %w", err)
	}
	return nil
}
