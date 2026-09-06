package relay

import (
	"bytes"
	"net/http/httptest"
	"testing"
)

func TestNormalizeService(t *testing.T) {
	cases := map[string]string{
		"FooService":            "FooService",
		"example.v1.FooService": "FooService",
		"a.b.c.d.FooService":    "FooService",
		"":                      "",
	}
	for in, want := range cases {
		if got := NormalizeService(in); got != want {
			t.Errorf("NormalizeService(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseRPCPath(t *testing.T) {
	cases := []struct {
		path        string
		wantService string
		wantMethod  string
		wantOK      bool
	}{
		{"/rpc/FooService/Bar", "FooService", "Bar", true},
		{"/rpc/example.v1.FooService/Bar", "example.v1.FooService", "Bar", true},
		{"/rpc/FooService/", "", "", false},
		{"/rpc/FooService", "", "", false},
		{"/rpc/", "", "", false},
		{"/rpc", "", "", false},
		{"/other/FooService/Bar", "", "", false},
		{"", "", "", false},
	}
	for _, c := range cases {
		service, method, ok := ParseRPCPath(c.path)
		if ok != c.wantOK || service != c.wantService || method != c.wantMethod {
			t.Errorf("ParseRPCPath(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.path, service, method, ok, c.wantService, c.wantMethod, c.wantOK)
		}
	}
}

func TestRegistryGet(t *testing.T) {
	reg := Registry{
		"FooService/Bar": HandlerEntry{FullMethod: "/example.v1.FooService/Bar"},
	}

	if _, ok := reg.Get("example.v1.FooService", "Bar"); !ok {
		t.Error("expected registry hit for known service/method")
	}
	if _, ok := reg.Get("example.v1.FooService", "Missing"); ok {
		t.Error("expected registry miss for unknown method")
	}
	if _, ok := reg.Get("UnknownService", "Bar"); ok {
		t.Error("expected registry miss for unknown service")
	}
}

func TestWriteFrame(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("hello")

	if err := WriteFrame(&buf, FrameTypeData, payload); err != nil {
		t.Fatalf("WriteFrame returned error: %v", err)
	}

	got := buf.Bytes()
	if len(got) != 5+len(payload) {
		t.Fatalf("frame length = %d, want %d", len(got), 5+len(payload))
	}
	if got[0] != FrameTypeData {
		t.Errorf("frame type = %d, want %d", got[0], FrameTypeData)
	}
	length := uint32(got[1])<<24 | uint32(got[2])<<16 | uint32(got[3])<<8 | uint32(got[4])
	if int(length) != len(payload) {
		t.Errorf("frame length prefix = %d, want %d", length, len(payload))
	}
	if !bytes.Equal(got[5:], payload) {
		t.Errorf("frame payload = %q, want %q", got[5:], payload)
	}
}

func TestWriteFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, FrameTypeError, nil); err != nil {
		t.Fatalf("WriteFrame returned error: %v", err)
	}
	if buf.Len() != 5 {
		t.Fatalf("frame length = %d, want 5", buf.Len())
	}
}

func TestWriteJSONError(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSONError(rec, 400, "BAD_REQUEST", "something went wrong")

	if rec.Code != 400 {
		t.Errorf("status code = %d, want 400", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	want := `{"error":{"code":"BAD_REQUEST","message":"something went wrong"}}` + "\n"
	if got := rec.Body.String(); got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}
