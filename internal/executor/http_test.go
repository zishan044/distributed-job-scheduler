package executor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zishan044/distributed-job-scheduler/internal/store"
)

func TestUnsafeAddressRanges(t *testing.T) {
	for _, ip := range []string{
		"0.1.2.3", "10.0.0.1", "100.64.0.0", "100.127.255.255", "127.0.0.1",
		"169.254.169.254", "172.16.0.1", "172.31.255.255", "192.168.1.1",
		"192.0.0.1", "192.0.2.1", "192.88.99.2", "198.18.0.1", "198.19.255.255",
		"198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1", "255.255.255.255",
		"::", "::1", "::127.0.0.1", "::ffff:127.0.0.1", "::ffff:100.64.0.1",
		"::ffff:240.0.0.1", "64:ff9b::a9fe:a9fe", "64:ff9b:1::1", "100::1",
		"100:0:0:1::1", "2001::1", "2001:2::1", "2001:db8::1", "2002:7f00:1::1",
		"3fff::1", "4000::1", "5f00::1", "fc00::1", "fd00:ec2::254", "fe80::1",
		"fec0::1", "ff02::1", "invalid",
	} {
		if !unsafeIP(net.ParseIP(ip)) {
			t.Errorf("allowed non-public address %s", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "1.1.1.1", "100.63.255.255", "100.128.0.0", "198.17.255.255", "198.20.0.0", "::ffff:8.8.8.8", "2001:4860:4860::8888", "2606:4700:4700::1111"} {
		if unsafeIP(net.ParseIP(ip)) {
			t.Errorf("blocked public address %s", ip)
		}
	}
}

func TestHTTPPolicyHeadersAndResponseLimit(t *testing.T) {
	var redirects atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirects.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer redirectTarget.Close()
	var proxyCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyCalls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("ALL_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	observed := make(chan http.Header, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Header.Clone()
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
		case "/limit":
			_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes)))
		case "/oversize":
			_, _ = w.Write([]byte(strings.Repeat("x", maxResponseBytes+1)))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client := HTTPClient([]string{u.Host})
	defer client.CloseIdleConnections()
	if client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("HTTP transport enables environment proxies")
	}
	runID := uuid.NewString()
	for _, path := range []string{"/ok", "/redirect", "/limit", "/oversize"} {
		payload, _ := json.Marshal(httpPayload{URL: server.URL + path, Headers: map[string]string{
			"idempotency-key": "payload-value", "IDEMPOTENCY-KEY": "also-wrong",
			"x-job-attempt": "bad\r\nvalue", "X-User": "preserved",
		}})
		err := Execute(context.Background(), client, store.DispatchMessage{RunID: runID, Attempt: 3, Type: "http", Payload: payload, TimeoutMS: 2000})
		if wantError := path == "/redirect" || path == "/oversize"; (err != nil) != wantError || (err != nil && !classify(err).Permanent) {
			t.Fatalf("%s execution = %v", path, err)
		}
		headers := <-observed
		if headers.Get("Idempotency-Key") != runID || headers.Get("X-Job-Attempt") != "3" || headers.Get("X-User") != "preserved" || len(headers.Values("Idempotency-Key")) != 1 {
			t.Fatalf("scheduler headers = %v", headers)
		}
	}
	if redirects.Load() != 0 || proxyCalls.Load() != 0 {
		t.Fatal("HTTP client followed a redirect or contacted a proxy")
	}
	for _, target := range []string{"ftp://127.0.0.1", "file:///etc/passwd", "http://", "http://user:password@" + u.Host} {
		payload, _ := json.Marshal(httpPayload{URL: target})
		err := Execute(context.Background(), client, store.DispatchMessage{Type: "http", Payload: payload})
		if err == nil || !classify(err).Permanent {
			t.Fatalf("invalid target %s = %v", target, err)
		}
	}
	blocked := HTTPClient(nil)
	defer blocked.CloseIdleConnections()
	payload, _ := json.Marshal(httpPayload{URL: server.URL})
	if err := Execute(context.Background(), blocked, store.DispatchMessage{Type: "http", Payload: payload}); err == nil || !classify(err).Permanent {
		t.Fatalf("unallowlisted loopback execution = %v", err)
	}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer tlsServer.Close()
	tlsClient := HTTPClient([]string{"127.0.0.1"})
	tlsClient.Transport.(*http.Transport).TLSClientConfig = tlsServer.Client().Transport.(*http.Transport).TLSClientConfig
	defer tlsClient.CloseIdleConnections()
	payload, _ = json.Marshal(httpPayload{URL: tlsServer.URL})
	if err := Execute(context.Background(), tlsClient, store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 2000}); err != nil {
		t.Fatalf("HTTPS execution = %v", err)
	}
}

func TestRetryAfterParsingAndClassification(t *testing.T) {
	date := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	for _, tc := range []struct {
		value string
		after time.Duration
		at    time.Time
	}{
		{"2", 2 * time.Second, time.Time{}},
		{" 12345 ", 12345 * time.Second, time.Time{}},
		{"0", 0, time.Time{}},
		{"-1", 0, time.Time{}},
		{"+1", 0, time.Time{}},
		{"1.5", 0, time.Time{}},
		{"invalid", 0, time.Time{}},
		{"9999999999999999999999999", 0, time.Time{}},
		{"9223372037", 0, time.Time{}},
		{"", 0, time.Time{}},
		{date.Format(http.TimeFormat), 0, date},
	} {
		after, at := parseRetryAfter(tc.value)
		if after != tc.after || !at.Equal(tc.at) {
			t.Errorf("Retry-After %q = %s, %s", tc.value, after, at)
		}
	}
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusBadRequest} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "42")
			w.WriteHeader(status)
		}))
		client := HTTPClient([]string{"127.0.0.1"})
		payload, _ := json.Marshal(httpPayload{URL: server.URL})
		err := Execute(context.Background(), client, store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 2000})
		client.CloseIdleConnections()
		server.Close()
		if err == nil {
			t.Fatal("expected HTTP failure")
		}
		failure := classify(fmt.Errorf("wrapped: %w", err))
		wantAfter := time.Duration(0)
		if status == http.StatusTooManyRequests {
			wantAfter = 42 * time.Second
		}
		if failure.RetryAfter != wantAfter || failure.Permanent != (status == http.StatusBadRequest) {
			t.Fatalf("classified HTTP %d = %+v", status, failure)
		}
	}
}

func TestHTTPDialsResolvedAddressWithoutSecondLookup(t *testing.T) {
	// A local DNS responder changes its A answer on the next lookup. A second
	// resolution inside the dialer would miss the server on 127.0.0.1.
	dns, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var lookups atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		packet := make([]byte, 512)
		for {
			n, peer, err := dns.ReadFrom(packet)
			if err != nil {
				return
			}
			if n < 12 {
				continue
			}
			end := 12
			for end < n && packet[end] != 0 {
				end += int(packet[end]) + 1
			}
			end++
			if end+4 > n {
				continue
			}
			questionType := binary.BigEndian.Uint16(packet[end : end+2])
			response := append([]byte(nil), packet[:end+4]...)
			copy(response[2:12], []byte{0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0})
			if questionType == 1 {
				last := byte(1)
				if lookups.Add(1) > 1 {
					last = 2
				}
				response[7] = 1
				response = append(response, 0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4, 127, 0, 0, last)
			}
			_, _ = dns.WriteTo(response, peer)
		}
	}()
	defer func() { _ = dns.Close(); <-done }()
	previous := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", dns.LocalAddr().String())
	}}
	defer func() { net.DefaultResolver = previous }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client := HTTPClient([]string{"checked.test"})
	defer client.CloseIdleConnections()
	payload, _ := json.Marshal(httpPayload{URL: "http://checked.test:" + u.Port()})
	if err := Execute(context.Background(), client, store.DispatchMessage{Type: "http", Payload: payload, TimeoutMS: 2000}); err != nil {
		t.Fatal(err)
	}
	if got := lookups.Load(); got != 1 {
		t.Fatalf("DNS A lookups = %s; want one", strconv.Itoa(int(got)))
	}
}
