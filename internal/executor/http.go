package executor

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponseBytes = 1 << 20

// Conservative exclusions for IANA special-purpose ranges, including protocol
// assignments and transition mechanisms. Explicit allowlisting still applies.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

var publicIPv6 = netip.MustParsePrefix("2000::/3")

func HTTPClient(allowedHosts []string) *http.Client {
	allowed := make(map[string]struct{}, len(allowedHosts))
	for _, host := range allowedHosts {
		allowed[strings.ToLower(host)] = struct{}{}
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, permanent(err)
			}
			_, trusted := allowed[strings.ToLower(net.JoinHostPort(host, port))]
			if !trusted {
				_, trusted = allowed[strings.ToLower(host)]
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			var lastErr error
			for _, addr := range ips {
				if !trusted && unsafeIP(addr.IP) {
					if lastErr == nil {
						lastErr = permanent(fmt.Errorf("target resolves to a non-public address"))
					}
					continue
				}
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(addr.String(), port))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			if lastErr == nil {
				lastErr = fmt.Errorf("target did not resolve to an address")
			}
			return nil, lastErr
		},
		TLSHandshakeTimeout: 5 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func unsafeIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return true
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return true
	}
	// Currently allocated global IPv6 unicast space; translated, site-local,
	// and reserved space require allowlisting instead of bypassing IPv4 checks.
	if addr.Is6() && !publicIPv6.Contains(addr) {
		return true
	}
	for _, prefix := range blockedPrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func doHTTP(ctx context.Context, client *http.Client, payload httpPayload, runID string, attempt int) error {
	u, err := url.Parse(payload.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return permanent(fmt.Errorf("invalid HTTP target URL"))
	}
	method := payload.Method
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), strings.NewReader(payload.Body))
	if err != nil {
		return permanent(fmt.Errorf("invalid HTTP request: %w", err))
	}
	for name, value := range payload.Headers {
		if strings.EqualFold(name, "Idempotency-Key") || strings.EqualFold(name, "X-Job-Attempt") {
			continue
		}
		if !validHeader(name, value) {
			return permanent(fmt.Errorf("invalid HTTP header %q", name))
		}
		req.Header.Set(name, value)
	}
	req.Header.Set("Idempotency-Key", runID)
	req.Header.Set("X-Job-Attempt", strconv.Itoa(attempt))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxResponseBytes {
		return permanent(fmt.Errorf("HTTP response exceeds %d bytes", maxResponseBytes))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("HTTP target returned status %d", resp.StatusCode)
		if resp.StatusCode == http.StatusTooManyRequests {
			after, at := parseRetryAfter(resp.Header.Get("Retry-After"))
			return retryAfterError{error: err, after: after, at: at}
		}
		if resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			return err
		}
		return permanent(err)
	}
	return nil
}

type retryAfterError struct {
	error
	after time.Duration
	at    time.Time
}

func (e retryAfterError) Unwrap() error { return e.error }

func parseRetryAfter(value string) (time.Duration, time.Time) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, time.Time{}
	}
	digits := true
	for _, char := range value {
		if char < '0' || char > '9' {
			digits = false
			break
		}
	}
	if digits {
		seconds, err := strconv.ParseInt(value, 10, 64)
		if err == nil && seconds <= int64((1<<63-1)/time.Second) {
			return time.Duration(seconds) * time.Second, time.Time{}
		}
		return 0, time.Time{}
	}
	// The store compares dates against DB time, so past dates naturally fall
	// below ordinary backoff without relying on the executor's wall clock.
	at, err := http.ParseTime(value)
	if err != nil {
		return 0, time.Time{}
	}
	return 0, at
}

func validHeader(name, value string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	for _, c := range value {
		if (c < 32 && c != '\t') || c == 127 {
			return false
		}
	}
	return true
}
