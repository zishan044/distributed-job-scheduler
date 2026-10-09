package executor

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseBytes = 1 << 20

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
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(addr.IP.String(), port))
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
	return !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
}

func doHTTP(ctx context.Context, client *http.Client, payload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}) error {
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
		if !validHeader(name, value) {
			return permanent(fmt.Errorf("invalid HTTP header %q", name))
		}
		req.Header.Set(name, value)
	}
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
		if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode <= 599) {
			return err
		}
		return permanent(err)
	}
	return nil
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
