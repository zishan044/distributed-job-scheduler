//go:build chaos

package chaos

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

type targetCall struct {
	At             time.Time  `json:"at"`
	Path           string     `json:"path"`
	Method         string     `json:"method"`
	IdempotencyKey string     `json:"idempotency_key"`
	Attempt        int        `json:"attempt"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Cancelled      bool       `json:"cancelled"`
}

type targetFixture struct {
	mu       sync.Mutex
	requests []*targetCall
	holds    map[string]chan struct{}
}

func (f *targetFixture) hold(path string, attempt int) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holds == nil {
		f.holds = make(map[string]chan struct{})
	}
	key := path + "/" + strconv.Itoa(attempt)
	release := make(chan struct{})
	f.holds[key] = release
	return sync.OnceFunc(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.holds, key)
		close(release)
	})
}

func (f *targetFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	attempt, err := strconv.Atoi(r.Header.Get("X-Job-Attempt"))
	if err != nil || attempt < 1 || r.Header.Get("Idempotency-Key") == "" {
		http.Error(w, "missing execution identity", http.StatusBadRequest)
		return
	}
	call := &targetCall{At: time.Now().UTC(), Path: r.URL.Path, Method: r.Method,
		IdempotencyKey: r.Header.Get("Idempotency-Key"), Attempt: attempt}
	f.mu.Lock()
	f.requests = append(f.requests, call)
	release := f.holds[r.URL.Path+"/"+strconv.Itoa(attempt)]
	f.mu.Unlock()
	cancelled := false
	if release != nil {
		select {
		case <-release:
		case <-r.Context().Done():
			cancelled = true
		}
	}
	finished := time.Now().UTC()
	f.mu.Lock()
	call.FinishedAt, call.Cancelled = &finished, cancelled
	f.mu.Unlock()
	if !cancelled {
		w.WriteHeader(http.StatusOK)
	}
}

func (f *targetFixture) calls(path string) []targetCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := make([]targetCall, 0)
	for _, call := range f.requests {
		if call.Path == path {
			calls = append(calls, *call)
		}
	}
	return calls
}
