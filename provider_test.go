package httpprovider

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	poll    = 20 * time.Millisecond
	waitFor = 2 * time.Second
)

// fakeServer serves a mutable body and status and counts requests.
type fakeServer struct {
	*httptest.Server
	mu     sync.Mutex
	status int
	body   string
	hits   atomic.Int32
}

func newServer(t *testing.T, body string) *fakeServer {
	t.Helper()
	s := &fakeServer{status: http.StatusOK, body: body}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		s.hits.Add(1)
		s.mu.Lock()
		status, body := s.status, s.body
		s.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *fakeServer) set(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.body = status, body
}

func TestReadBytes(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		opts    []Option
		want    string
		wantErr string
	}{
		{
			name:    "success",
			handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("a: 1")) },
			want:    "a: 1",
		},
		{
			name: "headers sent",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer x" || r.Header.Get("X-Env") != "prod" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				_, _ = w.Write([]byte("ok"))
			},
			opts: []Option{WithHeaders(http.Header{"Authorization": {"Bearer x"}, "X-Env": {"prod"}})},
			want: "ok",
		},
		{
			name:    "non-200",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) },
			wantErr: "unexpected status 404",
		},
		{
			name:    "body at max size",
			handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("0123456789")) },
			opts:    []Option{WithMaxSize(10)},
			want:    "0123456789",
		},
		{
			name:    "body over max size",
			handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("0123456789X")) },
			opts:    []Option{WithMaxSize(10)},
			wantErr: "exceeds max size of 10 bytes",
		},
		{
			name: "client timeout",
			handler: func(_ http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-time.After(waitFor):
				}
			},
			opts:    []Option{WithClient(&http.Client{Timeout: 50 * time.Millisecond})},
			wantErr: "Client.Timeout",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			got, err := Provider(srv.URL, tt.opts...).ReadBytes()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func TestReadBytesInvalidURL(t *testing.T) {
	_, err := Provider("://bad").ReadBytes()
	assert.Error(t, err)
}

func TestReadBytesValidationFails(t *testing.T) {
	srv := newServer(t, "bad")
	cache := filepath.Join(t.TempDir(), "cache.yaml")
	errBad := errors.New("bad config")

	_, err := Provider(srv.URL,
		WithCache(cache),
		WithValidate(func([]byte) error { return errBad }),
	).ReadBytes()

	require.ErrorIs(t, err, errBad)
	assert.NoFileExists(t, cache)
}

func TestReadBytesWritesCache(t *testing.T) {
	srv := newServer(t, "a: 1")
	dir := t.TempDir()
	cache := filepath.Join(dir, "cache.yaml")
	require.NoError(t, os.WriteFile(cache, []byte("old"), 0o644))

	got, err := Provider(srv.URL, WithCache(cache)).ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "a: 1", string(got))

	b, err := os.ReadFile(cache)
	require.NoError(t, err)
	assert.Equal(t, "a: 1", string(b))
	fi, err := os.Stat(cache)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "temp files left behind")
	assert.Equal(t, "cache.yaml", entries[0].Name())
}

func TestReadBytesCacheWriteFailureStillLoads(t *testing.T) {
	srv := newServer(t, "a: 1")
	cache := filepath.Join(t.TempDir(), "missing-dir", "cache.yaml")

	got, err := Provider(srv.URL, WithCache(cache)).ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "a: 1", string(got))
	assert.NoFileExists(t, cache)
}

func TestReadBytesFallback(t *testing.T) {
	srv := newServer(t, "")
	srv.set(http.StatusInternalServerError, "")
	cachedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	writeCache := func(t *testing.T, path string) {
		t.Helper()
		require.NoError(t, os.WriteFile(path, []byte("cached: true"), 0o600))
		require.NoError(t, os.Chtimes(path, cachedAt, cachedAt))
	}

	tests := []struct {
		name         string
		setup        func(t *testing.T, path string)
		noCache      bool
		want         string
		wantErr      []string
		wantFallback bool
	}{
		{
			name:         "cache present",
			setup:        writeCache,
			want:         "cached: true",
			wantFallback: true,
		},
		{
			name:    "no cache file",
			setup:   func(*testing.T, string) {},
			wantErr: []string{"unexpected status 500"},
		},
		{
			name:    "no cache configured",
			noCache: true,
			wantErr: []string{"unexpected status 500"},
		},
		{
			name:    "cache unreadable",
			setup:   func(t *testing.T, path string) { t.Helper(); require.NoError(t, os.Mkdir(path, 0o700)) },
			wantErr: []string{"unexpected status 500", "is a directory"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []Option
			if !tt.noCache {
				path := filepath.Join(t.TempDir(), "cache.yaml")
				tt.setup(t, path)
				opts = append(opts, WithCache(path))
			}
			var gotErr error
			var gotAt time.Time
			called := false
			opts = append(opts, WithOnFallback(func(err error, at time.Time) {
				called, gotErr, gotAt = true, err, at
			}))

			got, err := Provider(srv.URL, opts...).ReadBytes()
			assert.Equal(t, tt.wantFallback, called)
			if len(tt.wantErr) > 0 {
				require.Error(t, err)
				for _, want := range tt.wantErr {
					assert.Contains(t, err.Error(), want)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
			assert.Contains(t, gotErr.Error(), "unexpected status 500")
			assert.True(t, cachedAt.Equal(gotAt), "mtime: got %v want %v", gotAt, cachedAt)
		})
	}
}

func TestRead(t *testing.T) {
	_, err := Provider("http://example.invalid").Read()
	assert.EqualError(t, err, "httpprovider does not support Read()")
}

func TestOptionsIgnoreInvalidValues(t *testing.T) {
	p := Provider("http://example.invalid", WithClient(nil), WithPollInterval(0), WithMaxSize(-1))
	assert.NotNil(t, p.client)
	assert.Equal(t, defaultPollInterval, p.pollInterval)
	assert.Equal(t, int64(defaultMaxSize), p.maxSize)
}

// watcher records Watch callbacks.
type watcher struct {
	reloads atomic.Int32
	errs    atomic.Int32
}

func (w *watcher) cb(_ any, err error) {
	if err != nil {
		w.errs.Add(1)
		return
	}
	w.reloads.Add(1)
}

func startWatch(t *testing.T, p *HTTP) *watcher {
	t.Helper()
	w := &watcher{}
	require.NoError(t, p.Watch(w.cb))
	t.Cleanup(func() { _ = p.Unwatch() })
	return w
}

func TestWatchChangeTriggersOneReload(t *testing.T) {
	srv := newServer(t, "v: 1")
	cache := filepath.Join(t.TempDir(), "cache.yaml")
	p := Provider(srv.URL, WithPollInterval(poll), WithCache(cache))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	srv.set(http.StatusOK, "v: 2")

	assert.Eventually(t, func() bool { return w.reloads.Load() == 1 }, waitFor, poll/2)
	assert.Never(t, func() bool { return w.reloads.Load() > 1 }, 5*poll, poll/2)
	assert.Zero(t, w.errs.Load())
	b, err := os.ReadFile(cache)
	require.NoError(t, err)
	assert.Equal(t, "v: 2", string(b))
}

func TestWatchUnchangedContentDoesNothing(t *testing.T) {
	srv := newServer(t, "v: 1")
	p := Provider(srv.URL, WithPollInterval(poll))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	assert.Eventually(t, func() bool { return srv.hits.Load() >= 5 }, waitFor, poll/2)
	assert.Zero(t, w.reloads.Load())
	assert.Zero(t, w.errs.Load())
}

func TestWatchFailingServerReportsError(t *testing.T) {
	srv := newServer(t, "v: 1")
	p := Provider(srv.URL, WithPollInterval(poll))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	srv.set(http.StatusBadGateway, "")

	assert.Eventually(t, func() bool { return w.errs.Load() > 0 }, waitFor, poll/2)
	assert.Zero(t, w.reloads.Load())
}

func TestWatchInvalidContentDoesNotReload(t *testing.T) {
	srv := newServer(t, "v: 1")
	cache := filepath.Join(t.TempDir(), "cache.yaml")
	p := Provider(srv.URL,
		WithPollInterval(poll),
		WithCache(cache),
		WithValidate(func(b []byte) error {
			if strings.Contains(string(b), "bad") {
				return errors.New("bad config")
			}
			return nil
		}),
	)
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	srv.set(http.StatusOK, "bad")

	assert.Eventually(t, func() bool { return w.errs.Load() > 0 }, waitFor, poll/2)
	assert.Zero(t, w.reloads.Load())
	b, err := os.ReadFile(cache)
	require.NoError(t, err)
	assert.Equal(t, "v: 1", string(b))
}

func TestWatchTwiceReturnsError(t *testing.T) {
	srv := newServer(t, "v: 1")
	p := Provider(srv.URL, WithPollInterval(poll))
	startWatch(t, p)

	assert.Error(t, p.Watch(func(any, error) {}))
}

func TestWatchNilCallback(t *testing.T) {
	assert.Error(t, Provider("http://example.invalid").Watch(nil))
}

func TestUnwatch(t *testing.T) {
	srv := newServer(t, "v: 1")
	p := Provider(srv.URL, WithPollInterval(poll))

	require.NoError(t, p.Unwatch(), "Unwatch without Watch")

	startWatch(t, p)
	assert.Eventually(t, func() bool { return srv.hits.Load() >= 2 }, waitFor, poll/2)
	require.NoError(t, p.Unwatch())
	require.NoError(t, p.Unwatch())

	// One request may already be in flight when Unwatch is called.
	stoppedAt := srv.hits.Load()
	assert.Never(t, func() bool { return srv.hits.Load() > stoppedAt+1 }, 10*poll, poll/2)

	// Watching can be restarted after Unwatch.
	startWatch(t, p)
	assert.Eventually(t, func() bool { return srv.hits.Load() > stoppedAt+1 }, waitFor, poll/2)
}

func TestUnwatchCancelsSlowRequest(t *testing.T) {
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	p := Provider(srv.URL, WithPollInterval(poll), WithClient(&http.Client{}))
	w := startWatch(t, p)

	<-started
	require.NoError(t, p.Unwatch())
	assert.Never(t, func() bool { return w.errs.Load() > 0 || w.reloads.Load() > 0 }, 5*poll, poll/2)
}

// etagServer serves a mutable body with an ETag and answers 304 to a matching If-None-Match.
// An empty etag sends no ETag header; a non-zero status replaces the answer.
type etagServer struct {
	*httptest.Server
	mu          sync.Mutex
	body, etag  string
	status      int
	lastINM     string
	full        atomic.Int32
	notModified atomic.Int32
	failed      atomic.Int32
}

func newETagServer(t *testing.T, body, etag string) *etagServer {
	t.Helper()
	s := &etagServer{body: body, etag: etag}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inm := r.Header.Get("If-None-Match")
		s.mu.Lock()
		s.lastINM = inm
		body, etag, status := s.body, s.etag, s.status
		s.mu.Unlock()
		if status != 0 {
			s.failed.Add(1)
			w.WriteHeader(status)
			return
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		if etag != "" && inm == etag {
			s.notModified.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		s.full.Add(1)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *etagServer) set(body, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body, s.etag = body, etag
}

func (s *etagServer) setStatus(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

func (s *etagServer) hits() int32 {
	return s.full.Load() + s.notModified.Load() + s.failed.Load()
}

func (s *etagServer) ifNoneMatch() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastINM
}

func TestReadBytesConditional(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL)

	got, err := p.ReadBytes()
	require.NoError(t, err)
	assert.Empty(t, srv.ifNoneMatch(), "first load is unconditional")
	got[0] = 'X' // the caller owns the returned slice

	got, err = p.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "v: 1", string(got))
	assert.Equal(t, `"1"`, srv.ifNoneMatch())
	assert.Equal(t, int32(1), srv.full.Load())
	assert.Equal(t, int32(1), srv.notModified.Load())
}

func TestReloadAfterWatchChangeSkipsDownload(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL, WithPollInterval(poll))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	srv.set("v: 2", `"2"`)
	assert.Eventually(t, func() bool { return w.reloads.Load() == 1 }, waitFor, poll/2)
	require.NoError(t, p.Unwatch())

	got, err := p.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "v: 2", string(got))
	assert.Equal(t, int32(2), srv.full.Load(), "initial load + the poll that saw the change")
}

func TestWatchETagUnchangedGets304(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL, WithPollInterval(poll))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	assert.Eventually(t, func() bool { return srv.notModified.Load() >= 3 }, waitFor, poll/2)
	assert.Equal(t, int32(1), srv.full.Load(), "only ReadBytes downloads the body")
	assert.Equal(t, `"1"`, srv.ifNoneMatch())
	assert.Zero(t, w.reloads.Load())
	assert.Zero(t, w.errs.Load())
}

func TestWatchETagChangeReloadsOnce(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL, WithPollInterval(poll))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	srv.set("v: 2", `"2"`)

	assert.Eventually(t, func() bool { return w.reloads.Load() == 1 }, waitFor, poll/2)
	before := srv.notModified.Load()
	assert.Eventually(t, func() bool { return srv.notModified.Load() >= before+3 }, waitFor, poll/2)
	assert.Equal(t, `"2"`, srv.ifNoneMatch())
	assert.Equal(t, int32(1), w.reloads.Load())
	assert.Zero(t, w.errs.Load())
}

func TestWatchETagRetaggedSameContent(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL, WithPollInterval(poll))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	srv.set("v: 1", `"1b"`)

	assert.Eventually(t, func() bool { return srv.ifNoneMatch() == `"1b"` }, waitFor, poll/2)
	assert.Zero(t, w.reloads.Load())
	assert.Zero(t, w.errs.Load())
}

func TestWatchETagInvalidContentKeepsOldETag(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL,
		WithPollInterval(poll),
		WithValidate(func(b []byte) error {
			if string(b) == "bad" {
				return errors.New("bad config")
			}
			return nil
		}),
	)
	_, err := p.ReadBytes()
	require.NoError(t, err)
	w := startWatch(t, p)

	srv.set("bad", `"bad"`)

	// A 304 would not report an error, so two errors mean the invalid body was refetched.
	assert.Eventually(t, func() bool { return w.errs.Load() >= 2 }, waitFor, poll/2)
	assert.Equal(t, `"1"`, srv.ifNoneMatch())
	assert.Zero(t, w.reloads.Load())
}

func rejectBad(b []byte) error {
	if string(b) == "bad" {
		return errors.New("bad config")
	}
	return nil
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

// waitHits waits until the server has received n more requests.
func waitHits(t *testing.T, srv *etagServer, n int32) {
	t.Helper()
	want := srv.hits() + n
	require.Eventually(t, func() bool { return srv.hits() >= want }, waitFor, poll/2)
}

// assertPending checks that v: 1 is still the accepted content and cache, and that
// the next request is an unconditional retry.
func assertPending(t *testing.T, p *HTTP, cache string) {
	t.Helper()
	p.mu.Lock()
	last, etag, invalidated := string(p.last), p.etag, p.invalidated
	p.mu.Unlock()
	assert.Equal(t, "v: 1", last)
	assert.Empty(t, etag)
	assert.True(t, invalidated)
	assert.Equal(t, "v: 1", readFile(t, cache))
}

func TestInvalidateReadBytes(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL)
	_, err := p.ReadBytes()
	require.NoError(t, err)

	p.Invalidate()
	srv.set("v: 1", `"1b"`)

	got, err := p.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "v: 1", string(got))
	assert.Empty(t, srv.ifNoneMatch(), "the retry is unconditional")

	got, err = p.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "v: 1", string(got))
	assert.Equal(t, `"1b"`, srv.ifNoneMatch(), "conditional requests resume with the new ETag")
	assert.Equal(t, int32(2), srv.full.Load())
	assert.Equal(t, int32(1), srv.notModified.Load())
}

func TestInvalidateWatchSameContent(t *testing.T) {
	tests := []struct{ name, etag string }{
		{name: "with ETag", etag: `"1"`},
		{name: "without ETag", etag: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newETagServer(t, "v: 1", tt.etag)
			p := Provider(srv.URL, WithPollInterval(poll))
			_, err := p.ReadBytes()
			require.NoError(t, err)
			w := startWatch(t, p)

			p.Invalidate()

			assert.Eventually(t, func() bool { return w.reloads.Load() == 1 }, waitFor, poll/2)
			waitHits(t, srv, 3)
			assert.Equal(t, int32(1), w.reloads.Load(), "deduplication resumes")
			assert.Zero(t, w.errs.Load())
			assert.Equal(t, tt.etag, srv.ifNoneMatch())
		})
	}
}

func TestInvalidateRetriesAfterFailure(t *testing.T) {
	tests := []struct {
		name string
		fail func(*etagServer)
	}{
		{name: "server error", fail: func(s *etagServer) { s.setStatus(http.StatusServiceUnavailable) }},
		{name: "invalid content", fail: func(s *etagServer) { s.set("bad", `"bad"`) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newETagServer(t, "v: 1", `"1"`)
			cache := filepath.Join(t.TempDir(), "cache.yaml")
			p := Provider(srv.URL, WithPollInterval(poll), WithCache(cache), WithValidate(rejectBad))
			_, err := p.ReadBytes()
			require.NoError(t, err)
			w := startWatch(t, p)

			tt.fail(srv)
			p.Invalidate()

			// A 304 would not report an error, so repeated errors mean every poll retried.
			assert.Eventually(t, func() bool { return w.errs.Load() >= 3 }, waitFor, poll/2)
			assert.Zero(t, w.reloads.Load())
			assertPending(t, p, cache)

			srv.setStatus(0)
			srv.set("v: 1", `"1"`)

			assert.Eventually(t, func() bool { return w.reloads.Load() == 1 }, waitFor, poll/2)
			waitHits(t, srv, 3)
			assert.Equal(t, int32(1), w.reloads.Load())
			assert.Equal(t, `"1"`, srv.ifNoneMatch())
		})
	}
}

func TestInvalidateCacheFallback(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	cache := filepath.Join(t.TempDir(), "cache.yaml")
	fellBack := false
	p := Provider(srv.URL, WithPollInterval(poll), WithCache(cache),
		WithOnFallback(func(error, time.Time) { fellBack = true }))
	_, err := p.ReadBytes()
	require.NoError(t, err)

	p.Invalidate()
	srv.setStatus(http.StatusServiceUnavailable)

	got, err := p.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "v: 1", string(got))
	assert.True(t, fellBack)
	assertPending(t, p, cache)

	srv.setStatus(0)
	w := startWatch(t, p)

	assert.Eventually(t, func() bool { return w.reloads.Load() == 1 }, waitFor, poll/2)
	waitHits(t, srv, 3)
	assert.Equal(t, int32(1), w.reloads.Load())
	assert.Equal(t, "v: 1", readFile(t, cache))
}

// answer is a response the test sends to a request held by stepServer.
type answer struct {
	status     int
	body, etag string
}

// stepServer holds every request until the test answers it, so the test controls
// what happens while a request is in flight.
type stepServer struct {
	*httptest.Server
	reqs chan heldRequest
}

type heldRequest struct {
	inm    string // If-None-Match
	answer chan answer
}

func (r heldRequest) reply(a answer) { r.answer <- a }

func newStepServer(t *testing.T) *stepServer {
	t.Helper()
	s := &stepServer{reqs: make(chan heldRequest)}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		held := heldRequest{inm: r.Header.Get("If-None-Match"), answer: make(chan answer, 1)}
		var a answer
		select {
		case s.reqs <- held:
		case <-r.Context().Done():
			return
		}
		select {
		case a = <-held.answer:
		case <-r.Context().Done():
			return
		}
		if a.etag != "" {
			w.Header().Set("ETag", a.etag)
		}
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
	}))
	t.Cleanup(s.Close)
	return s
}

// await waits for the next request.
func (s *stepServer) await(t *testing.T) heldRequest {
	t.Helper()
	select {
	case r := <-s.reqs:
		return r
	case <-time.After(waitFor):
		t.Fatal("no request")
		return heldRequest{}
	}
}

type readResult struct {
	body string
	err  error
}

func readAsync(p *HTTP) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		b, err := p.ReadBytes()
		ch <- readResult{string(b), err}
	}()
	return ch
}

func TestInvalidateDuringFetch(t *testing.T) {
	v1 := answer{http.StatusOK, "v: 1", `"1"`}
	tests := []struct {
		name    string
		stale   answer // answer to the request in flight when Invalidate is called
		pollErr int32  // watch errors the stale answer causes
	}{
		{name: "changed content", stale: answer{http.StatusOK, "v: 2", `"2"`}},
		{name: "same content retagged", stale: answer{http.StatusOK, "v: 1", `"1b"`}},
		{name: "not modified", stale: answer{status: http.StatusNotModified}},
		{name: "failed fetch", stale: answer{status: http.StatusServiceUnavailable}, pollErr: 1},
	}
	// load starts a provider whose accepted content is v1, with ETag "1" and a cache.
	load := func(t *testing.T, opts ...Option) (*stepServer, *HTTP, string) {
		t.Helper()
		srv := newStepServer(t)
		cache := filepath.Join(t.TempDir(), "cache.yaml")
		p := Provider(srv.URL, append(opts, WithCache(cache))...)
		res := readAsync(p)
		srv.await(t).reply(v1)
		require.NoError(t, (<-res).err)
		return srv, p, cache
	}
	for _, tt := range tests {
		t.Run(tt.name+"/ReadBytes", func(t *testing.T) {
			srv, p, cache := load(t)

			res := readAsync(p)
			held := srv.await(t)
			assert.Equal(t, `"1"`, held.inm)
			p.Invalidate()
			held.reply(tt.stale)
			// Even a cache fallback is refused: its read started before Invalidate.
			require.ErrorIs(t, (<-res).err, errInvalidated)
			assertPending(t, p, cache)

			res = readAsync(p)
			retry := srv.await(t)
			assert.Empty(t, retry.inm, "the retry is unconditional")
			retry.reply(v1)
			r := <-res
			require.NoError(t, r.err)
			assert.Equal(t, "v: 1", r.body)

			res = readAsync(p)
			next := srv.await(t)
			assert.Equal(t, `"1"`, next.inm, "conditional requests resume")
			next.reply(answer{status: http.StatusNotModified})
			require.NoError(t, (<-res).err)
		})
		t.Run(tt.name+"/Watch", func(t *testing.T) {
			srv, p, cache := load(t, WithPollInterval(poll))
			w := startWatch(t, p)

			held := srv.await(t)
			assert.Equal(t, `"1"`, held.inm)
			p.Invalidate()
			held.reply(tt.stale)

			// Polls run one at a time: the next request means the stale answer was handled.
			retry := srv.await(t)
			assert.Empty(t, retry.inm, "the retry is unconditional")
			assertPending(t, p, cache)
			assert.Zero(t, w.reloads.Load())
			assert.Equal(t, tt.pollErr, w.errs.Load())
			retry.reply(v1)

			next := srv.await(t)
			assert.Equal(t, `"1"`, next.inm, "conditional polling resumes")
			assert.Equal(t, int32(1), w.reloads.Load(), "the identical retry is delivered")
			next.reply(answer{status: http.StatusNotModified})
		})
	}

	// A ReadBytes retry accepted while an older poll is in flight: the poll's
	// same-content answer must not bring back its ETag.
	t.Run("stale retag after accepted retry", func(t *testing.T) {
		srv, p, _ := load(t, WithPollInterval(poll))
		w := startWatch(t, p)

		held := srv.await(t)
		p.Invalidate()
		res := readAsync(p)
		retry := srv.await(t)
		assert.Empty(t, retry.inm)
		retry.reply(answer{http.StatusOK, "v: 1", `"1c"`})
		require.NoError(t, (<-res).err)
		held.reply(answer{http.StatusOK, "v: 1", `"1b"`})

		next := srv.await(t)
		assert.Equal(t, `"1c"`, next.inm, "the stale ETag is dropped")
		assert.Zero(t, w.reloads.Load())
		next.reply(answer{status: http.StatusNotModified})
	})
}

func TestInvalidateFromWatchCallback(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	p := Provider(srv.URL, WithPollInterval(poll))
	_, err := p.ReadBytes()
	require.NoError(t, err)
	var reloads atomic.Int32
	require.NoError(t, p.Watch(func(_ any, err error) {
		if err == nil {
			reloads.Add(1)
			p.Invalidate() // as if applying the reload failed
		}
	}))
	t.Cleanup(func() { _ = p.Unwatch() })

	srv.set("v: 2", `"2"`)

	// Each notification invalidates again, so the unchanged document is delivered again.
	assert.Eventually(t, func() bool { return reloads.Load() >= 3 }, waitFor, poll/2)
}

func TestInvalidateLifecycle(t *testing.T) {
	srv := newETagServer(t, "v: 1", `"1"`)
	cache := filepath.Join(t.TempDir(), "cache.yaml")
	p := Provider(srv.URL, WithPollInterval(time.Hour), WithCache(cache))

	p.Invalidate() // before the first load
	_, err := p.ReadBytes()
	require.NoError(t, err)
	assert.Empty(t, srv.ifNoneMatch())

	// Repeated calls coalesce into one unconditional retry and keep the cache.
	p.Invalidate()
	p.Invalidate()
	assert.Equal(t, "v: 1", readFile(t, cache))
	_, err = p.ReadBytes()
	require.NoError(t, err)
	assert.Empty(t, srv.ifNoneMatch())
	_, err = p.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, `"1"`, srv.ifNoneMatch())

	startWatch(t, p)
	require.NoError(t, p.Unwatch())
	p.Invalidate()
	assert.Equal(t, "v: 1", readFile(t, cache))
	got, err := p.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, "v: 1", string(got))
	assert.Empty(t, srv.ifNoneMatch())
}
