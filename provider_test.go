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
type etagServer struct {
	*httptest.Server
	mu          sync.Mutex
	body, etag  string
	lastINM     string
	full        atomic.Int32
	notModified atomic.Int32
}

func newETagServer(t *testing.T, body, etag string) *etagServer {
	t.Helper()
	s := &etagServer{body: body, etag: etag}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inm := r.Header.Get("If-None-Match")
		s.mu.Lock()
		s.lastINM = inm
		body, etag := s.body, s.etag
		s.mu.Unlock()
		w.Header().Set("ETag", etag)
		if inm == etag {
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
