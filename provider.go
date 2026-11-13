// Package httpprovider implements a koanf provider that loads config over HTTP(S),
// watches it by polling, and keeps a local copy of the last valid config so a
// service can still start when the network is down.
package httpprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultTimeout      = 10 * time.Second
	defaultPollInterval = 60 * time.Second
	defaultMaxSize      = 10 << 20
)

var (
	// errNotModified is returned by fetch when the server answers 304 to a conditional request.
	errNotModified = errors.New("httpprovider: not modified")
	// errInvalidated is returned by a ReadBytes that was in flight when Invalidate was called.
	errInvalidated = errors.New("httpprovider: read interrupted by Invalidate, retry")
)

// HTTP is a koanf provider that reads config from an HTTP(S) URL.
type HTTP struct {
	url          string
	client       *http.Client
	headers      http.Header
	pollInterval time.Duration
	maxSize      int64
	cachePath    string
	validate     func([]byte) error
	onFallback   func(fetchErr error, cachedAt time.Time)

	mu          sync.Mutex
	lastHash    [sha256.Size]byte
	etag        string
	last        []byte // content matching lastHash and etag; never mutated, only replaced
	gen         uint64 // incremented by Invalidate; responses to older requests are dropped
	invalidated bool   // set by Invalidate until a live fetch is accepted
	cancel      context.CancelFunc
}

// Provider returns an HTTP provider for the given URL.
func Provider(url string, opts ...Option) *HTTP {
	p := &HTTP{
		url:          url,
		client:       &http.Client{Timeout: defaultTimeout},
		pollInterval: defaultPollInterval,
		maxSize:      defaultMaxSize,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// ReadBytes fetches the config. If the fetch fails and a cache file exists,
// it returns the cached bytes and calls the OnFallback hook.
func (p *HTTP) ReadBytes() ([]byte, error) {
	// Conditional when an ETag is known: a 304 serves the in-memory copy, so the
	// reload that follows a change detected by Watch doesn't download the body again.
	p.mu.Lock()
	lastEtag, last, gen := p.etag, p.last, p.gen
	p.mu.Unlock()
	b, etag, fetchErr := p.fetch(context.Background(), lastEtag)
	if errors.Is(fetchErr, errNotModified) {
		p.mu.Lock()
		stale := gen != p.gen
		p.mu.Unlock()
		if stale {
			return nil, errInvalidated
		}
		return bytes.Clone(last), nil
	}
	if fetchErr == nil {
		if err := p.check(b); err != nil {
			return nil, err
		}
		if !p.commit(gen, b, etag, true) {
			return nil, errInvalidated
		}
		return b, nil
	}

	if p.cachePath == "" {
		return nil, fetchErr
	}
	b, cachedAt, cacheErr := p.readCache()
	if errors.Is(cacheErr, fs.ErrNotExist) {
		return nil, fetchErr
	}
	if cacheErr != nil {
		return nil, errors.Join(fetchErr, cacheErr)
	}
	if !p.commit(gen, b, "", false) {
		return nil, errors.Join(errInvalidated, fetchErr)
	}
	if p.onFallback != nil {
		p.onFallback(fetchErr, cachedAt)
	}
	return b, nil
}

// Read is not supported by the HTTP provider.
func (p *HTTP) Read() (map[string]any, error) {
	return nil, errors.New("httpprovider does not support Read()")
}

// Watch polls the URL every poll interval. cb is called with a nil error when the
// content changed (reload it), and with an error when a poll fails or the new
// content is invalid.
func (p *HTTP) Watch(cb func(event any, err error)) error {
	if cb == nil {
		return errors.New("httpprovider: nil watch callback")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		return errors.New("httpprovider: already watching")
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go p.poll(ctx, cb)
	return nil
}

// Unwatch stops watching. It is safe to call more than once, or without a prior Watch.
func (p *HTTP) Unwatch() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	return nil
}

// Invalidate forces the next live fetch to bypass conditional requests and
// content deduplication. It allows retrying content that the caller failed to
// apply. Cached content is preserved. It is safe to call concurrently with
// ReadBytes, Watch, and Unwatch, including from a watch callback.
func (p *HTTP) Invalidate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gen++
	p.etag = ""
	p.invalidated = true
}

func (p *HTTP) poll(ctx context.Context, cb func(event any, err error)) {
	t := time.NewTicker(p.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		p.mu.Lock()
		lastEtag, gen := p.etag, p.gen
		p.mu.Unlock()
		b, etag, err := p.fetch(ctx, lastEtag)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, errNotModified) {
			continue
		}
		if err != nil {
			cb(nil, err)
			continue
		}
		h := sha256.Sum256(b)
		p.mu.Lock()
		stale := gen != p.gen // Invalidate was called during the fetch: the next poll retries
		same := !p.invalidated && h == p.lastHash
		if !stale && same {
			p.etag = etag // same content re-tagged: remember it so the next poll can be a 304
		}
		p.mu.Unlock()
		if stale || same {
			continue
		}
		if err := p.check(b); err != nil {
			cb(nil, err) // keep the old hash and ETag: the next poll refetches and reports again
			continue
		}
		if !p.commit(gen, b, etag, true) {
			continue
		}
		cb(nil, nil)
	}
}

// fetch GETs the URL and returns the body and its ETag. A non-empty etag makes the
// request conditional; a 304 answer returns errNotModified.
func (p *HTTP) fetch(ctx context.Context, etag string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("httpprovider: %w", err)
	}
	for k, vs := range p.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("httpprovider: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if etag != "" && resp.StatusCode == http.StatusNotModified {
		return nil, "", errNotModified
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("httpprovider: unexpected status %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, p.maxSize+1))
	if err != nil {
		return nil, "", fmt.Errorf("httpprovider: read body: %w", err)
	}
	if int64(len(b)) > p.maxSize {
		return nil, "", fmt.Errorf("httpprovider: body exceeds max size of %d bytes", p.maxSize)
	}
	return b, resp.Header.Get("ETag"), nil
}

func (p *HTTP) check(b []byte) error {
	if p.validate == nil {
		return nil
	}
	if err := p.validate(b); err != nil {
		return fmt.Errorf("httpprovider: invalid config: %w", err)
	}
	return nil
}

// commit makes b the accepted content unless Invalidate was called after the request
// that produced it started (gen), and reports whether it did. live means b was fetched
// from the server: it then also replaces the cache and ends a pending invalidation.
// The cache is written under p.mu so a stale response can't overwrite a newer one.
func (p *HTTP) commit(gen uint64, b []byte, etag string, live bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen != p.gen {
		return false
	}
	if live {
		_ = p.writeCache(b) // best-effort: a failed cache write must not fail the load
		p.invalidated = false
	}
	p.lastHash, p.etag, p.last = sha256.Sum256(b), etag, bytes.Clone(b)
	return true
}

// writeCache atomically replaces the cache file with b (mode 0600, from os.CreateTemp).
func (p *HTTP) writeCache(b []byte) (err error) {
	if p.cachePath == "" {
		return nil
	}
	f, err := os.CreateTemp(filepath.Dir(p.cachePath), filepath.Base(p.cachePath)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, p.cachePath)
}

func (p *HTTP) readCache() ([]byte, time.Time, error) {
	fi, err := os.Stat(p.cachePath)
	if err != nil {
		return nil, time.Time{}, err
	}
	b, err := os.ReadFile(p.cachePath)
	if err != nil {
		return nil, time.Time{}, err
	}
	return b, fi.ModTime(), nil
}
