package httpprovider

import (
	"net/http"
	"time"
)

// Option configures the HTTP provider.
type Option func(*HTTP)

// WithClient sets the HTTP client used for requests. A nil client is ignored.
// Default: a client with a 10s timeout.
func WithClient(c *http.Client) Option {
	return func(p *HTTP) {
		if c != nil {
			p.client = c
		}
	}
}

// WithHeaders sets headers sent with every request.
func WithHeaders(h http.Header) Option {
	return func(p *HTTP) { p.headers = h.Clone() }
}

// WithPollInterval sets how often Watch polls the URL. Non-positive values are ignored.
// Default: 60s.
func WithPollInterval(d time.Duration) Option {
	return func(p *HTTP) {
		if d > 0 {
			p.pollInterval = d
		}
	}
}

// WithMaxSize sets the maximum accepted body size in bytes; a larger body is an error.
// Non-positive values are ignored. Default: 10 MiB.
func WithMaxSize(n int64) Option {
	return func(p *HTTP) {
		if n > 0 {
			p.maxSize = n
		}
	}
}

// WithCache sets the path of the local cache file holding the last valid config.
// An empty path disables the cache.
func WithCache(path string) Option {
	return func(p *HTTP) { p.cachePath = path }
}

// WithValidate sets a function that validates fetched content before it is accepted.
func WithValidate(fn func([]byte) error) Option {
	return func(p *HTTP) { p.validate = fn }
}

// WithOnFallback sets a hook called when ReadBytes serves the cache because the fetch failed.
// cachedAt is the modification time of the cache file.
func WithOnFallback(fn func(fetchErr error, cachedAt time.Time)) Option {
	return func(p *HTTP) { p.onFallback = fn }
}
