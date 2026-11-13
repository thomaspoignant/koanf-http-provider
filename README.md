# koanf-http-provider

A [koanf](https://github.com/knadh/koanf) v2 provider that loads config from an HTTP(S) URL.

- Fetches a config file with a GET request.
- Watches it for changes by polling.
- Keeps a local copy of the last valid config, so your service can still start when the config server is unreachable.

The library uses the Go standard library only. It does not import koanf; it satisfies `koanf.Provider` structurally.

## Install

```sh
go get github.com/thomaspoignant/koanf-http-provider
```

## Usage

```go
import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/v2"
	httpprovider "github.com/thomaspoignant/koanf-http-provider"
)

k := koanf.New(".")
p := httpprovider.Provider("https://config.example.com/app.yaml",
	httpprovider.WithHeaders(http.Header{"Authorization": {"Bearer " + os.Getenv("CONFIG_TOKEN")}}),
	httpprovider.WithPollInterval(30*time.Second),
	httpprovider.WithCache("/var/lib/app/config-cache.yaml"),
	httpprovider.WithOnFallback(func(err error, cachedAt time.Time) {
		log.Printf("config server unreachable (%v), using cache from %s", err, cachedAt)
	}),
)
if err := k.Load(p, yaml.Parser()); err != nil {
	log.Fatal(err)
}

if err := p.Watch(func(_ any, err error) {
	if err != nil {
		log.Printf("config poll failed: %v", err)
		return
	}
	if err := k.Load(p, yaml.Parser()); err != nil {
		log.Printf("config reload failed: %v", err)
		p.Invalidate() // retry on the next poll, even if the content is unchanged
	}
}); err != nil {
	log.Fatal(err)
}
defer func() { _ = p.Unwatch() }()
```

This snippet is the package's `Example()` in [`example_test.go`](example_test.go), so it is compiled with the tests.

## Options

| Option | Default | Description |
|---|---|---|
| `WithClient(*http.Client)` | client with a 10s timeout | HTTP client used for every request. |
| `WithHeaders(http.Header)` | none | Headers sent with every request, for example `Authorization`. |
| `WithPollInterval(time.Duration)` | `60s` | How often `Watch` polls the URL. |
| `WithMaxSize(int64)` | `10 MiB` | Maximum body size. A larger body is an error. |
| `WithCache(path string)` | `""` (no cache) | File that holds the last valid config. |
| `WithValidate(func([]byte) error)` | none | Rejects fetched content before it is accepted or cached. |
| `WithOnFallback(func(fetchErr error, cachedAt time.Time))` | none | Called when `ReadBytes` serves the cache. `cachedAt` is the cache file's mtime. |

A nil client and non-positive interval or size values are ignored, and the defaults are kept.

## Fallback semantics

**`ReadBytes()`** (called by `k.Load`):

1. GET the URL. Anything other than a `200` is an error. The body is limited to `MaxSize`.
2. On success, run `validate` if it is set.
   - If validation fails, return the error. The cache is left untouched.
   - Otherwise write the cache atomically (temp file plus rename, mode `0600`) and return the bytes. A failed cache write does not fail the load.
3. If the fetch fails and a cache file exists, return the cached bytes and call `OnFallback`.
   - If there is no cache, return the fetch error.
   - If the cache can't be read, return both errors joined.

Once an `ETag` is known, `ReadBytes` sends `If-None-Match`. On a `304` it returns the in-memory copy of the last accepted config, so the `k.Load` that follows a change detected by `Watch` doesn't download the body again.

**`Watch(cb)`** polls every `PollInterval`. It never falls back to the cache. If the server sent an `ETag`, each poll is a conditional request (`If-None-Match`), so an unchanged file costs a `304 Not Modified` with no body. For servers without ETags, the sha256 comparison below detects changes.

- **`304 Not Modified`:** nothing happens.
- **Fetch error:** `cb(nil, err)`.
- **Content changed and valid:** the cache is updated, then `cb(nil, nil)` is called. Reload with `k.Load` at this point.
- **Content changed but invalid:** `cb(nil, err)`. The last good version stays current.
- **Content unchanged** (same sha256): nothing happens.

**`Invalidate()`** tells the provider that you could not apply the config it delivered, for example because `k.Load` or your own apply step failed. The provider can't detect that on its own. After `Invalidate`:

- The next fetch skips `If-None-Match` and the sha256 comparison. `Watch` calls `cb(nil, nil)` on the next poll, even if the content hasn't changed. `ReadBytes` returns the fetched bytes.
- Fetch errors, validation errors and cache fallbacks keep the retry pending. Once a fetch succeeds, ETag and sha256 deduplication resume.
- The in-memory copy and the cache file are kept, so the fallback still works.
- It doesn't fetch anything or call `cb` itself; the next poll or `ReadBytes` does the retry. Repeated calls result in one retry. It's safe to call from a `Watch` callback, before the first load, and after `Unwatch`.
- A `ReadBytes` already in flight when `Invalidate` is called returns an error rather than the result of its older request. Responses to such older poll requests are dropped.

Call `Invalidate` on the same provider, wherever the apply error is observed, even if you reload asynchronously.

`Unwatch()` stops polling and cancels any in-flight request. It is safe to call more than once.

`Read()` is not supported and returns an error, like koanf's file provider.

## Security

- **The cache file may contain secrets** (credentials, API keys). It is written with mode `0600`. Put it in a directory only your service can read.
- **In containers, the cache only survives a restart if it is on a persistent volume.** On an ephemeral filesystem, the fallback only helps within the lifetime of a single container.
- Headers such as `Authorization` are sent with every request. Use an `https://` URL.

## Releases

Versions, tags and `CHANGELOG.md` are managed by [release-please](https://github.com/googleapis/release-please). It reads [conventional commit](https://www.conventionalcommits.org/) messages on `main` (`feat:`, `fix:`, …), so use them in PR titles.

## License

[MIT](LICENSE)
