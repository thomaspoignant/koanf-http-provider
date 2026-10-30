package httpprovider_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpprovider "github.com/thomaspoignant/koanf-http-provider"
)

// Example is the README usage example; keeping it here proves it compiles.
func Example() {
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
		}
	}); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = p.Unwatch() }()
}

func TestKoanfEndToEnd(t *testing.T) {
	var mu sync.Mutex
	body := "app:\n  name: v1\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	p := httpprovider.Provider(srv.URL, httpprovider.WithPollInterval(20*time.Millisecond))
	k := koanf.New(".")
	require.NoError(t, k.Load(p, yaml.Parser()))
	assert.Equal(t, "v1", k.String("app.name"))

	changed := make(chan error, 1)
	require.NoError(t, p.Watch(func(_ any, err error) {
		select {
		case changed <- err:
		default:
		}
	}))
	defer func() { _ = p.Unwatch() }()

	mu.Lock()
	body = "app:\n  name: v2\n"
	mu.Unlock()

	select {
	case err := <-changed:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("no change detected")
	}
	require.NoError(t, k.Load(p, yaml.Parser()))
	assert.Equal(t, "v2", k.String("app.name"))
}
