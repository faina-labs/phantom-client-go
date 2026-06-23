# phantom-client-go — Integration Guide

A Go client for publishing events to Phantom Tracker via NATS JetStream.
Bypasses the REST API entirely: sub-millisecond publish, zero-loss durability, compile-time schema enforcement.

---

## Contents

1. [Why this exists](#why-this-exists)
2. [Module info & versioning](#module-info--versioning)
3. [Installing in faina-api](#installing-in-faina-api)
4. [API reference](#api-reference)
5. [Migrating from REST API](#migrating-from-rest-api)
6. [Patterns & recipes](#patterns--recipes)
7. [Error handling](#error-handling)
8. [Durability contract](#durability-contract)
9. [Configuration reference](#configuration-reference)

---

## Why this exists

faina-api previously called Phantom's REST `/ingest` endpoint over HTTP:

```
faina-api  →  HTTP POST /ingest  →  phantom  →  NATS  →  ES
```

Problems with that path:
- **3 network hops** (faina → phantom HTTP → NATS → ES)
- **Phantom HTTP load** directly gates faina-api throughput
- **HTTP failure = event loss** unless faina-api implements its own retry/queue

`phantom-client-go` collapses this to:

```
faina-api  →  NATS JetStream (phantom.ingest)  →  phantom consumer  →  ES
```

- **1 hop** to NATS, <0.5ms publish
- **Phantom HTTP can be fully down** — NATS holds 48h of events, consumer drains on recovery
- **Compile-time schema**: faina-api cannot send malformed events — `Event` is a Go struct

---

## Module info & versioning

| | |
|---|---|
| **Module** | `github.com/faina-labs/phantom-client-go` |
| **Repo** | https://github.com/faina-labs/phantom-client-go |
| **Latest** | `v0.1.0` |
| **Visibility** | Public — no auth config needed |

### Releasing a new version

```bash
git tag v0.X.0
git push origin v0.X.0
```

Each semver tag is immediately fetchable via `go get` and indexed on pkg.go.dev.

### Local development against an unreleased version

**Option A — Go workspace** (recommended when faina-api and phantom-client-go are cloned side by side):

```bash
# /projects/
#   phantom-client-go/
#   faina-api/

cd /projects/
go work init
go work use ./phantom-client-go
go work use ./faina-api
```

`go.work`:
```
go 1.21

use (
    ./phantom-client-go
    ./faina-api
)
```

Do not commit `go.work` — it's in `.gitignore`.

**Option B — `replace` directive** (quick, single machine):

Add to faina-api's `go.mod`:

```
require github.com/faina-labs/phantom-client-go v0.0.0

replace github.com/faina-labs/phantom-client-go => ../phantom-client-go
```

Remove before merging — `replace` breaks downstream consumers.

---

## Installing in faina-api

```bash
go get github.com/faina-labs/phantom-client-go@v0.1.0
```

Import path:
```go
import phantom "github.com/faina-labs/phantom-client-go"
```

---

## API reference

### `phantom.NewClient`

```go
func NewClient(natsURL string, opts ...Option) (*Client, error)
```

Creates a connected client. Establishes NATS connection + JetStream context.
Reconnects automatically on disconnect (`MaxReconnects=-1`).

```go
client, err := phantom.NewClient("nats://localhost:4222")
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

**Options:**

| Option | Default | Description |
|---|---|---|
| `WithStream(name)` | `"phantom.ingest"` | Override NATS stream name |
| `WithSubject(subj)` | `"phantom.ingest.events"` | Override NATS subject |
| `WithMaxBatch(n)` | `500` | Max events per NATS message |

```go
// Custom subject (e.g., route by environment)
client, err := phantom.NewClient(natsURL,
    phantom.WithSubject("phantom.ingest.production"),
    phantom.WithMaxBatch(200),
)
```

---

### `phantom.Event`

```go
type Event struct {
    ID         string         // auto-generated UUID if empty
    WebsiteID  string         // required — Phantom website ID
    VisitorID  string         // optional — known visitor ID
    SessionID  string         // optional — session ID
    Name       string         // required — event name (e.g. "impression", "click")
    IPAddress  string         // recommended — client IP for geo enrichment
    UserAgent  string         // recommended — for device/browser enrichment
    Language   string         // optional — Accept-Language header value
    URL        string         // optional — page URL
    Referer    string         // optional — HTTP Referer
    Properties map[string]any // optional — custom event data
    Timestamp  time.Time      // optional — defaults to time.Now()
}
```

**Required fields:** `WebsiteID`, `Name`
**Recommended fields:** `IPAddress`, `UserAgent` (without these, geo/device enrichment is skipped)

---

### `client.Ingest`

```go
func (c *Client) Ingest(ctx context.Context, events []Event) error
```

Validates all events locally (fail fast — no NATS round-trip for bad data),
auto-fills `ID` and `Timestamp` if empty,
publishes in batches of up to `MaxBatch` events per NATS message,
and **blocks until JetStream acknowledgment** from the NATS server.

After `Ingest` returns `nil`, NATS JetStream guarantees at-least-once delivery to the phantom consumer.

```go
err := client.Ingest(ctx, []phantom.Event{
    {
        WebsiteID: "ws_abc123",
        Name:      "impression",
        IPAddress: r.RemoteAddr,
        UserAgent: r.Header.Get("User-Agent"),
        Properties: map[string]any{
            "ad_id":    "ad_xyz",
            "campaign": "summer_2026",
        },
    },
})
if err != nil {
    // NATS publish failed — event not in JetStream yet
    // Safe to retry or log
}
```

**Batching:** If you pass 1500 events with `MaxBatch=500`, three NATS messages are published (500+500+500). Each publish blocks for JetStream ack individually.

---

### `client.Close`

```go
func (c *Client) Close()
```

Closes the NATS connection. Call on app shutdown.

---

## Migrating from REST API

### Before — REST `/ingest`

```go
// faina-api: current pattern (simplified)
type ingestRequest struct {
    Events []vastEvent `json:"events"`
}

func trackEvents(events []vastEvent) error {
    body, _ := json.Marshal(ingestRequest{Events: events})

    req, _ := http.NewRequest("POST", phantomURL+"/ingest", bytes.NewReader(body))
    req.Header.Set("X-API-Key", phantomAPIKey)
    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return fmt.Errorf("phantom HTTP failed: %w", err)  // event may be lost
    }
    defer resp.Body.Close()

    if resp.StatusCode != 200 {
        return fmt.Errorf("phantom returned %d", resp.StatusCode)
    }
    return nil
}
```

**Problems:**
- If phantom HTTP is down: `trackEvents` returns error → event lost (unless faina-api retries)
- Latency: ~50-200ms per call (HTTP round-trip + phantom processing)
- Throughput: capped by phantom's HTTP concurrency

---

### After — phantom-client-go

```go
// faina-api: new pattern

// Initialize once at startup (package-level or DI)
var phantomClient *phantom.Client

func initPhantom(natsURL string) error {
    var err error
    phantomClient, err = phantom.NewClient(natsURL)
    return err
}

func trackEvents(ctx context.Context, websiteID string, events []vastEvent) error {
    phantomEvents := make([]phantom.Event, len(events))
    for i, e := range events {
        phantomEvents[i] = phantom.Event{
            WebsiteID:  websiteID,
            Name:       e.EventType,   // "impression", "start", "complete", etc.
            IPAddress:  e.IP,
            UserAgent:  e.UserAgent,
            Properties: map[string]any{
                "ad_id":      e.AdID,
                "campaign":   e.CampaignID,
                "creative":   e.CreativeID,
                "duration":   e.Duration,
            },
        }
    }
    return phantomClient.Ingest(ctx, phantomEvents)
}
```

**What changed:**
- No HTTP call — direct NATS publish
- If phantom HTTP is down: events are safe in NATS (48h retention)
- Latency: <0.5ms per batch
- No API key needed — trust is at the NATS level

---

### Field mapping — REST vs phantom-client-go

| REST `/ingest` field | phantom.Event field | Notes |
|---|---|---|
| `events[].name` | `Name` | same |
| `events[].websiteId` | `WebsiteID` | same (snake_case in JSON) |
| `events[].visitorId` | `VisitorID` | same |
| `events[].sessionId` | `SessionID` | same |
| `events[].properties` | `Properties` | same |
| `events[].context.ip` | `IPAddress` | promoted to top-level |
| `events[].context.userAgent` | `UserAgent` | promoted to top-level |
| `events[].context.url` | `URL` | promoted to top-level |
| `events[].context.referer` | `Referer` | promoted to top-level |
| `events[].context.language` | `Language` | promoted to top-level |
| `events[].timestamp` | `Timestamp` | same; auto-filled if zero |
| `events[].id` | `ID` | auto-generated UUID if empty |

---

## Patterns & recipes

### Singleton client (recommended)

Initialize once in `main.go` or an init function. `Client` is thread-safe.

```go
// tracker.go
package tracker

import (
    "sync"
    phantom "github.com/faina-labs/phantom-client-go"
)

var (
    once   sync.Once
    client *phantom.Client
)

func Init(natsURL string) error {
    var initErr error
    once.Do(func() {
        client, initErr = phantom.NewClient(natsURL)
    })
    return initErr
}

func Track(ctx context.Context, events []phantom.Event) error {
    return client.Ingest(ctx, events)
}

func Close() {
    if client != nil {
        client.Close()
    }
}
```

```go
// main.go
func main() {
    if err := tracker.Init(os.Getenv("NATS_URL")); err != nil {
        log.Fatal(err)
    }
    defer tracker.Close()

    // ... rest of app
}
```

---

### Fire-and-forget with async goroutine

`Ingest` blocks on JetStream ack (~0.5ms). For request handlers where you don't want
to block the response on tracking, publish in a goroutine:

```go
func handleVASTRequest(w http.ResponseWriter, r *http.Request) {
    // ... serve the VAST response ...
    w.Write(vastXML)

    // Track async — don't block response
    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := phantomClient.Ingest(ctx, []phantom.Event{
            {
                WebsiteID: websiteID,
                Name:      "impression",
                IPAddress: clientIP(r),
                UserAgent: r.Header.Get("User-Agent"),
            },
        }); err != nil {
            log.Printf("phantom.Ingest failed: %v", err)
        }
    }()
}
```

> **Note:** If your process can exit before the goroutine completes, use a `sync.WaitGroup`
> or a buffered channel to drain in-flight publishes on shutdown.

---

### Batch accumulation (high-throughput)

For extremely high volumes, accumulate events in memory and flush periodically:

```go
type batcher struct {
    client  *phantom.Client
    website string
    mu      sync.Mutex
    buf     []phantom.Event
    ticker  *time.Ticker
    done    chan struct{}
}

func newBatcher(client *phantom.Client, websiteID string, interval time.Duration) *batcher {
    b := &batcher{
        client:  client,
        website: websiteID,
        buf:     make([]phantom.Event, 0, 500),
        ticker:  time.NewTicker(interval),
        done:    make(chan struct{}),
    }
    go b.run()
    return b
}

func (b *batcher) Add(e phantom.Event) {
    b.mu.Lock()
    b.buf = append(b.buf, e)
    full := len(b.buf) >= 500
    b.mu.Unlock()
    if full {
        b.flush()
    }
}

func (b *batcher) flush() {
    b.mu.Lock()
    if len(b.buf) == 0 {
        b.mu.Unlock()
        return
    }
    events := make([]phantom.Event, len(b.buf))
    copy(events, b.buf)
    b.buf = b.buf[:0]
    b.mu.Unlock()

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := b.client.Ingest(ctx, events); err != nil {
        log.Printf("batcher flush failed: %v", err)
    }
}

func (b *batcher) run() {
    for {
        select {
        case <-b.ticker.C:
            b.flush()
        case <-b.done:
            b.flush() // final flush on shutdown
            return
        }
    }
}

func (b *batcher) Close() {
    b.ticker.Stop()
    close(b.done)
}
```

Usage:
```go
b := newBatcher(phantomClient, "ws_abc123", 100*time.Millisecond)
defer b.Close()

// In VAST handler:
b.Add(phantom.Event{Name: "impression", IPAddress: ip, UserAgent: ua})
```

---

### Sending VAST pixel events

VAST fires multiple events per ad play. Map them all:

```go
var vastEventNames = map[string]string{
    "start":          "vast_start",
    "firstQuartile":  "vast_first_quartile",
    "midpoint":       "vast_midpoint",
    "thirdQuartile":  "vast_third_quartile",
    "complete":       "vast_complete",
    "impression":     "vast_impression",
    "click":          "vast_click",
    "skip":           "vast_skip",
    "mute":           "vast_mute",
    "unmute":         "vast_unmute",
    "pause":          "vast_pause",
    "resume":         "vast_resume",
    "fullscreen":     "vast_fullscreen",
    "creativeView":   "vast_creative_view",
}

func handleVASTPixel(w http.ResponseWriter, r *http.Request) {
    vastEvent := r.URL.Query().Get("event")
    eventName, ok := vastEventNames[vastEvent]
    if !ok {
        http.Error(w, "unknown event", http.StatusBadRequest)
        return
    }

    // Respond immediately — tracking is async
    w.WriteHeader(http.StatusNoContent)

    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        defer cancel()
        _ = phantomClient.Ingest(ctx, []phantom.Event{
            {
                WebsiteID: r.URL.Query().Get("wid"),
                Name:      eventName,
                IPAddress: realIP(r),
                UserAgent: r.Header.Get("User-Agent"),
                Language:  r.Header.Get("Accept-Language"),
                URL:       r.Referer(),
                Properties: map[string]any{
                    "ad_id":       r.URL.Query().Get("ad_id"),
                    "campaign_id": r.URL.Query().Get("cid"),
                    "creative_id": r.URL.Query().Get("crid"),
                },
            },
        })
    }()
}
```

---

## Error handling

`Ingest` returns an error only when NATS publish fails. This is rare — NATS reconnects
automatically and queues publishes during transient disconnects.

```go
err := client.Ingest(ctx, events)
switch {
case err == nil:
    // JetStream acked — events are durable

case errors.Is(err, context.DeadlineExceeded):
    // NATS took too long — increase context timeout or check NATS health
    // Events were NOT published; safe to retry

case errors.Is(err, context.Canceled):
    // Caller canceled the context
    // Events were NOT published; do not retry

default:
    // NATS connection error
    // Check err.Error() for details
    // Safe to retry with backoff
}
```

**Validation errors** (missing `WebsiteID` or `Name`) are returned before any NATS call:
```go
err := client.Ingest(ctx, []phantom.Event{{Name: ""}})
// err: "phantom: event[0]: phantom: Event.Name is required"
// No NATS publish attempted
```

---

## Durability contract

Once `Ingest` returns `nil`:

1. NATS JetStream has acknowledged the message to at least one replica
2. The message is persisted for 48 hours even if phantom consumer is fully down
3. When phantom consumer recovers, it drains the backlog at full speed
4. ES indexing is idempotent (event ID = ES document ID) — safe to redeliver

**What faina-api does NOT need:**
- Its own retry queue for phantom events
- Circuit breaker around phantom HTTP
- Dead letter handling for phantom-related failures

NATS handles all of that.

---

## Configuration reference

### Environment variables (phantom consumer side — no faina-api changes needed)

| Variable | Default | Description |
|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `ELASTICSEARCH_URLS` | `http://localhost:9200` | ES endpoint |
| `DLQ_ENABLED` | `true` | Enable Redis DLQ for failed ES writes |
| `GEOIP_DB_PATH` | _(empty)_ | Path to GeoLite2-City.mmdb for geo enrichment |

### faina-api environment variables (new)

| Variable | Example | Description |
|---|---|---|
| `NATS_URL` | `nats://phantom-nats:4222` | Same NATS cluster phantom uses |

### NATS stream (phantom owns, auto-created on phantom startup)

| Property | Value |
|---|---|
| Stream name | `phantom.ingest` |
| Subjects | `phantom.ingest.>` |
| Retention | 48 hours |
| Replicas | 3 |
| Storage | File (persistent) |
| Max events per message | 500 (client-go default) |

---

## Checklist: migrating faina-api

- [ ] `go get github.com/faina-labs/phantom-client-go@v0.1.0`
- [ ] Initialize `phantom.Client` once at startup with `NATS_URL`
- [ ] Replace REST calls to `/ingest` with `client.Ingest`
- [ ] Map existing event fields using the field mapping table above
- [ ] Add `NATS_URL` env var pointing to the shared NATS cluster
- [ ] Remove `PHANTOM_API_KEY` env var (no longer needed for event publishing)
- [ ] Remove HTTP client / retry logic for phantom ingest (NATS handles it)
- [ ] Test with phantom consumer running — verify events appear in ES
- [ ] Test with phantom consumer stopped — verify events appear in ES after consumer restarts
- [ ] After full rollout: remove TRACKING_EVENTS consumer from phantom (coordinate with phantom team)
