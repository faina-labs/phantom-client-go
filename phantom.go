// Package phantom provides a Go client for publishing events to Phantom Tracker
// via NATS JetStream (phantom.ingest stream).
//
// Sync mode (default) — returns after JetStream ack, guarantees durability:
//
//	client, err := phantom.NewClient("nats://localhost:4222")
//	if err != nil { ... }
//	defer client.Close()
//	err = client.Ingest(ctx, []phantom.Event{{Name: "purchase", WebsiteID: "ws_abc"}})
//
// Buffered mode — async, best-effort, for high-frequency fire-and-forget traffic:
//
//	client, err := phantom.NewClient("nats://localhost:4222",
//	    phantom.WithBuffer(500, 10*time.Millisecond),
//	)
//
// Shared connection — multiple clients, one TCP connection per pod:
//
//	conn, _ := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
//	syncClient, _   := phantom.NewClientWithConn(conn)
//	pixelClient, _  := phantom.NewClientWithConn(conn, phantom.WithBuffer(500, 10*time.Millisecond))
//	// shutdown — caller closes conn last:
//	pixelClient.Close()
//	syncClient.Close()
//	conn.Close()
//
// Module: github.com/faina-labs/phantom-client-go
package phantom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultStream   = "phantom.ingest"
	defaultSubject  = "phantom.ingest.events"
	defaultMaxBatch = 500

	// Startup race: phantom consumer creates the stream; faina-api may publish before it's ready.
	// Retry ErrNoResponders with exponential backoff up to this ceiling.
	retryMaxTotal = 30 * time.Second
	retryInitial  = 100 * time.Millisecond
	retryCap      = 5 * time.Second
)

// Event is a single analytics event to be published to Phantom Tracker.
// ID is auto-generated if empty. Timestamp defaults to time.Now() if zero.
type Event struct {
	ID         string         `json:"id"`
	WebsiteID  string         `json:"website_id"`
	VisitorID  string         `json:"visitor_id,omitempty"`
	SessionID  string         `json:"session_id,omitempty"`
	Name       string         `json:"name"`
	IPAddress  string         `json:"ip_address"`
	UserAgent  string         `json:"user_agent,omitempty"`
	Language   string         `json:"language,omitempty"`
	URL        string         `json:"url,omitempty"`
	Referer    string         `json:"referer,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
	Timestamp  time.Time      `json:"timestamp,omitempty"`
}

func (e *Event) validate() error {
	if e.WebsiteID == "" {
		return fmt.Errorf("phantom: Event.WebsiteID is required")
	}
	if e.Name == "" {
		return fmt.Errorf("phantom: Event.Name is required")
	}
	return nil
}

type ingestBatch struct {
	Events []Event `json:"events"`
}

// Option configures a Client.
type Option func(*Client)

// WithStream overrides the NATS stream name (default: "phantom.ingest").
func WithStream(stream string) Option {
	return func(c *Client) { c.stream = stream }
}

// WithSubject overrides the NATS subject (default: "phantom.ingest.events").
func WithSubject(subject string) Option {
	return func(c *Client) { c.subject = subject }
}

// WithMaxBatch sets the maximum number of events per NATS message (default: 500).
func WithMaxBatch(n int) Option {
	return func(c *Client) { c.maxBatch = n }
}

// WithBuffer enables buffered (async) publishing for high-frequency, fire-and-forget traffic.
//
// Events from concurrent Ingest() callers are collected and flushed as batched NATS messages
// when the internal buffer reaches maxEvents or maxAge elapses — whichever comes first.
//
// In buffered mode:
//   - Ingest() enqueues events and returns nil immediately (not ack-backed — best-effort)
//   - If the channel is full (10×maxEvents capacity), excess events are dropped silently
//   - Close() drains and flushes remaining events before closing the NATS connection
//
// Use for pixel/impression tracking where throughput matters more than per-call durability.
// For paths where callers need ack before responding (e.g. TrackBatch), use a non-buffered client.
func WithBuffer(maxEvents int, maxAge time.Duration) Option {
	return func(c *Client) {
		c.bufMaxEvents = maxEvents
		c.bufMaxAge = maxAge
	}
}

// Client publishes events to Phantom Tracker via NATS JetStream.
// Thread-safe; safe for concurrent use.
type Client struct {
	conn     *nats.Conn
	js       jetstream.JetStream
	ownsConn bool // true when NewClient created conn; false when NewClientWithConn
	stream   string
	subject  string
	maxBatch int

	// buffered mode — nil buf means sync mode
	buf          chan Event
	bufMaxEvents int
	bufMaxAge    time.Duration
	bufStop      chan struct{}
	bufDone      chan struct{}
}

// NewClient creates a Phantom Tracker client and opens a dedicated NATS connection.
// Close() will close the connection. Use NewClientWithConn to share a connection.
func NewClient(natsURL string, opts ...Option) (*Client, error) {
	conn, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("phantom: failed to connect to NATS: %w", err)
	}

	c, err := newClient(conn, opts...)
	if err != nil {
		conn.Close()
		return nil, err
	}
	c.ownsConn = true
	return c, nil
}

// NewClientWithConn creates a Phantom Tracker client using an existing NATS connection.
// The caller owns the connection lifecycle — Close() flushes the buffer but does NOT
// close conn. Call conn.Close() explicitly after all clients sharing it are closed.
func NewClientWithConn(conn *nats.Conn, opts ...Option) (*Client, error) {
	return newClient(conn, opts...)
}

func newClient(conn *nats.Conn, opts ...Option) (*Client, error) {
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("phantom: failed to create JetStream context: %w", err)
	}

	c := &Client{
		conn:     conn,
		js:       js,
		stream:   defaultStream,
		subject:  defaultSubject,
		maxBatch: defaultMaxBatch,
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.bufMaxEvents > 0 {
		c.buf = make(chan Event, c.bufMaxEvents*10)
		c.bufStop = make(chan struct{})
		c.bufDone = make(chan struct{})
		go c.runBuffer()
	}

	return c, nil
}

// Ingest publishes events to phantom.ingest. Auto-fills Event.ID and Timestamp if empty.
// Splits events into batches of up to MaxBatch per NATS message.
//
// Sync mode (default): returns after JetStream ack. Retries on ErrNoResponders (startup race)
// with exponential backoff capped at 30s total.
//
// Buffered mode (WithBuffer): enqueues events and returns nil immediately. Best-effort.
func (c *Client) Ingest(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	for i := range events {
		if err := events[i].validate(); err != nil {
			return fmt.Errorf("phantom: event[%d]: %w", i, err)
		}
		if events[i].ID == "" {
			events[i].ID = uuid.New().String()
		}
		if events[i].Timestamp.IsZero() {
			events[i].Timestamp = time.Now()
		}
	}

	if c.buf != nil {
		for _, e := range events {
			select {
			case c.buf <- e:
			default:
				// channel full: drop — best-effort fire-and-forget
			}
		}
		return nil
	}

	return c.publish(ctx, events)
}

// publish sends events as batched NATS messages, retrying ErrNoResponders with backoff.
func (c *Client) publish(ctx context.Context, events []Event) error {
	for i := 0; i < len(events); i += c.maxBatch {
		end := i + c.maxBatch
		if end > len(events) {
			end = len(events)
		}
		data, err := json.Marshal(ingestBatch{Events: events[i:end]})
		if err != nil {
			return fmt.Errorf("phantom: failed to marshal batch [%d:%d]: %w", i, end, err)
		}
		if err := c.publishWithRetry(ctx, data); err != nil {
			return fmt.Errorf("phantom: failed to publish batch [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

// publishWithRetry calls js.Publish and retries on ErrNoResponders using exponential backoff.
// ErrNoResponders means the phantom.ingest stream doesn't exist yet (startup race).
// All other errors are returned immediately.
func (c *Client) publishWithRetry(ctx context.Context, data []byte) error {
	backoff := retryInitial
	deadline := time.Now().Add(retryMaxTotal)

	for {
		_, err := c.js.Publish(ctx, c.subject, data)
		if err == nil {
			return nil
		}
		if !errors.Is(err, nats.ErrNoResponders) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("phantom.ingest stream unavailable after %s: %w", retryMaxTotal, err)
		}
		wait := backoff
		if wait > remaining {
			wait = remaining
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		backoff *= 2
		if backoff > retryCap {
			backoff = retryCap
		}
	}
}

// runBuffer is the background goroutine for buffered mode.
// Flushes when batch reaches bufMaxEvents or bufMaxAge elapses.
func (c *Client) runBuffer() {
	defer close(c.bufDone)

	ticker := time.NewTicker(c.bufMaxAge)
	defer ticker.Stop()

	batch := make([]Event, 0, c.bufMaxEvents)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), retryMaxTotal)
		defer cancel()
		_ = c.publish(ctx, batch) // best-effort: errors dropped in buffered mode
		batch = batch[:0]
	}

	for {
		select {
		case e := <-c.buf:
			batch = append(batch, e)
			if len(batch) >= c.bufMaxEvents {
				flush()
				ticker.Reset(c.bufMaxAge)
			}
		case <-ticker.C:
			flush()
		case <-c.bufStop:
			// drain channel then flush
			for len(c.buf) > 0 {
				batch = append(batch, <-c.buf)
			}
			flush()
			return
		}
	}
}

// Close flushes the buffer (if in buffered mode) and, when the client owns its
// NATS connection (created via NewClient), closes it. When the connection was
// provided via NewClientWithConn, Close() does NOT close the connection — the
// caller must call conn.Close() explicitly after all sharing clients are closed.
func (c *Client) Close() {
	if c.buf != nil {
		close(c.bufStop)
		<-c.bufDone
	}
	if c.ownsConn && c.conn != nil {
		c.conn.Close()
	}
}
