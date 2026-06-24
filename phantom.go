// Package phantom provides a Go client for publishing events to Phantom Tracker
// via NATS JetStream (phantom.ingest stream).
//
// Usage:
//
//	client, err := phantom.NewClient("nats://localhost:4222")
//	if err != nil { ... }
//	defer client.Close()
//
//	err = client.Ingest(ctx, []phantom.Event{
//	    {
//	        Name:      "impression",
//	        WebsiteID: "ws_abc123",
//	        IPAddress: "5.116.56.197",
//	        UserAgent: "Mozilla/5.0...",
//	        Properties: map[string]any{"ad_id": "abc123"},
//	    },
//	})
//
// Module: github.com/faina-labs/phantom-client-go
package phantom

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultStream  = "phantom.ingest"
	defaultSubject = "phantom.ingest.events"
	defaultMaxBatch = 500
)

// Event is a single analytics event to be published to Phantom Tracker.
// ID is auto-generated if empty.
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

// ingestBatch is the wire format for phantom.ingest messages.
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

// Client publishes events to Phantom Tracker via NATS JetStream.
// Thread-safe; safe for concurrent use.
type Client struct {
	conn     *nats.Conn
	js       jetstream.JetStream
	stream   string
	subject  string
	maxBatch int
}

// NewClient creates and connects a Phantom Tracker client.
// Returns after NATS connection is established and JetStream context created.
func NewClient(natsURL string, opts ...Option) (*Client, error) {
	conn, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("phantom: failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
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
	return c, nil
}

// Ingest publishes events to phantom.ingest. Auto-fills Event.ID if empty.
// Splits events into batches of up to MaxBatch per NATS message.
// Returns after JetStream acknowledgment — guarantees durability (<0.5ms per batch).
// Returns error if any batch fails; successfully published batches are not rolled back.
// Safe for concurrent use — nats.go JetStream handles locking internally.
func (c *Client) Ingest(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Validate all events up front — fail fast, no NATS round-trip for bad data
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

	for i := 0; i < len(events); i += c.maxBatch {
		end := i + c.maxBatch
		if end > len(events) {
			end = len(events)
		}

		batch := ingestBatch{Events: events[i:end]}
		data, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("phantom: failed to marshal batch [%d:%d]: %w", i, end, err)
		}

		if _, err := c.js.Publish(ctx, c.subject, data); err != nil {
			return fmt.Errorf("phantom: failed to publish batch [%d:%d]: %w", i, end, err)
		}
	}

	return nil
}

// Close closes the NATS connection.
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}
