package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// shutdownGrace bounds how long a server waits for in-flight requests before
// it drops them. The harness starts and stops these processes dozens of times
// per run, and a node that lingers holding its port poisons every later run,
// so the wait is short and hard.
const shutdownGrace = 3 * time.Second

// A Config is late configuration, delivered over POST /configure.
//
// The harness launches every node with -addr 127.0.0.1:0 so the kernel picks
// the ports, which means no node can know its peers' addresses at launch. The
// fields are pointers so that "not mentioned" and "set to nothing" stay
// distinct: a ForwardStore told {"leader":""} is being made the leader, and a
// QuorumStore told {"peers":[]} is being isolated, whereas a store told {} is
// being left alone.
type Config struct {
	// Leader is the address a ForwardStore follower forwards to. An empty
	// string means this node is itself the leader.
	Leader *string `json:"leader,omitempty"`
	// Peers is the set of nodes a QuorumStore replicates to.
	Peers *[]string `json:"peers,omitempty"`
	// Sync switches a QuorumStore into synchronous replication: a write waits
	// for its peers before replying, and then reports success whether or not
	// they answered. See kv.QuorumStore for why that is deliberate.
	Sync *bool `json:"sync,omitempty"`
	// Promote lets a ForwardStore follower appoint itself leader once it has
	// lost contact, and serve clients from a cache of what it saw go past.
	// See kv.ForwardStore for why that is deliberate.
	Promote *bool `json:"promote,omitempty"`
	// PromoteAfter is how many consecutive failed forwards it takes.
	PromoteAfter *int `json:"promote_after,omitempty"`
}

// A Store is the key-value behaviour behind the HTTP surface. The three
// implementations in this package are the systems under test.
type Store interface {
	// Apply performs one operation.
	//
	// A nil error means the store reached a decision and the Response says
	// what it was, including a Response with OK false, which asserts that the
	// operation definitely did not take effect.
	//
	// A non-nil error means something weaker and much more important: the
	// store does not know whether the operation took effect. The server turns
	// that into a 502 so the client records it as indeterminate. Returning a
	// declining Response where an error is meant would let the checker delete
	// an operation that really happened.
	Apply(ctx context.Context, req Request) (Response, error)

	// Configure replaces the store's view of its peers. It must be safe to
	// call before any traffic arrives, concurrently with Apply, and more than
	// once; calling it twice replaces the configuration rather than adding to
	// it. Fields left nil in cfg are left alone.
	Configure(cfg Config) error

	// Close stops any background work the store started and waits for it.
	Close() error
}

// Indeterminate reports that the store does not know whether the operation
// took effect, and names the HTTP status the server should answer with.
//
// The status is purely descriptive - any non-200 makes the client record
// history.Info - but it is worth being accurate, because these servers are
// meant to be legible to a person with curl. A follower that could not get an
// answer out of its leader says 504; a store that has no better explanation
// says 502.
func Indeterminate(status int, format string, args ...any) error {
	return &statusError{status: status, msg: fmt.Sprintf(format, args...)}
}

type statusError struct {
	status int
	msg    string
}

// Error renders the reason the store could not decide.
func (e *statusError) Error() string { return e.msg }

// statusOf picks the status to report for an error out of Store.Apply.
func statusOf(err error) int {
	var se *statusError
	if errors.As(err, &se) {
		return se.status
	}
	return http.StatusBadGateway
}

// A Router is a Store that serves endpoints of its own beyond /kv. QuorumStore
// uses it to add /replicate for peer traffic.
type Router interface {
	// Routes registers the store's own handlers.
	Routes(mux *http.ServeMux)
}

// NewLogger returns the structured one-line-per-event logger the binaries use.
// It must never be pointed at stdout: the harness parses stdout for the bound
// address and one stray line there sends every later request to the wrong
// place.
func NewLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

// A Server exposes a Store over HTTP.
type Server struct {
	id    string
	store Store
	log   *slog.Logger
	mux   *http.ServeMux
	http  *http.Server
	ln    net.Listener
}

// NewServer wraps store in the shared HTTP surface. id is the node identifier
// reported by /health and attached to every log line. A nil logger discards.
func NewServer(id string, store Store, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{id: id, store: store, log: logger, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /kv", s.handleKV)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /configure", s.handleConfigure)
	if r, ok := store.(Router); ok {
		r.Routes(s.mux)
	}
	s.http = &http.Server{
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelDebug),
	}
	return s
}

// Handler exposes the routes without binding a port, for httptest.
func (s *Server) Handler() http.Handler { return s.mux }

// Listen binds addr without serving, so that a caller can learn the resolved
// address before any request can arrive. Passing a port of 0 lets the kernel
// choose.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("binding %s: %w", addr, err)
	}
	s.ln = ln
	return nil
}

// Addr returns the bound address, empty before Listen.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Serve handles requests until Shutdown. It returns nil on a clean shutdown
// rather than http.ErrServerClosed, because that is not an error a caller
// should have to recognise.
func (s *Server) Serve() error {
	if s.ln == nil {
		return errors.New("serve before listen")
	}
	if err := s.http.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops serving, then closes the store. It is safe to call more than
// once.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)
	if cerr := s.store.Close(); err == nil {
		err = cerr
	}
	return err
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, struct {
		OK bool   `json:"ok"`
		ID string `json:"id"`
	}{true, s.id})
}

func (s *Server) handleConfigure(w http.ResponseWriter, r *http.Request) {
	var cfg Config
	dec := json.NewDecoder(io.LimitReader(r.Body, maxRequestBytes))
	if err := dec.Decode(&cfg); err != nil {
		s.log.Warn("configure rejected", "node", s.id, "error", err)
		writeJSON(w, http.StatusOK, Declined("malformed configuration: %v", err))
		return
	}
	if err := s.store.Configure(cfg); err != nil {
		s.log.Warn("configure rejected", "node", s.id, "error", err)
		writeJSON(w, http.StatusOK, Declined("%v", err))
		return
	}
	s.log.Info("configured", "node", s.id, "leader", derefOr(cfg.Leader, "(unchanged)"), "peers", peersOf(cfg))
	writeJSON(w, http.StatusOK, Response{OK: true})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	started := time.Now()

	req, err := decodeRequest(r.Body)
	if err != nil {
		// A request the server could not even parse was certainly not
		// applied, so this is a definite refusal and the client is right to
		// record it as a failure. Answering 200 with ok:false rather than 400
		// keeps one rule on the wire: a 200 is a decision, anything else is
		// the server admitting it does not know.
		writeJSON(w, http.StatusOK, Declined("%v", err))
		s.log.Info("kv", "node", s.id, "op", "unparseable", "ok", false,
			"error", err.Error(), "dur_us", micros(started))
		return
	}

	resp, err := s.store.Apply(r.Context(), req)
	if err != nil {
		// No "ok" field on this reply. The client decides on the status
		// before it looks at the body, but a fixture that puts a plausible
		// decision into an indeterminate reply is an invitation for the next
		// reader of a history to misinterpret it.
		writeJSON(w, statusOf(err), struct {
			Err string `json:"error"`
		}{err.Error()})
		s.log.Info("kv", "node", s.id, "op", req.String(), "ok", "unknown",
			"error", err.Error(), "dur_us", micros(started))
		return
	}

	writeJSON(w, http.StatusOK, resp)
	s.log.Info("kv", "node", s.id, "op", req.String(), "ok", resp.OK,
		"result", describe(resp), "error", resp.Err, "dur_us", micros(started))
}

func describe(r Response) string {
	switch {
	case r.Value != nil:
		return fmt.Sprintf("value=%d", *r.Value)
	case r.Swapped != nil:
		return fmt.Sprintf("swapped=%t", *r.Swapped)
	default:
		return ""
	}
}

func micros(since time.Time) int64 { return time.Since(since).Microseconds() }

func derefOr(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	if *s == "" {
		return "(self)"
	}
	return *s
}

func peersOf(cfg Config) string {
	if cfg.Peers == nil {
		return "(unchanged)"
	}
	return fmt.Sprint(*cfg.Peers)
}

// writeJSON sends v with a definite Content-Length, so a client that reads a
// short body knows the connection broke rather than that the server finished.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		// Nothing here is capable of failing to marshal, but a panic in a
		// handler would be indistinguishable to the client from a crash, and
		// a fixture that crashes is a fixture nobody trusts.
		http.Error(w, `{"error":"encoding the reply failed"}`, http.StatusInternalServerError)
		return
	}
	body = append(body, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(status)
	w.Write(body)
}

// Run is the body of every kv binary: bind, announce, serve, stop cleanly.
//
// It writes exactly one line to stdout, "listening <addr>\n", carrying the
// address the kernel actually chose. The harness parses that line, so nothing
// else in any of these binaries may write to stdout; logs go to stderr. The
// write is a single syscall on an unbuffered *os.File, so it needs no explicit
// flush and cannot be interleaved with a later one. It happens only after the
// stop signals are being watched, so that announcing readiness never precedes
// being able to shut down cleanly.
//
// It returns when ctx is cancelled, when SIGINT or SIGTERM arrives, or when
// stdin reaches EOF. The last of those is how the harness stops a node on
// Windows, which has no usable SIGTERM; the consequence is that running one of
// these binaries with stdin already closed makes it exit at once, which is
// correct but surprising the first time.
func Run(ctx context.Context, addr, id string, store Store, logger *slog.Logger) error {
	srv := NewServer(id, store, logger)
	if err := srv.Listen(addr); err != nil {
		return err
	}
	// The signal handler goes in before the address is announced, and the
	// order is load-bearing. Announcing is what tells a supervisor the node is
	// ready, and a supervisor that stops it immediately afterwards would
	// otherwise land its SIGTERM in the window before signal.NotifyContext has
	// registered - where the default disposition applies and the process dies
	// by signal instead of shutting down. Found by CI on Linux; Windows has no
	// SIGTERM a parent can send, so it could not have surfaced here.
	ctx, stopSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		io.Copy(io.Discard, os.Stdin)
		cancel()
	}()

	if _, err := fmt.Fprintf(os.Stdout, "listening %s\n", srv.Addr()); err != nil {
		return fmt.Errorf("announcing the bound address: %w", err)
	}
	if logger != nil {
		logger.Info("listening", "node", id, "addr", srv.Addr())
	}

	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	var err error
	select {
	case err = <-served:
	case <-ctx.Done():
	}

	shutdownCtx, done := context.WithTimeout(context.Background(), shutdownGrace)
	defer done()
	if serr := srv.Shutdown(shutdownCtx); err == nil {
		err = serr
	}
	if logger != nil {
		logger.Info("stopped", "node", id)
	}
	return err
}
