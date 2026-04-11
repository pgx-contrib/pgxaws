package pgxaws

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go/logging"
	"github.com/jackc/pgx/v5"
)

// Connector connects the pgx to AWS RDS.
type Connector struct {
	mu          sync.Mutex
	initialized bool
	close       context.CancelFunc
	token       atomic.Pointer[string]
	config      aws.Config
}

// Connect creates a new connector.
func Connect(ctx context.Context, options ...func(*config.LoadOptions) error) (*Connector, error) {
	// Prepend a no-op logger as the default; caller-supplied options that set
	// a logger will override it because they are appended after.
	withLogger := func(opt *config.LoadOptions) error {
		opt.Logger = logging.Nop{}
		return nil
	}
	options = slices.Insert(options, 0, withLogger)

	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, err
	}

	return &Connector{config: cfg}, nil
}

// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func (x *Connector) BeforeConnect(ctx context.Context, config *pgx.ConnConfig) error {
	if config.User == "" {
		x.config.Logger.Logf(logging.Debug, "no user set")
		return nil
	}
	if x.config.Region == "" {
		x.config.Logger.Logf(logging.Debug, "no region set")
		return nil
	}

	// Fast path: a valid token is already cached.
	if token := x.token.Load(); token != nil {
		config.Password = *token
		return nil
	}

	// Slow path: acquire the lock and initialize. Multiple concurrent callers
	// may all observe a nil token; the mutex ensures only one performs the
	// authorization and goroutine spawn.
	x.mu.Lock()
	defer x.mu.Unlock()

	// Double-check after acquiring the lock; another goroutine may have
	// stored a token while we were waiting.
	if token := x.token.Load(); token != nil {
		config.Password = *token
		return nil
	}

	token, err := x.authorize(ctx, config)
	if err != nil {
		x.config.Logger.Logf(logging.Debug, err.Error())
		return err
	}
	x.token.Store(token)

	if !x.initialized {
		bgCtx, cancel := context.WithCancel(context.Background())
		x.close = cancel
		x.initialized = true
		go x.session(bgCtx, config)
	}

	config.Password = *token
	return nil
}

// Close stops the background token refresh goroutine and clears the cached token.
// After Close returns, the connector can be re-used; BeforeConnect will
// re-authorize and restart the refresh goroutine on the next call.
func (x *Connector) Close() {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.close != nil {
		x.close()
		x.close = nil
	}
	x.initialized = false
	x.token.Store(nil)
}

// session refreshes the token every 10 minutes until ctx is cancelled.
// Both RDS and DSQL tokens have a 15-minute validity window; refreshing at
// 10 minutes provides a 5-minute safety margin.
func (x *Connector) session(ctx context.Context, config *pgx.ConnConfig) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			token, err := x.authorize(ctx, config)
			if err != nil {
				// Keep the current token active; it remains valid for up to
				// 15 minutes from when it was issued.
				x.config.Logger.Logf(logging.Warn, err.Error())
				continue
			}
			x.token.Store(token)
		case <-ctx.Done():
			return
		}
	}
}

// Authorizer is an interface that defines the authorization method for the connector.
type Authorizer interface {
	Authorize(ctx context.Context, config *pgx.ConnConfig) (*string, error)
}

func (x *Connector) authorize(ctx context.Context, config *pgx.ConnConfig) (*string, error) {
	var auth Authorizer

	switch {
	case strings.Contains(config.Host, ".rds."):
		auth = &RDSAuth{Config: &x.config}
	case strings.Contains(config.Host, ".dsql."):
		auth = &DSQLAuth{Config: &x.config}
	default:
		return nil, fmt.Errorf("unsupported host %q: must contain .rds. or .dsql.", config.Host)
	}

	return auth.Authorize(ctx, config)
}
