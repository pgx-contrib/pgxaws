package pgxaws

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go/logging"
	"github.com/jackc/pgx/v5"
)

// Connector connects the pgx to AWS RDS.
type Connector struct {
	// Close is the close function.
	close func()
	// Token is the bearer token.
	token atomic.Pointer[string]
	// Config is the AWS configuration.
	config aws.Config
}

// Connect creates a new connector.
func Connect(ctx context.Context, options ...func(*config.LoadOptions) error) (*Connector, error) {
	// prepare the logger
	WithLogger := func(opt *config.LoadOptions) error {
		opt.Logger = logging.Nop{}
		return nil
	}
	// prepare the default options
	options = slices.Insert(options, 0, WithLogger)
	// prepare the AWS settings
	config, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, err
	}

	return &Connector{config: config}, nil
}

// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func (x *Connector) BeforeConnect(ctx context.Context, config *pgx.ConnConfig) (err error) {
	// if there is no user, we can't issue a token
	if config.User == "" {
		x.config.Logger.Logf(logging.Debug, "no user set")
		return
	}

	// if there is no region, we can't issue a token
	if x.config.Region == "" {
		x.config.Logger.Logf(logging.Debug, "no region set")
		return
	}

	token := x.token.Load()
	// we should authorize the token
	if token == nil {
		// authorize
		if token, err = x.authorize(ctx, config); err != nil {
			x.config.Logger.Logf(logging.Debug, err.Error())
			return err
		}
		// set the token
		x.token.Store(token)
		// prepare the Background context
		ctx = context.Background()
		ctx, x.close = context.WithCancel(ctx)
		// refresh the token
		go x.session(ctx, config)
	}

	// set the token as password
	config.Password = *token
	// done!
	return nil
}

// Close closes the connector.
func (x *Connector) Close() {
	if x.close == nil {
		return
	}

	x.close()
}

// Session refreshes the token.
func (x *Connector) session(ctx context.Context, config *pgx.ConnConfig) {
	ticker := time.NewTicker(10 * time.Minute)

	for {
		select {
		case <-ticker.C:
			token, err := x.authorize(ctx, config)
			if err != nil {
				x.config.Logger.Logf(logging.Warn, err.Error())
			}
			// store the token
			x.token.Store(token)
		case <-ctx.Done():
			x.token.Store(nil)
			x.close = nil
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
		return nil, fmt.Errorf("unsupported")
	}

	return auth.Authorize(ctx, config)
}
