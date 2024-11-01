package pgxaws

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/smithy-go/logging"
	"github.com/jackc/pgx/v5"
)

// Connector connects the the pgx to AWS RDS.
type Connector struct {
	// token is the bearer token.
	token atomic.Pointer[string]
	// connection count.
	count atomic.Int64
	// config is the AWS configuration.
	config aws.Config
}

// Connect creates a new connector.
func Connect(ctx context.Context, options ...func(*config.LoadOptions) error) (*Connector, error) {
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
	// increment the connection count
	x.count.Add(1)

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
		// refresh the token
		go x.refresh(ctx, config)
	}

	// set the token as password
	config.Password = *token
	// done!
	return nil
}

// BeforeClose is called before a connection is closed.
func (x *Connector) BeforeClose(conn *pgx.Conn) {
	// decrement the connection count
	x.count.Add(-1)
}

func (x *Connector) refresh(ctx context.Context, config *pgx.ConnConfig) {
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
		default:
			if count := x.count.Load(); count == 0 {
				x.config.Logger.Logf(logging.Debug, "stop refreshing token")
				// set the token to nil so we can prepare the connector to start again if needed
				x.token.Store(nil)
				return
			}
			// wait before we check again
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (x *Connector) authorize(ctx context.Context, config *pgx.ConnConfig) (*string, error) {
	endpoint := config.Host + ":" + strconv.Itoa(int(config.Port))
	// build token
	token, err := auth.BuildAuthToken(ctx, endpoint, x.config.Region, config.User, x.config.Credentials)
	if err != nil {
		return nil, err
	}

	return &token, nil
}
