package pgxaws

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/smithy-go/auth/bearer"
	"github.com/jackc/pgx/v5"
)

// Connector connects the the pgx to AWS RDS.
type Connector struct {
	// token is the bearer token.
	token atomic.Pointer[bearer.Token]
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
	// if there is no user, we can't issue a token
	if config.User == "" {
		return nil
	}

	// if there is no region, we can't issue a token
	if x.config.Region == "" {
		return nil
	}

	now := time.Now()
	// get the token
	token := x.token.Load()
	// issue new token
	if token == nil || token.Expired(now) {
		// issue new token
		token = &bearer.Token{
			Expires:   now.Add(10 * time.Minute),
			CanExpire: true,
		}
		// issue the token
		if token.Value, err = x.auth(ctx, config); err != nil {
			return err
		}
		// set the token
		x.token.Store(token)
	}

	// set the token as password
	config.Password = token.Value
	// done!
	return nil
}

func (x *Connector) auth(ctx context.Context, config *pgx.ConnConfig) (string, error) {
	endpoint := config.Host + ":" + strconv.Itoa(int(config.Port))
	// build token
	return auth.BuildAuthToken(ctx, endpoint, x.config.Region, config.User, x.config.Credentials)
}
