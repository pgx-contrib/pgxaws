package pgxaws

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
)

// Connector connects the the pgx to AWS RDS.
type Connector struct {
	// Options to configure the AWS session
	Options []func(*config.LoadOptions) error
	// SkipAuth skips the authentication
	SkipAuth bool
}

// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func (x *Connector) BeforeConnect(ctx context.Context, conn *pgx.ConnConfig) error {
	// skip any authentication
	if x.SkipAuth {
		return nil
	}

	if conn.User != "" {
		session, err := config.LoadDefaultConfig(ctx, x.Options...)
		if err != nil {
			return err
		}

		// prepare the endpoint
		endpoint := conn.Host + ":" + strconv.Itoa(int(conn.Port))
		// issue the token
		token, err := auth.BuildAuthToken(ctx, endpoint, session.Region, conn.User, session.Credentials)
		if err != nil {
			return err
		}

		// set the token as password
		conn.Password = token
	}

	return nil
}
