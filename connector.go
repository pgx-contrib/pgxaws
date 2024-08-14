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
}

// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func (x *Connector) BeforeConnect(ctx context.Context, conn *pgx.ConnConfig) error {
	// if there is no user, we can't issue a token
	if conn.User == "" {
		return nil
	}

	// prepare the AWS settings
	settings, err := config.LoadDefaultConfig(ctx, x.Options...)
	if err != nil {
		return err
	}

	// if there is no region, we can't issue a token
	if settings.Region == "" {
		return nil
	}

	// issue the token
	token, err := auth.BuildAuthToken(ctx, x.endpoint(conn), settings.Region, conn.User, settings.Credentials)
	if err != nil {
		return err
	}

	// set the token as password
	conn.Password = token
	// done!
	return nil
}

func (x *Connector) endpoint(conn *pgx.ConnConfig) string {
	return conn.Host + ":" + strconv.Itoa(int(conn.Port))
}
