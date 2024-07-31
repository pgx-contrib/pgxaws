package pgxaws

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
)

// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func BeforeConnect(ctx context.Context, conn *pgx.ConnConfig) error {
	if region := conn.RuntimeParams["aws_region"]; region != "" {
		session, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return err
		}

		// prepare the endpoint
		endpoint := conn.Host + ":" + strconv.Itoa(int(conn.Port))
		// issue the token
		token, err := auth.BuildAuthToken(ctx,
			endpoint, region, conn.User, session.Credentials)
		if err != nil {
			return err
		}

		// set the token as password
		conn.Password = token
	}

	return nil
}
