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
	if conn.User != "" {
		if region, ok := conn.RuntimeParams["aws_region"]; ok && region != "" {
			session, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return err
			}

			// prepare the endpoint
			endpoint := conn.Host + ":" + strconv.Itoa(int(conn.Port))
			// issue the token
			token, err := auth.BuildAuthToken(ctx, endpoint, region, conn.User, session.Credentials)
			if err != nil {
				return err
			}

			// set the token as password
			conn.Password = token
			// remove the region from the runtime parameters
			delete(conn.RuntimeParams, "aws_region")
		}
	}

	return nil
}
