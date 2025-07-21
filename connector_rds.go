package pgxaws

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
)

var _ Authorizer = (*RDSAuth)(nil)

// RDSAuth is an implementation of pgxaws.Auth that uses AWS RDS IAM authentication.
type RDSAuth struct {
	// Config is the AWS configuration.
	Config *aws.Config
}

// Authorize authorizes the connection to AWS RDS using IAM authentication.
func (x *RDSAuth) Authorize(ctx context.Context, config *pgx.ConnConfig) (*string, error) {
	endpoint := config.Host + ":" + strconv.Itoa(int(config.Port))
	// build token
	token, err := auth.BuildAuthToken(ctx, endpoint, x.Config.Region, config.User, x.Config.Credentials)
	if err != nil {
		return nil, err
	}

	return &token, nil
}
