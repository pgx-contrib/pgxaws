package pgxaws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/jackc/pgx/v5"
)

// DSQLAuth is an implementation of pgxaws.Auth that uses AWS DSQL IAM authentication.
type DSQLAuth struct {
	// Config is the AWS configuration.
	Config *aws.Config
}

// Authorize authorizes the connection to AWS DSQL using IAM authentication.
func (x *DSQLAuth) Authorize(ctx context.Context, config *pgx.ConnConfig) (*string, error) {
	var BuildAuthToken func(ctx context.Context, endpoint, region string, creds aws.CredentialsProvider, optFns ...func(options *auth.TokenOptions)) (string, error)

	if config.User != "admin" {
		BuildAuthToken = auth.GenerateDbConnectAuthToken
	} else {
		BuildAuthToken = auth.GenerateDBConnectAdminAuthToken
	}

	// build token
	token, err := BuildAuthToken(ctx, config.Host, x.Config.Region, x.Config.Credentials)
	if err != nil {
		return nil, err
	}

	return &token, nil
}
