package pgxaws_test

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgx-contrib/pgxaws"
	"github.com/pgx-contrib/pgxcache"
)

func ExampleDynamoQueryCacher() {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	conn, err := pgxpool.NewWithConfig(context.TODO(), config)
	if err != nil {
		panic(err)
	}

	// Create a new client
	client := NewClient()
	// Create a new cacher
	cacher := pgxaws.NewDynamoQueryCacher(client, "queries")

	// create a new querier
	querier := &pgxcache.Querier{
		// set the default query options, which can be overridden by the query
		// -- @cache-max-rows 100
		// -- @cache-ttl 30s
		Options: &pgxcache.QueryOptions{
			MaxLifetime: 30 * time.Second,
			MaxRows:     1,
		},
		Cacher:  cacher,
		Querier: conn,
	}

	row := querier.QueryRow(context.TODO(), "SELECT 1")
	if err := row.Scan(&count); err != nil {
		panic(err)
	}
}

// NewClient creates a new dynamodb client
func NewClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}
