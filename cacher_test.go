package pgxaws_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jackc/pgx/v5"
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
	// close the connection
	defer conn.Close()

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

	rows, err := querier.Query(context.TODO(), "SELECT * from customer")
	if err != nil {
		panic(err)
	}
	// close the rows
	defer rows.Close()

	// Customer struct must be defined
	type Customer struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}

	for rows.Next() {
		customer, err := pgx.RowToStructByName[Customer](rows)
		if err != nil {
			panic(err)
		}

		fmt.Println(customer.FirstName)
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
