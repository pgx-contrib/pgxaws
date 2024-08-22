package pgxaws_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgx-contrib/pgxaws"
)

func ExampleConnector() {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	// Set max connection lifetime to 5 minutes in postgres connection pool configuration.
	// Note: this will refresh connections before the 15 min expiration on the IAM AWS auth token,
	// while leveraging the BeforeConnect hook to recreate the token in time dynamically.
	config.MaxConnLifetime = 5 * time.Minute

	// Create a new pgxaws.Connector
	connector := &pgxaws.Connector{}
	// Set BeforeConnect hook to pgxaws.BeforeConnect
	config.BeforeConnect = connector.BeforeConnect

	// Create a new pgxpool with the config
	conn, err := pgxpool.NewWithConfig(context.TODO(), config)
	if err != nil {
		panic(err)
	}

	rows, err := conn.Query(context.TODO(), "SELECT * from organization")
	if err != nil {
		panic(err)
	}
	// close the rows
	defer rows.Close()

	// Organization struct must be defined
	type Organization struct {
		Name string `db:"name"`
	}

	for rows.Next() {
		organization, err := pgx.RowToStructByName[Organization](rows)
		if err != nil {
			panic(err)
		}

		fmt.Println(organization.Name)
	}
}
