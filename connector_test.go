package pgxaws_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgx-contrib/pgxaws"
)

func ExampleConnector() {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	ctx := context.TODO()
	// Create a new pgxaws.Connector
	connector, err := pgxaws.Connect(ctx)
	if err != nil {
		panic(err)
	}

	// Set BeforeConnect hook to pgxaws.BeforeConnect
	config.BeforeConnect = connector.BeforeConnect

	// Create a new pgxpool with the config
	conn, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}

	rows, err := conn.Query(ctx, "SELECT * from organization")
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
