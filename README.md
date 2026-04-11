# pgxaws

[![CI](https://github.com/pgx-contrib/pgxaws/actions/workflows/ci.yaml/badge.svg)](https://github.com/pgx-contrib/pgxaws/actions/workflows/ci.yaml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxaws)](https://github.com/pgx-contrib/pgxaws/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxaws.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxaws)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxaws)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pgx-contrib/pgxaws)](go.mod)
[![pgx](https://img.shields.io/badge/pgx-v5-blue)](https://github.com/jackc/pgx)
[![AWS](https://img.shields.io/badge/AWS-SDK_v2-orange)](https://github.com/aws/aws-sdk-go-v2)

AWS IAM authentication for [pgx v5](https://github.com/jackc/pgx), supporting Amazon RDS and Aurora DSQL connections, plus query caching backends using DynamoDB and S3.

## Features

- **IAM Authentication** for Amazon RDS and Aurora DSQL via `pgx.ConnConfig.BeforeConnect`
- **Automatic token refresh** — tokens are renewed every 10 minutes in the background
- **DynamoQueryCacher** — query result caching backed by DynamoDB (implements [pgxcache](https://github.com/pgx-contrib/pgxcache))
- **S3QueryCacher** — query result caching backed by S3 (implements [pgxcache](https://github.com/pgx-contrib/pgxcache))

## Installation

```bash
go get github.com/pgx-contrib/pgxaws
```

## Usage

### Connector (RDS / DSQL)

The `Connector` automatically detects whether the host is an RDS or DSQL endpoint and issues the appropriate IAM auth token.

```go
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
// close the connector
defer connector.Close()

config.BeforeConnect = connector.BeforeConnect

// Create a new pgxpool with the config
pool, err := pgxpool.NewWithConfig(ctx, config)
if err != nil {
    panic(err)
}
defer pool.Close()

rows, err := pool.Query(ctx, "SELECT * from organization")
if err != nil {
    panic(err)
}
defer rows.Close()

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
```

### DynamoQueryCacher

Cache query results in DynamoDB using [pgxcache](https://github.com/pgx-contrib/pgxcache):

```go
// Create a new cacher
cacher := &pgxaws.DynamoQueryCacher{
    Client: dynamodb.NewFromConfig(cfg),
    Table:  "queries",
}

// create a new querier
querier := &pgxcache.Querier{
    Options: &pgxcache.QueryOptions{
        MaxLifetime: 30 * time.Second,
        MaxRows:     1,
    },
    Cacher:  cacher,
    Querier: conn,
}

rows, err := querier.Query(context.TODO(), "SELECT * from customer")
```

### S3QueryCacher

Cache query results in S3 using [pgxcache](https://github.com/pgx-contrib/pgxcache):

```go
// Create a new cacher
cacher := &pgxaws.S3QueryCacher{
    Client: s3.NewFromConfig(cfg),
    Bucket: "queries",
}

// create a new querier
querier := &pgxcache.Querier{
    Options: &pgxcache.QueryOptions{
        MaxLifetime: 30 * time.Second,
        MaxRows:     1,
    },
    Cacher:  cacher,
    Querier: conn,
}

rows, err := querier.Query(context.TODO(), "SELECT * from customer")
```

## Contributing

The project uses [Nix](https://nixos.org/) for reproducible development environments and supports [Dev Containers](https://containers.dev/).

**With Nix:**

```bash
nix develop
go tool ginkgo run -r --race
```

**With Dev Containers:**

Open the repository in VS Code or GitHub Codespaces — the devcontainer will configure the environment automatically.

Note: integration tests require real AWS infrastructure (RDS/DSQL/DynamoDB/S3). Unit tests can be run locally without AWS credentials.

## License

[MIT](LICENSE)
