package pgxaws

import (
	"context"
	"time"

	"github.com/guregu/dynamo/v2"
	"github.com/guregu/dynamo/v2/dynamodbiface"
	"github.com/pgx-contrib/pgxcache"
	"github.com/vmihailenco/msgpack/v4"
)

// DynamoQuery represents a record in the dynamodb table.
type DynamoQuery struct {
	ID       string    `dynamo:"query_id,hash"`
	Data     []byte    `dynamo:"query_data"`
	ExpireAt time.Time `dynamo:"query_expire_at,unixtime"`
}

var _ pgxcache.QueryCacher = &DynamoQueryCacher{}

// DynamoQueryCacher implements cache.DynamoQueryCacher interface to use redis as backend with
// go-redis as the redis client library.
type DynamoQueryCacher struct {
	client *dynamo.DB
	table  string
}

// NewDynamoQueryCacher creates a new instance of dynamodb backend using dynamodb client.
// All rows created in dynamodb by pgxcache will have stored with table.
func NewDynamoQueryCacher(client dynamodbiface.DynamoDBAPI, table string) *DynamoQueryCacher {
	return &DynamoQueryCacher{
		client: dynamo.NewFromIface(client),
		table:  table,
	}
}

// Get gets a cache item from redis. Returns pointer to the item, a boolean
// which represents whether key exists or not and an error.
func (r *DynamoQueryCacher) Get(ctx context.Context, key *pgxcache.QueryKey) (*pgxcache.QueryResult, error) {
	// get the record
	row := &DynamoQuery{}
	// get the item from the table
	err := r.client.Table(r.table).Get("query_id", key.String()).One(ctx, row)

	switch err {
	case nil:
		var item pgxcache.QueryResult
		// unmarshal the result
		if err := msgpack.Unmarshal(row.Data, &item); err != nil {
			return nil, err
		}
		return &item, nil
	case dynamo.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

// Set sets the given item into redis with provided TTL duration.
func (r *DynamoQueryCacher) Set(ctx context.Context, key *pgxcache.QueryKey, item *pgxcache.QueryResult, ttl time.Duration) error {
	data, err := msgpack.Marshal(item)
	if err != nil {
		return err
	}

	// get the record
	row := &DynamoQuery{
		ID:       key.String(),
		Data:     data,
		ExpireAt: time.Now().UTC().Add(ttl),
	}

	return r.client.Table(r.table).Put(row).Run(ctx)
}
