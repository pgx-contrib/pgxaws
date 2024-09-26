package pgxaws

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/guregu/dynamo/v2"
	"github.com/pgx-contrib/pgxcache"
)

// DynamoQuery represents a record in the dynamodb table.
type DynamoQuery struct {
	ID       string    `dynamo:"query_id,hash"`
	Data     []byte    `dynamo:"query_data"`
	ExpireAt time.Time `dynamo:"query_expire_at,unixtime"`
}

var _ pgxcache.QueryCacher = &DynamoQueryCacher{}

// DynamoQueryCacher implements pgxcache.QueryCacher interface to use Dynamo DB.
type DynamoQueryCacher struct {
	// Client to interact with Dynamo DB
	Client *dynamodb.Client
	// Table name in Dynamo DB
	Table string
}

// NewDynamoQueryCacher creates a new instance of DynamoQueryCacher.
func NewDynamoQueryCacher(table string) *DynamoQueryCacher {
	conf, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}

	return &DynamoQueryCacher{
		Client: dynamodb.NewFromConfig(conf),
		Table:  table,
	}
}

// Get gets a cache item from Dynamo DB. Returns pointer to the item, a boolean
// which represents whether key exists or not and an error.
func (r *DynamoQueryCacher) Get(ctx context.Context, key *pgxcache.QueryKey) (*pgxcache.QueryResult, error) {
	// get the record
	row := &DynamoQuery{}
	// wrap the client
	client := dynamo.NewFromIface(r.Client)
	// get the item from the table
	err := client.Table(r.Table).Get("query_id", key.String()).One(ctx, row)

	switch err {
	case nil:
		item := &pgxcache.QueryResult{}
		// unmarshal the result
		if err := item.UnmarshalText(row.Data); err != nil {
			return nil, err
		}

		return item, nil
	case dynamo.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

// Set sets the given item into Dynamo DB with provided TTL duration.
func (r *DynamoQueryCacher) Set(ctx context.Context, key *pgxcache.QueryKey, item *pgxcache.QueryResult, ttl time.Duration) error {
	data, err := item.MarshalText()
	if err != nil {
		return err
	}

	// get the record
	row := &DynamoQuery{
		ID:       key.String(),
		Data:     data,
		ExpireAt: time.Now().UTC().Add(ttl),
	}

	// wrap the client
	client := dynamo.NewFromIface(r.Client)
	// put the item in the table
	return client.Table(r.Table).Put(row).Run(ctx)
}

// Reset implements pgxcache.QueryCacher.
func (r *DynamoQueryCacher) Reset(context.Context) error {
	// TODO: implement this method
	return nil
}

var _ pgxcache.QueryCacher = &S3QueryCacher{}

// S3QueryCacher implements pgxcache.QueryCacher interface to use S3.
type S3QueryCacher struct {
	// Client to interact with S3
	Client *s3.Client
	// Bucket name in S3
	Bucket string
}

// Get implements pgxcache.QueryCacher.
func (r *S3QueryCacher) Get(ctx context.Context, key *pgxcache.QueryKey) (*pgxcache.QueryResult, error) {
	// preapre the args
	args := &s3.GetObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(key.String()),
	}

	// put the item in the bucket
	row, err := r.Client.GetObject(ctx, args)
	switch err {
	case nil:
		data, err := io.ReadAll(row.Body)
		if err != nil {
			return nil, err
		}

		item := &pgxcache.QueryResult{}
		// unmarshal the result
		if err := item.UnmarshalText(data); err != nil {
			return nil, err
		}

		return item, nil
	default:
		var nerr *types.NotFound
		// if the error is not found
		if errors.As(err, &nerr) {
			return nil, nil
		}

		var kerr *types.NoSuchKey
		// if the error is not found
		if errors.As(err, &kerr) {
			return nil, nil
		}
		// done
		return nil, err
	}
}

// Set implements pgxcache.QueryCacher.
func (r *S3QueryCacher) Set(ctx context.Context, key *pgxcache.QueryKey, item *pgxcache.QueryResult, ttl time.Duration) error {
	data, err := item.MarshalText()
	if err != nil {
		return err
	}

	// preapre the args
	args := &s3.PutObjectInput{
		Bucket:  aws.String(r.Bucket),
		Key:     aws.String(key.String()),
		Body:    bytes.NewReader(data),
		Expires: aws.Time(time.Now().UTC().Add(ttl)),
	}

	// put the item in the bucket
	_, err = r.Client.PutObject(ctx, args)
	return err
}

// Reset implements pgxcache.QueryCacher.
func (r *S3QueryCacher) Reset(context.Context) error {
	// TODO: implement this method
	return nil
}
