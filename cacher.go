package pgxaws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/guregu/dynamo/v2"
	"github.com/pgx-contrib/pgxcache"
)

// DynamoQuery represents a record in the DynamoDB cache table.
type DynamoQuery struct {
	ID       string    `dynamo:"query_id,hash"`
	Data     []byte    `dynamo:"query_data"`
	ExpireAt time.Time `dynamo:"query_expire_at,unixtime"`
}

var _ pgxcache.QueryCacher = &DynamoQueryCacher{}

// DynamoQueryCacher implements pgxcache.QueryCacher interface to use DynamoDB.
type DynamoQueryCacher struct {
	// Client to interact with DynamoDB.
	Client *dynamodb.Client
	// Table name in DynamoDB.
	Table string
}

// NewDynamoQueryCacher creates a new DynamoQueryCacher using the default AWS configuration.
func NewDynamoQueryCacher(ctx context.Context, table string) (*DynamoQueryCacher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &DynamoQueryCacher{
		Client: dynamodb.NewFromConfig(cfg),
		Table:  table,
	}, nil
}

// Get retrieves a cache item from DynamoDB.
func (r *DynamoQueryCacher) Get(ctx context.Context, key *pgxcache.QueryKey) (*pgxcache.QueryItem, error) {
	row := &DynamoQuery{}
	client := dynamo.NewFromIface(r.Client)

	err := client.Table(r.Table).Get("query_id", key.String()).One(ctx, row)
	switch err {
	case nil:
		item := &pgxcache.QueryItem{}
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

// Set stores a cache item in DynamoDB with the provided TTL.
// The table must have TTL enabled on the query_expire_at attribute for
// automatic item expiration.
func (r *DynamoQueryCacher) Set(ctx context.Context, key *pgxcache.QueryKey, item *pgxcache.QueryItem, lifetime time.Duration) error {
	data, err := item.MarshalText()
	if err != nil {
		return err
	}

	row := &DynamoQuery{
		ID:       key.String(),
		Data:     data,
		ExpireAt: time.Now().UTC().Add(lifetime),
	}

	client := dynamo.NewFromIface(r.Client)
	return client.Table(r.Table).Put(row).Run(ctx)
}

// Reset deletes all items from the DynamoDB cache table.
func (r *DynamoQueryCacher) Reset(ctx context.Context) error {
	// Project only the hash key — we only need keys to issue deletes.
	// Using an expression attribute name avoids conflicts with DynamoDB reserved words.
	paginator := dynamodb.NewScanPaginator(r.Client, &dynamodb.ScanInput{
		TableName:                aws.String(r.Table),
		ProjectionExpression:     aws.String("#qid"),
		ExpressionAttributeNames: map[string]string{"#qid": "query_id"},
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}

		// BatchWriteItem accepts at most 25 requests per call.
		for i := 0; i < len(page.Items); i += 25 {
			end := min(i+25, len(page.Items))
			deletes := make([]dynamodbtypes.WriteRequest, end-i)
			for j, item := range page.Items[i:end] {
				deletes[j] = dynamodbtypes.WriteRequest{
					DeleteRequest: &dynamodbtypes.DeleteRequest{Key: item},
				}
			}

			// Retry any unprocessed items returned by DynamoDB.
			input := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodbtypes.WriteRequest{r.Table: deletes},
			}
			for len(input.RequestItems) > 0 {
				out, err := r.Client.BatchWriteItem(ctx, input)
				if err != nil {
					return err
				}
				input.RequestItems = out.UnprocessedItems
			}
		}
	}

	return nil
}

// metaKeyExpiresAt is the S3 user-defined metadata key that stores the
// cache item expiration time as an RFC 3339 timestamp.
// S3 normalises user-defined metadata keys to lowercase on write.
const metaKeyExpiresAt = "expires-at"

var _ pgxcache.QueryCacher = &S3QueryCacher{}

// S3QueryCacher implements pgxcache.QueryCacher interface to use S3.
type S3QueryCacher struct {
	// Client to interact with S3.
	Client *s3.Client
	// Bucket name in S3.
	Bucket string
}

// NewS3QueryCacher creates a new S3QueryCacher using the default AWS configuration.
func NewS3QueryCacher(ctx context.Context, bucket string) (*S3QueryCacher, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &S3QueryCacher{
		Client: s3.NewFromConfig(cfg),
		Bucket: bucket,
	}, nil
}

// Get retrieves a cache item from S3.
func (r *S3QueryCacher) Get(ctx context.Context, key *pgxcache.QueryKey) (*pgxcache.QueryItem, error) {
	row, err := r.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(key.String()),
	})
	if err != nil {
		var nerr *s3types.NotFound
		if errors.As(err, &nerr) {
			return nil, nil
		}
		var kerr *s3types.NoSuchKey
		if errors.As(err, &kerr) {
			return nil, nil
		}
		return nil, err
	}
	defer row.Body.Close()

	// Check the client-side TTL stored in object metadata.
	if v, ok := row.Metadata[metaKeyExpiresAt]; ok {
		if t, err := time.Parse(time.RFC3339, v); err == nil && time.Now().UTC().After(t) {
			return nil, nil
		}
	}

	data, err := io.ReadAll(row.Body)
	if err != nil {
		return nil, err
	}

	item := &pgxcache.QueryItem{}
	if err := item.UnmarshalText(data); err != nil {
		return nil, err
	}
	return item, nil
}

// Set stores a cache item in S3. The expiration time is recorded in object
// metadata (expires-at) and enforced client-side by Get. Objects are not
// automatically deleted by S3 unless a matching lifecycle rule is configured
// on the bucket.
func (r *S3QueryCacher) Set(ctx context.Context, key *pgxcache.QueryKey, item *pgxcache.QueryItem, ttl time.Duration) error {
	data, err := item.MarshalText()
	if err != nil {
		return err
	}

	_, err = r.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(key.String()),
		Body:   bytes.NewReader(data),
		Metadata: map[string]string{
			metaKeyExpiresAt: time.Now().UTC().Add(ttl).Format(time.RFC3339),
		},
	})
	return err
}

// Reset deletes all objects from the S3 cache bucket.
func (r *S3QueryCacher) Reset(ctx context.Context) error {
	paginator := s3.NewListObjectsV2Paginator(r.Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.Bucket),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		if len(page.Contents) == 0 {
			continue
		}

		// ListObjectsV2 returns at most 1000 objects per page, which is also
		// the DeleteObjects limit, so each page maps to exactly one API call.
		objects := make([]s3types.ObjectIdentifier, len(page.Contents))
		for i, obj := range page.Contents {
			objects[i] = s3types.ObjectIdentifier{Key: obj.Key}
		}

		out, err := r.Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(r.Bucket),
			Delete: &s3types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return err
		}
		if len(out.Errors) > 0 {
			first := out.Errors[0]
			return fmt.Errorf("s3: delete %s: %s", aws.ToString(first.Key), aws.ToString(first.Message))
		}
	}

	return nil
}
