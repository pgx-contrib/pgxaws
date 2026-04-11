package pgxaws

import (
	"context"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxcache"
)

var _ = Describe("DynamoQueryCacher", func() {
	// -------------------------------------------------------------------------
	Describe("NewDynamoQueryCacher", func() {
		It("creates a cacher with the correct table name and a non-nil client", func() {
			cacher, err := NewDynamoQueryCacher(context.Background(), "my-table")
			Expect(err).NotTo(HaveOccurred())
			Expect(cacher.Table).To(Equal("my-table"))
			Expect(cacher.Client).NotTo(BeNil())
		})
	})

	// -------------------------------------------------------------------------
	Describe("Integration", Ordered, func() {
		var (
			cacher *DynamoQueryCacher
			ctx    context.Context
			key    *pgxcache.QueryKey
		)

		BeforeAll(func() {
			table := os.Getenv("PGXAWS_DYNAMODB_TABLE")
			if table == "" {
				Skip("PGXAWS_DYNAMODB_TABLE not set")
			}

			var err error
			cacher, err = NewDynamoQueryCacher(context.Background(), table)
			Expect(err).NotTo(HaveOccurred())

			ctx = context.Background()
			key = &pgxcache.QueryKey{SQL: "SELECT 1"}

			// Start each integration run with a clean table.
			Expect(cacher.Reset(ctx)).To(Succeed())
		})

		It("returns nil for a missing key", func() {
			item, err := cacher.Get(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(item).To(BeNil())
		})

		It("round-trips an item through Set and Get", func() {
			item := &pgxcache.QueryItem{CommandTag: "SELECT"}
			Expect(cacher.Set(ctx, key, item, time.Minute)).To(Succeed())

			got, err := cacher.Get(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).NotTo(BeNil())
			Expect(got.CommandTag).To(Equal("SELECT"))
		})

		It("Reset removes all items so a subsequent Get returns nil", func() {
			Expect(cacher.Reset(ctx)).To(Succeed())

			got, err := cacher.Get(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(BeNil())
		})
	})
})

var _ = Describe("S3QueryCacher", func() {
	// -------------------------------------------------------------------------
	Describe("NewS3QueryCacher", func() {
		It("creates a cacher with the correct bucket name and a non-nil client", func() {
			cacher, err := NewS3QueryCacher(context.Background(), "my-bucket")
			Expect(err).NotTo(HaveOccurred())
			Expect(cacher.Bucket).To(Equal("my-bucket"))
			Expect(cacher.Client).NotTo(BeNil())
		})
	})

	// -------------------------------------------------------------------------
	Describe("Integration", Ordered, func() {
		var (
			cacher *S3QueryCacher
			ctx    context.Context
			key    *pgxcache.QueryKey
		)

		BeforeAll(func() {
			bucket := os.Getenv("PGXAWS_S3_BUCKET")
			if bucket == "" {
				Skip("PGXAWS_S3_BUCKET not set")
			}

			var err error
			cacher, err = NewS3QueryCacher(context.Background(), bucket)
			Expect(err).NotTo(HaveOccurred())

			ctx = context.Background()
			key = &pgxcache.QueryKey{SQL: "SELECT 1"}

			// Start each integration run with a clean bucket.
			Expect(cacher.Reset(ctx)).To(Succeed())
		})

		It("returns nil for a missing key", func() {
			item, err := cacher.Get(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(item).To(BeNil())
		})

		It("round-trips an item through Set and Get", func() {
			item := &pgxcache.QueryItem{CommandTag: "SELECT"}
			Expect(cacher.Set(ctx, key, item, time.Minute)).To(Succeed())

			got, err := cacher.Get(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).NotTo(BeNil())
			Expect(got.CommandTag).To(Equal("SELECT"))
		})

		It("Get returns nil when the item TTL has already elapsed", func() {
			expiredKey := &pgxcache.QueryKey{SQL: "SELECT 'expired'"}
			item := &pgxcache.QueryItem{CommandTag: "SELECT"}
			// A negative TTL puts the expiry timestamp in the past.
			Expect(cacher.Set(ctx, expiredKey, item, -time.Second)).To(Succeed())

			got, err := cacher.Get(ctx, expiredKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(BeNil())
		})

		It("Reset deletes all objects so a subsequent Get returns nil", func() {
			Expect(cacher.Reset(ctx)).To(Succeed())

			got, err := cacher.Get(ctx, key)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(BeNil())
		})
	})
})
