package pgxaws

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go/logging"
	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// staticCredentials returns a CredentialsProvider backed by fixed dummy keys.
// This lets authorize() complete its SigV4 presigning without real AWS access.
func staticCredentials() aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
			SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		}, nil
	})
}

// rdsConfig returns a ConnConfig pointing at a fake RDS host.
func rdsConfig() *pgx.ConnConfig {
	cfg := &pgx.ConnConfig{}
	cfg.Host = "mydb.cluster.us-east-1.rds.amazonaws.com"
	cfg.User = "testuser"
	cfg.Port = 5432
	return cfg
}

var _ = Describe("Connector", func() {
	var (
		connector *Connector
		ctx       context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		connector = &Connector{
			config: aws.Config{
				Region:      "us-east-1",
				Credentials: staticCredentials(),
				Logger:      logging.Nop{},
			},
		}
	})

	AfterEach(func() {
		connector.Close()
	})

	// -------------------------------------------------------------------------
	Describe("BeforeConnect guard conditions", func() {
		It("returns nil and leaves password empty when user is not set", func() {
			cfg := rdsConfig()
			cfg.User = ""

			Expect(connector.BeforeConnect(ctx, cfg)).To(Succeed())
			Expect(cfg.Password).To(BeEmpty())
		})

		It("returns nil and leaves password empty when AWS region is not set", func() {
			connector.config.Region = ""
			cfg := rdsConfig()

			Expect(connector.BeforeConnect(ctx, cfg)).To(Succeed())
			Expect(cfg.Password).To(BeEmpty())
		})

		It("returns an error that includes the host name for unsupported hosts", func() {
			cfg := &pgx.ConnConfig{}
			cfg.Host = "db.example.com"
			cfg.User = "testuser"

			err := connector.BeforeConnect(ctx, cfg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("db.example.com"))
		})
	})

	// -------------------------------------------------------------------------
	Describe("authorize", func() {
		DescribeTable("dispatches to the correct authorizer based on host",
			func(host, user string, expectUnsupported bool) {
				cfg := &pgx.ConnConfig{}
				cfg.Host = host
				cfg.User = user
				cfg.Port = 5432

				_, err := connector.authorize(ctx, cfg)
				if expectUnsupported {
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("unsupported"))
				} else {
					Expect(err).NotTo(HaveOccurred())
				}
			},
			Entry("RDS host", "mydb.cluster.us-east-1.rds.amazonaws.com", "testuser", false),
			Entry("DSQL admin user", "abc123.dsql.us-east-1.on.aws", "admin", false),
			Entry("DSQL regular user", "abc123.dsql.us-east-1.on.aws", "app", false),
			Entry("unknown host", "db.example.com", "testuser", true),
		)

		It("includes the host in quotes in the unsupported-host error", func() {
			cfg := &pgx.ConnConfig{}
			cfg.Host = "postgres.internal"
			cfg.User = "testuser"

			_, err := connector.authorize(ctx, cfg)
			Expect(err).To(MatchError(ContainSubstring(`"postgres.internal"`)))
		})
	})

	// -------------------------------------------------------------------------
	Describe("BeforeConnect", func() {
		It("populates the password on the conn config after a successful authorization", func() {
			cfg := rdsConfig()

			Expect(connector.BeforeConnect(ctx, cfg)).To(Succeed())
			Expect(cfg.Password).NotTo(BeEmpty())
		})

		It("marks the connector as initialized and starts the session goroutine", func() {
			Expect(connector.BeforeConnect(ctx, rdsConfig())).To(Succeed())
			Expect(connector.initialized).To(BeTrue())
		})

		It("reuses the cached token on subsequent calls without re-authorizing", func() {
			cfg1 := rdsConfig()
			Expect(connector.BeforeConnect(ctx, cfg1)).To(Succeed())
			first := cfg1.Password

			cfg2 := rdsConfig()
			Expect(connector.BeforeConnect(ctx, cfg2)).To(Succeed())
			Expect(cfg2.Password).To(Equal(first))
		})
	})

	// -------------------------------------------------------------------------
	Describe("Close", func() {
		It("does not panic when called before any BeforeConnect", func() {
			Expect(connector.Close).NotTo(Panic())
		})

		It("can be called multiple times without panic", func() {
			connector.Close()
			Expect(connector.Close).NotTo(Panic())
		})

		It("clears the token and resets the initialized flag", func() {
			Expect(connector.BeforeConnect(ctx, rdsConfig())).To(Succeed())
			Expect(connector.initialized).To(BeTrue())
			Expect(connector.token.Load()).NotTo(BeNil())

			connector.Close()

			Expect(connector.initialized).To(BeFalse())
			Expect(connector.token.Load()).To(BeNil())
		})

		It("allows the connector to be re-initialized after Close", func() {
			Expect(connector.BeforeConnect(ctx, rdsConfig())).To(Succeed())
			connector.Close()

			cfg := rdsConfig()
			Expect(connector.BeforeConnect(ctx, cfg)).To(Succeed())
			Expect(cfg.Password).NotTo(BeEmpty())
			Expect(connector.initialized).To(BeTrue())
		})
	})

	// -------------------------------------------------------------------------
	Describe("concurrent BeforeConnect", func() {
		It("handles concurrent calls safely and spawns exactly one session goroutine", func() {
			const goroutines = 20

			var (
				wg   sync.WaitGroup
				mu   sync.Mutex
				errs []error
			)

			wg.Add(goroutines)
			for i := 0; i < goroutines; i++ {
				go func() {
					defer wg.Done()
					err := connector.BeforeConnect(ctx, rdsConfig())
					if err != nil {
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			Expect(errs).To(BeEmpty())
			Expect(connector.initialized).To(BeTrue())
		})
	})
})
