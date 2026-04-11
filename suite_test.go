package pgxaws_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgxaws(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxaws Suite")
}
