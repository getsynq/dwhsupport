package snowflake

import (
	"os"
	"testing"

	"github.com/joho/godotenv"
	gosnowflake "github.com/snowflakedb/gosnowflake"
)

func TestMain(m *testing.M) {
	_ = godotenv.Load("../../.env")
	// Connect failures are a deliberate fixture here, so the driver's own error logging
	// is noise that buries the actual test output.
	_ = gosnowflake.GetLogger().SetLogLevel("fatal")

	os.Exit(m.Run())
}
