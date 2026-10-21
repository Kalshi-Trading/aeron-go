package conductorfaults

import (
	"os"
	"testing"
)

// TestMain tears the dedicated driver stack down after the suite so suites can run back to back.
func TestMain(m *testing.M) {
	code := m.Run()
	if os.Getenv("AERON_E2E") == "1" && os.Getenv("AERON_ADVERSARIAL") == "1" {
		_ = docker("kill", "-s", "CONT", driverName)
		_ = compose("down", "--remove-orphans")
	}
	os.Exit(code)
}
