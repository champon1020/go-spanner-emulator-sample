package sample_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/champon1020/go-spanner-emulator-sample/testutil"
)

func TestMain(m *testing.M) {
	if err := testutil.SetupInstance(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup instance: %v", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}
