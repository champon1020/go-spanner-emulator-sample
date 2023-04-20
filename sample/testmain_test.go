package sample_test

import (
	"testing"

	"github.com/champon1020/go-spanner-emulator-sample/testutil"
)

func TestMain(m *testing.M) {
	if err := testutil.SetupInstance(); err != nil {
		panic(err)
	}

	m.Run()

	if err := testutil.DropDatabases(); err != nil {
		panic(err)
	}
}
