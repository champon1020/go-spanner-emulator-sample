package main

import (
	"testing"

	"cloud.google.com/go/spanner"

	testutil "github.com/champon1020/go-spanner-emulator-sample/testutil"
)

func TestMain(m *testing.M) {
	if err := testutil.SetupInstance(); err != nil {
		panic(err)
	}

	m.Run()
}

func TestSample(t *testing.T) {
	client := testutil.NewTestClient(t, &spanner.ClientConfig{})
	if err := client.CreateDatabase("./schema.sql"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
}
