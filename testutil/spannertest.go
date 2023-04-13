package testutil

import (
	"context"
	"fmt"
	"hash/maphash"
	"os"
	"sync"

	databaseadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
	instanceadminpb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	testProjectName  = "testing-project"
	testInstanceName = "testing-instance"

	// For making a database name unique
	seed     = maphash.MakeSeed()
	hashPool = sync.Pool{
		New: func() interface{} {
			var h maphash.Hash
			h.SetSeed(seed)
			return &h
		},
	}
)

func getDatabaseName(hashOrigin string) string {
	h := hashPool.Get().(*maphash.Hash)
	h.WriteString(hashOrigin)
	databaseName := fmt.Sprintf("db_%x", h.Sum64())
	h.Reset()
	hashPool.Put(h)
	return databaseName
}

func fullDatabaseName(databaseName string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectName, testInstanceName, databaseName)
}

func SetupInstance() error {
	if v := os.Getenv("SPANNER_EMULATOR_HOST"); v == "" {
		return fmt.Errorf("EnvSpannerEmulatorHost is not set")
	}

	ctx := context.Background()
	client, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create instance client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateInstance(ctx, &instanceadminpb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", testProjectName),
		InstanceId: testInstanceName,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("failed to create instance operation: %w", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait operation done: %w", err)
	}

	return nil
}

type dumpDatabases struct {
	sync.Mutex
	databases map[string]struct{}
}

func newDumpDatabases() *dumpDatabases {
	return &dumpDatabases{
		databases: make(map[string]struct{}),
	}
}

func DropDatabases() error {
	ctx := context.Background()
	client, err := databaseadmin.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create database admin client: %w", err)
	}
	defer client.Close()

	iterator := client.ListDatabases(ctx, &databaseadminpb.ListDatabasesRequest{
		Parent: fmt.Sprintf("projects/%s/instances/%s", testProjectName, testInstanceName),
	})

	// The limit of the number of databases is 100.
	// ref: https://cloud.google.com/spanner/quotas#database-limits
	databases := make([]*databaseadminpb.Database, 0, 100)
	for {
		database, err := iterator.Next()
		if err == iterator.Done() {
			break
		}
		if err != nil {
			return fmt.Errorf("iterator error: %w", err)
		}
	}
}
