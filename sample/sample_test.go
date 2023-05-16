package sample_test

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/iterator"

	"github.com/champon1020/go-spanner-emulator-sample/testutil"
)

type User struct {
	ID   int64
	Name string
	Age  int64
}

func TestSample1(t *testing.T) {
	client := testutil.NewTestClient(t, &spanner.ClientConfig{})
	t.Cleanup(func() {
		testutil.DumpDatabases.Add(t.Name())
		client.Close()
	})
	if err := client.CreateDatabase("./schema.sql"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	for name, tt := range map[string]struct {
		stmt      spanner.Statement
		wantUsers []User
	}{
		"sample1-1": {
			stmt: spanner.Statement{SQL: "SELECT ID, Name, Age FROM Users ORDER BY ID"},
			wantUsers: []User{
				{ID: 0, Name: "test_user", Age: 25},
				{ID: 1, Name: "test_user", Age: 26},
			},
		},
		"sample1-2": {
			stmt: spanner.Statement{SQL: "SELECT ID, Name, Age FROM Users WHERE ID = 1"},
			wantUsers: []User{
				{ID: 1, Name: "test_user", Age: 26},
			},
		},
	} {

		tt := tt
		t.Run(name, func(t *testing.T) {
			// ※ datarace の可能性があるため並列化しない!
			// t.Parallel()

			// テストが終わったらテーブルをリセット
			t.Cleanup(func() {
				client.TruncateTables("Users")
			})

			// テストに使用するデータを準備
			ctx := context.Background()
			mu := []*spanner.Mutation{
				spanner.InsertOrUpdate("Users", []string{"ID", "Name", "Age"}, []interface{}{0, "test_user", 25}),
				spanner.InsertOrUpdate("Users", []string{"ID", "Name", "Age"}, []interface{}{1, "test_user", 26}),
			}
			if _, err := client.Apply(ctx, mu); err != nil {
				t.Fatalf("failed to apply mutation: %v", err)
			}

			iter := client.Single().Query(ctx, tt.stmt)
			defer iter.Stop()

			gotUsers := []User{}
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Fatalf("failed to iterate rows: %v", err)
				}
				user := &User{}
				if err := row.Columns(&user.ID, &user.Name, &user.Age); err != nil {
					t.Fatalf("failed to get columns: %v", err)
				}
				gotUsers = append(gotUsers, *user)
			}

			if diff := cmp.Diff(tt.wantUsers, gotUsers); diff != "" {
				t.Fatalf("mismatch: (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSample2(t *testing.T) {
	client := testutil.NewTestClient(t, &spanner.ClientConfig{})
	t.Cleanup(func() {
		testutil.DumpDatabases.Add(t.Name())
		client.Close()
	})
	if err := client.CreateDatabase("./schema.sql"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
}
