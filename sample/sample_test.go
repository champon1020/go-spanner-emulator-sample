package sample_test

import (
	"context"
	"log"
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
		if err := client.DropDatabase(); err != nil {
			log.Fatal(err)
		}
		client.Close()
	})
	if err := client.CreateDatabase("./schemas/users.sql"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	for name, tt := range map[string]struct {
		userData  []User
		stmt      spanner.Statement
		wantUsers []User
	}{
		"sample1-1": {
			userData: []User{
				{ID: 0, Name: "Taro", Age: 25},
				{ID: 1, Name: "Jiro", Age: 41},
				{ID: 2, Name: "Hanako", Age: 28},
			},
			stmt: spanner.Statement{SQL: "SELECT ID, Name, Age FROM Users ORDER BY ID"},
			wantUsers: []User{
				{ID: 0, Name: "Taro", Age: 25},
				{ID: 1, Name: "Jiro", Age: 41},
				{ID: 2, Name: "Hanako", Age: 28},
			},
		},
		"sample1-2": {
			userData: []User{
				{ID: 0, Name: "Taro", Age: 25},
				{ID: 1, Name: "Jiro", Age: 41},
				{ID: 2, Name: "Hanako", Age: 28},
			},
			stmt: spanner.Statement{SQL: "SELECT ID, Name, Age FROM Users WHERE Age > 40"},
			wantUsers: []User{
				{ID: 1, Name: "Jiro", Age: 41},
			},
		},
	} {

		tt := tt
		t.Run(name, func(t *testing.T) {
			// ※ データ競合の可能性があるため並列化しない!
			// t.Parallel()

			// テストが終わったらテーブルをリセット
			t.Cleanup(func() {
				client.TruncateTables("Users")
			})

			// テストに使用するデータを準備
			ctx := context.Background()
			mu := []*spanner.Mutation{}
			for _, data := range tt.userData {
				mu = append(mu, spanner.InsertOrUpdate("Users", []string{"ID", "Name", "Age"}, []interface{}{data.ID, data.Name, data.Age}))
			}
			if _, err := client.Apply(ctx, mu); err != nil {
				t.Fatalf("failed to apply mutation: %v", err)
			}

			// クエリを実行
			iter := client.Single().Query(ctx, tt.stmt)
			defer iter.Stop()

			// クエリの結果を取得
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

			// 期待する結果が得られているか確認
			if diff := cmp.Diff(tt.wantUsers, gotUsers); diff != "" {
				t.Fatalf("mismatch: (-want +got):\n%s", diff)
			}
		})
	}
}

type Item struct {
	ID       int64
	Name     string
	Category string
}

func TestSample2(t *testing.T) {
	client := testutil.NewTestClient(t, &spanner.ClientConfig{})
	t.Cleanup(func() {
		if err := client.DropDatabase(); err != nil {
			log.Fatal(err)
		}
		client.Close()
	})
	if err := client.CreateDatabase("./schemas/items.sql"); err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	for name, tt := range map[string]struct {
		itemData  []Item
		stmt      spanner.Statement
		wantItems []Item
	}{
		"sample2-1": {
			itemData: []Item{
				{ID: 0, Name: "Orange", Category: "Fruit"},
				{ID: 1, Name: "Grape", Category: "Fruit"},
				{ID: 2, Name: "Chair", Category: "Furniture"},
			},
			stmt: spanner.Statement{SQL: "SELECT ID, Name, Category FROM Items ORDER BY ID"},
			wantItems: []Item{
				{ID: 0, Name: "Orange", Category: "Fruit"},
				{ID: 1, Name: "Grape", Category: "Fruit"},
				{ID: 2, Name: "Chair", Category: "Furniture"},
			},
		},
		"sample2-2": {
			itemData: []Item{
				{ID: 0, Name: "Orange", Category: "Fruit"},
				{ID: 1, Name: "Grape", Category: "Fruit"},
				{ID: 2, Name: "Chair", Category: "Furniture"},
			},
			stmt: spanner.Statement{SQL: "SELECT ID, Name, Category FROM Items WHERE Category = 'Fruit' ORDER BY ID"},
			wantItems: []Item{
				{ID: 0, Name: "Orange", Category: "Fruit"},
				{ID: 1, Name: "Grape", Category: "Fruit"},
			},
		},
	} {
		tt := tt
		t.Run(name, func(t *testing.T) {
			// ※ データ競合の可能性があるため並列化しない!
			// t.Parallel()

			// テストが終わったらテーブルをリセット
			t.Cleanup(func() {
				client.TruncateTables("Users")
			})

			// テストに使用するデータを準備
			ctx := context.Background()
			mu := []*spanner.Mutation{}
			for _, data := range tt.itemData {
				mu = append(mu, spanner.InsertOrUpdate("Items", []string{"ID", "Name", "Category"}, []interface{}{data.ID, data.Name, data.Category}))
			}
			if _, err := client.Apply(ctx, mu); err != nil {
				t.Fatalf("failed to apply mutation: %v", err)
			}

			// クエリを実行
			iter := client.Single().Query(ctx, tt.stmt)
			defer iter.Stop()

			// クエリの結果を取得
			gotItems := []Item{}
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Fatalf("failed to iterate rows: %v", err)
				}
				item := &Item{}
				if err := row.Columns(&item.ID, &item.Name, &item.Category); err != nil {
					t.Fatalf("failed to get columns: %v", err)
				}
				gotItems = append(gotItems, *item)
			}

			// 期待する結果が得られているか確認
			if diff := cmp.Diff(tt.wantItems, gotItems); diff != "" {
				t.Fatalf("mismatch: (-want +got):\n%s", diff)
			}
		})
	}
}
