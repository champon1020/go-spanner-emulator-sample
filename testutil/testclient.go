package testutil

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	databaseadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type Client struct {
	*spanner.Client

	databaseName   string
	databaseClient *databaseadmin.DatabaseAdminClient
}

func NewTestClient(tb testing.TB, spannerConfig *spanner.ClientConfig) *Client {
	tb.Helper()

	databaseName := getDatabaseName(tb.Name())

	ctx := context.Background()
	client, err := spanner.NewClientWithConfig(ctx, fullDatabaseName(databaseName), *spannerConfig)
	if err != nil {
		tb.Fatalf("failed to create spanner client: %v", err)
	}

	dbClient, err := databaseadmin.NewDatabaseAdminClient(ctx)
	if err != nil {
		tb.Fatalf("failed to create database admin clinet: %v", err)
	}

	return &Client{
		Client:         client,
		databaseName:   databaseName,
		databaseClient: dbClient,
	}
}

func (c *Client) CreateDatabase(schemaPath string) error {
	statements, err := c.parseSchemaToStatements(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to parse schema file: %w", err)
	}

	// os.Exit(1)

	ctx := context.Background()
	op, err := c.databaseClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", testProjectName, testInstanceName),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", c.databaseName),
		ExtraStatements: statements,
	})
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait operation: %w", err)
	}

	return nil
}

func (c *Client) parseSchemaToStatements(schemaPath string) ([]string, error) {
	f, err := os.Open(schemaPath)
	if err != nil {
		return []string{}, fmt.Errorf("failed to open schema file: %w", err)
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return []string{}, fmt.Errorf("failed to read schema file: %w", err)
	}

	contents := strings.Split(string(data), ";")
	statements := make([]string, 0, len(contents))
	for _, cont := range contents {
		cont = strings.TrimSpace(cont)
		if cont == "" {
			continue
		}
		statements = append(statements, cont)
	}

	return statements, nil
}

func (c *Client) TruncateTables(tables ...string) error {
	mu := make([]*spanner.Mutation, len(tables))
	for i, table := range tables {
		mu[i] = spanner.Delete(table, spanner.AllKeys())
	}

	ctx := context.Background()
	if _, err := c.Client.Apply(ctx, mu, spanner.ApplyAtLeastOnce()); err != nil {
		return fmt.Errorf("failed to apply: %w", err)
	}

	return nil
}
