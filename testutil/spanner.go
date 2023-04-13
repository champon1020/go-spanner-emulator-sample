package testutil

import (
	"context"
	"fmt"
	"os"

	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
	instanceadminpb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	testProjectName  = "testing-project"
	testInstanceName = "testing-instance"
)

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
		return fmt.Errorf("failed to create instance: %w", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait operation: %w", err)
	}

	return nil
}
