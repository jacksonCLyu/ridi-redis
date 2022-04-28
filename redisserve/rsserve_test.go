package redisserve

import (
	"context"
	"testing"

	"github.com/jacksonCLyu/ridi-utils/utils/assignutil"
	"github.com/jacksonCLyu/ridi-utils/utils/rescueutil"
)

func TestGetClient(t *testing.T) {
	defer rescueutil.Recover(func(err any) {
		t.Errorf("TestGetClient error: %v", err)
	})
	// InitPool("test")
	cmdable := GetClient("test")
	defer ReleaseClient("test", cmdable)
	strstrCmd := cmdable.HGetAll(context.TODO(), "test")
	strstr := assignutil.Assign(strstrCmd.Result())
	t.Log(strstr)
}
