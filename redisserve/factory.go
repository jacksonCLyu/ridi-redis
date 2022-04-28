package redisserve

import (
	"github.com/go-redis/redis/v8"
	"github.com/jacksonCLyu/ridi-utils/utils/errcheck"
)

// GetClient returns a redis client
func GetClient(serviceName string) redis.Cmdable {
	obj, _ := cmdableMap.LoadOrStore(serviceName, InitPool(serviceName))
	return obj.(redis.Cmdable)
}

// ReleaseClient release a redis client
func ReleaseClient(serviceName string, cmdable redis.Cmdable) {
	if cmdable == nil {
		return
	}
	if c, ok := cmdable.(*redis.Conn); ok {
		err := c.Close()
		errcheck.CheckAndPrintf(err, "redis conn close error: %v", err)
	}
}
