package redisserve

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jacksonCLyu/ridi-config/pkg/config"
	"github.com/jacksonCLyu/ridi-log/log"
	"github.com/jacksonCLyu/ridi-utils/utils/assignutil"
	"github.com/jacksonCLyu/ridi-utils/utils/errcheck"
	"github.com/jacksonCLyu/ridi-utils/utils/rescueutil"
)

var cmdableMap sync.Map

// InitPool initializes the redis pool
func InitPool(serviceName string) redis.Cmdable {
	defer rescueutil.Recover(func(err any) {
		log.Errorf("InitPool error: %v", err)
	})
	hostStrKey := strings.Join([]string{serviceName, "redis.hostStr"}, ".")
	if !config.ContainsKey(hostStrKey) {
		panic("redis host not found")
	}
	hostStr := assignutil.Assign(config.GetString(hostStrKey))
	var cmdable redis.Cmdable
	var msg string
	dialTimeout := assignutil.Assign(config.GetInt64(strings.Join([]string{serviceName, "redis.dialTimeout"}, ".")))
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(dialTimeout))
	defer cancel()
	if strings.Contains(hostStr, ",") {
		// cluster config
		cmdable = redis.NewClusterClient(GetClusterOptionsConfig(serviceName))
	} else {
		// single client config
		cmdable = redis.NewClient(GetOptionsConfig(serviceName))
	}
	state := cmdable.Ping(ctx)
	msg = assignutil.Assign(state.Result())
	log.Debugf("client pool assign msg: {}", msg)
	return cmdable
}

// DestoryPool destory the redis pool
func DestoryPool(pool redis.Cmdable) {
	switch value := pool.(type) {
	case *redis.Client:
		errcheck.CheckAndPanic(value.Close())
	case *redis.ClusterClient:
		errcheck.CheckAndPanic(value.Close())
	default:
		errcheck.CheckAndPanic(value.Pipeline().Close())
	}
}

// GetOptionsConfig returns the redis options
func GetOptionsConfig(serviceName string) *redis.Options {
	hostStrKey := strings.Join([]string{serviceName, "redis.hostStr"}, ".")
	hostStr := assignutil.Assign(config.GetString(hostStrKey))
	subConfig := assignutil.Assign(config.GetSection(serviceName))
	hosts := strings.Split(hostStr, ":")
	if len(hosts) < 2 {
		panic("invalid redis host")
	}
	ip := hosts[0]
	pIndex := strings.LastIndex(ip, ".") + 1
	kStr := ip[pIndex:]
	password, _ := subConfig.GetString(strings.Join([]string{"redis.password", "redis" + kStr}, "."))
	var maxRetries int
	if subConfig.ContainsKey("redis.maxRetries") {
		maxRetries = assignutil.Assign(subConfig.GetInt("redis.maxRetries"))
	} else {
		maxRetries = 3
	}
	var dialTo int64
	if subConfig.ContainsKey("redis.dialTimeout") {
		dialTo = assignutil.Assign(subConfig.GetInt64("redis.dialTimeout"))
	} else {
		dialTo = 1000
	}
	dialTimeout := time.Duration(dialTo) * time.Millisecond
	var readTo int64
	if subConfig.ContainsKey("redis.readTimeout") {
		readTo = assignutil.Assign(subConfig.GetInt64("redis.readTimeout"))
	} else {
		readTo = 3000
	}
	readTimeout := time.Duration(readTo) * time.Millisecond
	var writeTo int64
	if subConfig.ContainsKey("redis.writeTimeout") {
		writeTo = assignutil.Assign(subConfig.GetInt64("redis.writeTimeout"))
	} else {
		writeTo = 30000
	}
	writeTimeout := time.Duration(writeTo) * time.Millisecond
	var poolSize int
	if subConfig.ContainsKey("redis.poolSize") {
		poolSize = assignutil.Assign(subConfig.GetInt("redis.poolSize"))
	} else {
		poolSize = 10 * runtime.NumCPU()
	}
	return &redis.Options{
		Addr:         serviceName,
		Password:     password,
		MaxRetries:   maxRetries,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	}
}

// GetClusterOptionsConfig returns the redis cluster options
func GetClusterOptionsConfig(serviceName string) *redis.ClusterOptions {
	hostStrKey := strings.Join([]string{serviceName, "redis.hostStr"}, ".")
	hostStr := assignutil.Assign(config.GetString(hostStrKey))
	hosts := strings.Split(hostStr, ",")
	subConfig := assignutil.Assign(config.GetSection(serviceName))
	password, _ := subConfig.GetString("redis.password")
	var maxRetries int
	if subConfig.ContainsKey("redis.maxRetries") {
		maxRetries = assignutil.Assign(subConfig.GetInt("redis.maxRetries"))
	} else {
		maxRetries = 3
	}
	var dialTo int64
	if subConfig.ContainsKey("redis.dialTimeout") {
		dialTo = assignutil.Assign(subConfig.GetInt64("redis.dialTimeout"))
	} else {
		dialTo = 1000
	}
	dialTimeout := time.Duration(dialTo) * time.Millisecond
	var readTo int64
	if subConfig.ContainsKey("redis.readTimeout") {
		readTo = assignutil.Assign(subConfig.GetInt64("redis.readTimeout"))
	} else {
		readTo = 3000
	}
	readTimeout := time.Duration(readTo) * time.Millisecond
	var writeTo int64
	if subConfig.ContainsKey("redis.writeTimeout") {
		writeTo = assignutil.Assign(subConfig.GetInt64("redis.writeTimeout"))
	} else {
		writeTo = 30000
	}
	writeTimeout := time.Duration(writeTo) * time.Millisecond
	var poolSize int
	if subConfig.ContainsKey("redis.poolSize") {
		poolSize = assignutil.Assign(subConfig.GetInt("redis.poolSize"))
	} else {
		poolSize = 10 * runtime.NumCPU()
	}

	return &redis.ClusterOptions{
		Addrs:        hosts,
		Password:     password,
		MaxRetries:   maxRetries,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
	}
}
