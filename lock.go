package redlock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Lock struct {
	redis *redis.Client
}

func NewLock(redis *redis.Client) *Lock {
	return &Lock{redis: redis}
}

var ErrMaxTTLShouldBeSet = errors.New("max ttl in lock should be set")

// LockWithTime 给指定 ID 加分布式锁, 此 ID 在指定 minTTL 时间内无论是否解锁均不会被再次加锁
func (l *Lock) LockWithTime(ctx context.Context, id string, minTTL, maxTTL time.Duration) (bool, error) {
	if maxTTL <= 0 {
		return false, ErrMaxTTLShouldBeSet
	}
	if minTTL < 0 {
		minTTL = 0
	}
	if minTTL > maxTTL {
		maxTTL = minTTL
	}
	// 此锁不应该在该时间戳前过期, 锁的 value 即是此过期时间
	lockShouldNotExpBefore := fmt.Sprintf("%d", time.Now().Add(minTTL).Unix())
	return l.redis.SetNX(ctx, id, lockShouldNotExpBefore, maxTTL).Result()
}

// UnLock 解锁给定 ID
//  如果锁不存在 返回 -1
//  如果存在但已过最小锁定期解锁后返回 0
//  如果存在但未过最小锁定期解锁后返回 对应的存活时间 以秒计
func (l *Lock) UnLock(ctx context.Context, id string) (int, error) {
	return luaReleaseLock.Run(ctx, l.redis, []string{id}, time.Now().Unix()).Int()
}

// luaReleaseLock Redis lua script 解锁指定 key
var luaReleaseLock = redis.NewScript(`
local exp_time = redis.call("get",KEYS[1])
if exp_time then
 local ttl = exp_time - ARGV[1]
 if ttl > 0 then
  redis.call("expire", KEYS[1], ttl)
  return ttl
 else
  redis.call("del", KEYS[1])
  return 0
 end
else
 return -1
end`)
