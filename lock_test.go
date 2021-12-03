package redlock

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"math/rand"
	"testing"
	"time"
)

func TestLock_LockWithTime(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:         "10.1.3.102:6379",
		Password:     "haxii",
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})
	rand.Seed(time.Now().Unix())
	lock1 := fmt.Sprintf("lock:red-%d", rand.Int())
	lock2 := fmt.Sprintf("lock:red-%d", rand.Int())
	lock3 := fmt.Sprintf("lock:red-%d", rand.Int())
	lock4 := fmt.Sprintf("lock:red-%d", rand.Int())
	lock5 := fmt.Sprintf("lock:red-%d", rand.Int())
	lock6 := fmt.Sprintf("lock:red-%d", rand.Int())
	type args struct {
		ctx    context.Context
		id     string
		minTTL time.Duration
		maxTTL time.Duration

		unlockAfter       time.Duration
		secondUnlockAfter time.Duration
	}
	ctx := context.Background()
	tests := []struct {
		name     string
		args     args
		lockWant bool

		unlockWant       int
		secondUnlockWant int
	}{
		{"case-1", args{ctx: ctx, id: lock1,
			minTTL: 10 * time.Second, maxTTL: 30 * time.Second, unlockAfter: time.Second, secondUnlockAfter: 4 * time.Second},
			true, 1, 1},
		{"case-2", args{ctx: ctx, id: lock2,
			minTTL: 3 * time.Second, maxTTL: 30 * time.Second, unlockAfter: 4 * time.Second, secondUnlockAfter: time.Second},
			true, 0, -1},
		{"case-3", args{ctx: ctx, id: lock3,
			minTTL: 2 * time.Second, maxTTL: 3 * time.Second, unlockAfter: 5 * time.Second, secondUnlockAfter: time.Second},
			true, -1, -1},
		{"case-4", args{ctx: ctx, id: lock4,
			minTTL: 3 * time.Second, maxTTL: time.Second, unlockAfter: 1 * time.Second, secondUnlockAfter: 3 * time.Second},
			true, 1, -1},
		{"case-5", args{ctx: ctx, id: lock5,
			minTTL: 3 * time.Second, maxTTL: time.Second, unlockAfter: 5 * time.Second, secondUnlockAfter: time.Millisecond},
			true, -1, -1},
		{"case-6", args{ctx: ctx, id: lock6,
			minTTL: 0, maxTTL: time.Second, unlockAfter: time.Millisecond, secondUnlockAfter: 10 * time.Millisecond},
			true, 0, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &Lock{
				redis: redisClient,
			}
			got, err := l.LockWithTime(tt.args.ctx, tt.args.id, tt.args.minTTL, tt.args.maxTTL)
			if err != nil {
				t.Errorf("should not have error in 1st lock")
				return
			}
			if got != tt.lockWant {
				t.Errorf("LockWithTime() got = %v, lockWant %v", got, tt.lockWant)
			}
			// try to lock again
			got, err = l.LockWithTime(tt.args.ctx, tt.args.id, tt.args.minTTL, tt.args.maxTTL)
			if err != nil {
				t.Errorf("should not have error in 2nd lock")
			}
			if got {
				t.Errorf("should not hold the lock")
			}
			// sleep in given time
			time.Sleep(tt.args.unlockAfter)
			// try to unlock
			unlockResult, unlockErr := l.UnLock(tt.args.ctx, tt.args.id)
			if unlockErr != nil {
				t.Errorf("should not have error in 2nd lock")
			}
			if tt.unlockWant > 0 {
				if unlockResult <= 0 {
					t.Errorf("UnLock() got = %v, unlockWant %v", unlockResult, tt.unlockWant)
				} else {
					t.Logf("unlock ttl %v", unlockResult)
				}
			} else {
				if unlockResult != tt.unlockWant {
					t.Errorf("UnLock() got = %v, unlockWant %v", unlockResult, tt.unlockWant)
				}
			}
			// sleep again in given time
			time.Sleep(tt.args.secondUnlockAfter)
			// try to unlock again
			unlockResult, unlockErr = l.UnLock(tt.args.ctx, tt.args.id)
			if unlockErr != nil {
				t.Errorf("should not have error in 2nd lock")
			}
			if tt.secondUnlockWant > 0 {
				if unlockResult <= 0 {
					t.Errorf("UnLock() got = %v, unlockWant %v", unlockResult, tt.secondUnlockWant)
				} else {
					t.Logf("unlock ttl %v", unlockResult)
				}
			} else {
				if unlockResult != tt.secondUnlockWant {
					t.Errorf("UnLock() got = %v, unlockWant %v", unlockResult, tt.secondUnlockWant)
				}
			}
		})
	}
}
