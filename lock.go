// Copyright 2023 xuqil
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zk_lock

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/go-zookeeper/zk"
)

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = errors.New("zk_lock: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first been acquired.
	ErrNotLocked = errors.New("zk_lock: not locked")

	errDifferentKey = errors.New("zk_lock: different key")
)

var (
	defaultRetries  = 3
	defaultBasePath = "/lock"
)

type lockOption func(l *Lock)

func LockWithRetry(retries int) func(l *Lock) {
	return func(l *Lock) {
		l.retries = retries
	}
}

func LockWithBasePath(basePath string) func(l *Lock) {
	return func(l *Lock) {
		if !strings.HasPrefix(basePath, "/") {
			basePath = "/" + basePath
		}
		l.basePath = basePath
	}
}

// Lock is a mutual exclusion lock.
type Lock struct {
	c        *zk.Conn
	basePath string
	key      string
	lockPath string
	retries  int
	acl      []zk.ACL
}

// NewLock creates a new lock instance using the provided connection, key, and acl.
// A lock instances starts unlocked until Lock() is called.
func NewLock(c *zk.Conn, key string, acl []zk.ACL, opts ...lockOption) *Lock {
	lock := &Lock{
		c:        c,
		basePath: defaultBasePath,
		retries:  defaultRetries,
		key:      key + "-",
		acl:      acl,
	}
	for _, opt := range opts {
		opt(lock)
	}
	return lock
}

// Lock attempts to acquire the lock. It works like lockWithData, but it doesn't
// write any data to the lock node.
func (l *Lock) Lock(ctx context.Context) error {
	return l.lockWithData(ctx, nil)
}

// lockWithData attempts to acquire the lock with context.Context, writing data into the lock node.
// It will wait to return until the lock is acquired or an error occurs. If
// this instance already has the lock then ErrDeadlock is returned.
func (l *Lock) lockWithData(ctx context.Context, data []byte) error {
	if l.lockPath != "" {
		return ErrDeadlock
	}

	lockPath := ""
	var err error
	// try to create children node.
	for i := 0; i < l.retries; i++ {
		lockPath, err = l.c.CreateProtectedEphemeralSequential(l.basePath+"/"+l.key, data, l.acl)
		if errors.Is(err, zk.ErrNoNode) {
			if er := l.createParent(); er != nil {
				return er
			}
		} else if err != nil {
			return err
		} else {
			break
		}
	}

	seq, err := l.parseSeq(lockPath)
	if err != nil {
		return err
	}

	for {
		lowestSeq := seq
		prevSeq := -1
		prevPath := ""

		children, _, err := l.c.Children(l.basePath)
		if err != nil {
			return err
		}
		for _, child := range children {
			s, err := l.parseSeq(child)
			if err != nil {
				if errors.Is(err, errDifferentKey) {
					continue
				}
				return err
			}
			if s < lowestSeq {
				lowestSeq = s
			}
			if s < seq && s > prevSeq {
				prevSeq = s
				prevPath = child
			}
		}

		// Acquired the lock.
		if seq == lowestSeq {
			break
		}

		// Wait on the node next in line for the lock.
		_, _, events, err := l.c.GetW(l.basePath + "/" + prevPath)
		if err != nil {
			// try again.
			if errors.Is(err, zk.ErrNoNode) {
				continue
			}
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-events:
			if event.Err != nil {
				return event.Err
			}
		}
	}
	l.lockPath = lockPath
	return nil
}

func (l *Lock) parseSeq(path string) (int, error) {
	parts := strings.Split(path, l.key)
	if len(parts) != 2 || strings.HasPrefix(parts[1], "-") {
		return 0, errDifferentKey
	}
	return strconv.Atoi(parts[len(parts)-1])
}

// createParent create the parent node and returns error if failed.
func (l *Lock) createParent() error {
	parts := strings.Split(l.basePath, "/")
	path := ""
	for _, part := range parts[1:] {
		path += "/" + part
		exists, _, err := l.c.Exists(path)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		_, err = l.c.Create(path, nil, 0, l.acl)
		if err != nil && errors.Is(err, zk.ErrNodeExists) {
			return err
		}
	}
	return nil
}

// Unlock releases an acquired lock with context.Context. If the lock is not currently acquired by
// this Lock instance than ErrNotLocked is returned.
func (l *Lock) Unlock(ctx context.Context) error {
	if l.lockPath == "" {
		return ErrNotLocked
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := l.c.Delete(l.lockPath, -1); err != nil {
				return err
			}
			l.lockPath = ""
			return nil
		}
	}
}
