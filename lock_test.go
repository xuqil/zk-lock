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

//go:build e2e

package zk_lock

import (
	"context"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var servers = []string{"localhost:2181"}

func TestClient_Lock(t *testing.T) {
	conn, _, err := zk.Connect(servers, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	basePath := "/lock"

	testCases := []struct {
		name string

		before func(t *testing.T)

		timeout time.Duration
		key     string
		wantErr error
	}{
		{
			name: "locked",
			before: func(t *testing.T) {

			},
			timeout: 10 * time.Second,
			key:     "locked-key",
		},
		{
			name: "lock timeout",
			before: func(t *testing.T) {
				_, err := conn.CreateProtectedEphemeralSequential(basePath+"/locked-key2", nil, zk.WorldACL(zk.PermAll))
				require.NoError(t, err)
			},
			timeout: 5 * time.Second,
			key:     "locked-key2",
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "retry and locked",
			before: func(t *testing.T) {
				path, err := conn.CreateProtectedEphemeralSequential(basePath+"/locked-key3", nil, zk.WorldACL(zk.PermAll))
				require.NoError(t, err)
				go func() {
					time.Sleep(3 * time.Second)
					err = conn.Delete(path, -1)
					require.NoError(t, err)
				}()
			},
			timeout: 5 * time.Second,
			key:     "locked-key3",
		},
		{
			name: "different key and locked",
			before: func(t *testing.T) {
				path, err := conn.CreateProtectedEphemeralSequential(basePath+"/locked-diff", nil, zk.WorldACL(zk.PermAll))
				require.NoError(t, err)
				go func() {
					time.Sleep(3 * time.Second)
					err = conn.Delete(path, -1)
					require.NoError(t, err)
				}()
			},
			timeout: 5 * time.Second,
			key:     "locked-key4",
		},
		{
			name: "invalid key",
			before: func(t *testing.T) {

			},
			timeout: 5 * time.Second,
			key:     "/invalid",
			wantErr: zk.ErrInvalidPath,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			lock := NewLock(conn, tc.key, zk.WorldACL(zk.PermAll),
				LockWithBasePath(basePath), LockWithRetry(3))
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			err := lock.Lock(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Contains(t, lock.lockPath, tc.key)
		})
	}
}

func TestLock_Unlock(t *testing.T) {
	conn, _, err := zk.Connect(servers, 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()

	basePath := "/lock"
	testCases := []struct {
		name   string
		before func(t *testing.T, lock *Lock)

		prefix  string
		wantErr error
	}{
		{
			name: "unlock",
			before: func(t *testing.T, lock *Lock) {

			},
			prefix: "unlock1",
		},
		{
			name: "lock not hold",
			before: func(t *testing.T, lock *Lock) {
				err := conn.Delete(lock.lockPath, -1)
				require.NoError(t, err)
				lock.lockPath = ""
			},
			prefix:  "unlock1",
			wantErr: ErrNotLocked,
		},
		{
			name: "node not exists",
			before: func(t *testing.T, lock *Lock) {
				err := conn.Delete(lock.lockPath, -1)
				require.NoError(t, err)
			},
			prefix:  "unlock1",
			wantErr: zk.ErrNoNode,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := &Lock{
				c:        conn,
				basePath: basePath,
				key:      tc.prefix,
				retries:  3,
				acl:      zk.WorldACL(zk.PermAll),
			}
			lockPath, err := l.c.CreateProtectedEphemeralSequential(basePath+"/"+tc.prefix, nil, zk.WorldACL(zk.PermAll))
			require.NoError(t, err)
			l.lockPath = lockPath
			tc.before(t, l)
			err = l.Unlock(context.Background())
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
