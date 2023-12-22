/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package rename

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestRename_String(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Rename string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 10*time.Second).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not string type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "world").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 10*time.Second).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 1000*time.Second).Err())
		require.NoError(t, rdb.Set(ctx, "a2", "world", 1000*time.Second).Err())
		require.NoError(t, rdb.Set(ctx, "a3", "world", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a3").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a3").Val(), time.Second, 10*time.Second)
	})

	t.Run("RenameNX string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "world", rdb.Get(ctx, "a1").Val())

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())
	})

}
