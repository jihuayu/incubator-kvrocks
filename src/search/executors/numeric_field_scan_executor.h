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
 *
 */

#pragma once

#include <string>

#include "db_util.h"
#include "encoding.h"
#include "search/plan_executor.h"
#include "search/search_encoding.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"

namespace kqir {

struct NumericFieldScanExecutor : ExecutorNode {
  NumericFieldScan *scan;
  redis::LatestSnapShot ss;
  util::UniqueIterator iter{nullptr};

  IndexInfo *index;
  std::string ns_key;

  NumericFieldScanExecutor(ExecutorContext *ctx, NumericFieldScan *scan)
      : ExecutorNode(ctx), scan(scan), ss(ctx->storage), index(scan->field->info->index) {
    ns_key = ComposeNamespaceKey(index->ns, index->name, ctx->storage->IsSlotIdEncoded());
  }

  std::string IndexKey(double num) {
    return InternalKey(ns_key, redis::ConstructNumericFieldSubkey(scan->field->name, num, {}), index->metadata.version,
                       ctx->storage->IsSlotIdEncoded())
        .Encode();
  }

  bool InRangeDecode(Slice key, Slice field, double num, double *curr, Slice *user_key) {
    auto ikey = InternalKey(key, ctx->storage->IsSlotIdEncoded());
    if (ikey.GetVersion() != index->metadata.version) return false;
    auto subkey = ikey.GetSubKey();

    uint8_t flag = 0;
    if (!GetFixed8(&subkey, &flag)) return false;
    if (flag != (uint8_t)redis::SearchSubkeyType::NUMERIC_FIELD) return false;

    Slice value;
    if (!GetSizedString(&subkey, &value)) return false;
    if (value != field) return false;

    if (!GetDouble(&subkey, curr)) return false;

    if (!GetSizedString(&subkey, user_key)) return false;

    return true;
  }

  StatusOr<Result> Next() override {
    if (!iter) {
      rocksdb::ReadOptions read_options = ctx->storage->DefaultScanOptions();
      read_options.snapshot = ss.GetSnapShot();

      iter = util::UniqueIterator(ctx->storage, read_options, ctx->storage->GetCFHandle(ColumnFamilyID::Search));
      if (scan->order == SortByClause::ASC) {
        iter->Seek(IndexKey(scan->range.l));
      } else {
        iter->SeekForPrev(IndexKey(IntervalSet::PrevNum(scan->range.r)));
      }
    }

    if (!iter->Valid()) {
      return end;
    }

    double curr = 0;
    Slice user_key;
    if (!InRangeDecode(iter->key(), scan->field->name, scan->range.r, &curr, &user_key)) {
      return end;
    }

    if (scan->order == SortByClause::ASC ? curr >= scan->range.r : curr < scan->range.l) {
      return end;
    }

    auto key_str = user_key.ToString();

    if (scan->order == SortByClause::ASC) {
      iter->Next();
    } else {
      iter->Prev();
    }
    return RowType{key_str, {{scan->field->info, std::to_string(curr)}}, scan->field->info->index};
  }
};

}  // namespace kqir