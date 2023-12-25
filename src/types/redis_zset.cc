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

#include "redis_zset.h"

#include <cmath>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>

#include "db_util.h"
#include "storage/redis_metadata.h"

namespace redis {

rocksdb::Status ZSet::GetMetadata(const Slice &ns_key, ZSetMetadata *metadata) {
  return Database::GetMetadata(kRedisZSet, ns_key, metadata);
}

rocksdb::Status ZSet::Add(const Slice &user_key, ZAddFlags flags, MemberScores *mscores, uint64_t *added_cnt) {
  *added_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  int added = 0;
  int changed = 0;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  std::unordered_set<std::string_view> added_member_keys;
  for (auto it = mscores->rbegin(); it != mscores->rend(); it++) {
    if (!added_member_keys.insert(it->member).second) {
      continue;
    }
    std::string member_key = InternalKey(ns_key, it->member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    if (metadata.size > 0) {
      std::string old_score_bytes;
      s = storage_->Get(rocksdb::ReadOptions(), member_key, &old_score_bytes);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        if (!s.IsNotFound() && flags.HasNX()) {
          continue;
        }
        double old_score = DecodeDouble(old_score_bytes.data());
        if (flags.HasIncr()) {
          if ((flags.HasLT() && it->score >= 0) || (flags.HasGT() && it->score <= 0)) {
            continue;
          }
          it->score += old_score;
          if (std::isnan(it->score)) {
            return rocksdb::Status::InvalidArgument("resulting score is not a number (NaN)");
          }
        }
        if (it->score != old_score) {
          if ((flags.HasLT() && it->score >= old_score) || (flags.HasGT() && it->score <= old_score)) {
            continue;
          }
          old_score_bytes.append(it->member);
          std::string old_score_key =
              InternalKey(ns_key, old_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
          batch->Delete(score_cf_handle_, old_score_key);
          std::string new_score_bytes;
          PutDouble(&new_score_bytes, it->score);
          batch->Put(member_key, new_score_bytes);
          new_score_bytes.append(it->member);
          std::string new_score_key =
              InternalKey(ns_key, new_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
          batch->Put(score_cf_handle_, new_score_key, Slice());
          changed++;
        }
        continue;
      }
    }
    if (flags.HasXX()) {
      continue;
    }
    std::string score_bytes;
    PutDouble(&score_bytes, it->score);
    batch->Put(member_key, score_bytes);
    score_bytes.append(it->member);
    std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    batch->Put(score_cf_handle_, score_key, Slice());
    added++;
  }
  if (added > 0) {
    *added_cnt = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  if (flags.HasCH()) {
    *added_cnt += changed;
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::Card(const Slice &user_key, uint64_t *size) {
  *size = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Count(const Slice &user_key, const RangeScoreSpec &spec, uint64_t *size) {
  return RangeByScore(user_key, spec, nullptr, size);
}

rocksdb::Status ZSet::IncrBy(const Slice &user_key, const Slice &member, double increment, double *score) {
  uint64_t ret = 0;
  std::vector<MemberScore> mscores;
  mscores.emplace_back(MemberScore{member.ToString(), increment});
  rocksdb::Status s = Add(user_key, ZAddFlags::Incr(), &mscores, &ret);
  if (!s.ok()) return s;
  *score = mscores[0].score;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Pop(const Slice &user_key, int count, bool min, MemberScores *mscores) {
  mscores->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  if (count <= 0) return rocksdb::Status::OK();
  if (count > static_cast<int>(metadata.size)) count = static_cast<int>(metadata.size);

  std::string score_bytes;
  double score = min ? kMinScore : kMaxScore;
  PutDouble(&score_bytes, score);
  std::string start_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  iter->Seek(start_key);
  // see comment in rangebyscore()
  if (!min && (!iter->Valid() || !iter->key().starts_with(prefix_key))) {
    iter->SeekForPrev(start_key);
  }
  for (; iter->Valid() && iter->key().starts_with(prefix_key); min ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    GetDouble(&score_key, &score);
    mscores->emplace_back(MemberScore{score_key.ToString(), score});
    std::string default_cf_key = InternalKey(ns_key, score_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    batch->Delete(default_cf_key);
    batch->Delete(score_cf_handle_, iter->key());
    if (mscores->size() >= static_cast<unsigned>(count)) break;
  }

  if (!mscores->empty()) {
    metadata.size -= mscores->size();
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::RangeByRank(const Slice &user_key, const RangeRankSpec &spec, MemberScores *mscores,
                                  uint64_t *removed_cnt) {
  if (mscores) mscores->clear();

  uint64_t cnt = 0;
  if (!removed_cnt) removed_cnt = &cnt;
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  std::optional<LockGuard> lock_guard;
  if (spec.with_deletion) lock_guard.emplace(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  int start = spec.start;
  int stop = spec.stop;

  if (start < 0) start += static_cast<int>(metadata.size);
  if (stop < 0) stop += static_cast<int>(metadata.size);
  if (start < 0) start = 0;
  if (stop < 0 || start > stop) {
    return rocksdb::Status::OK();
  }

  std::string score_bytes;
  double score = !(spec.reversed) ? kMinScore : kMaxScore;
  PutDouble(&score_bytes, score);
  std::string start_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  int removed_subkey = 0;
  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto batch = storage_->GetWriteBatchBase();
  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  iter->Seek(start_key);
  // see comment in rangebyscore()
  if (spec.reversed && (!iter->Valid() || !iter->key().starts_with(prefix_key))) {
    iter->SeekForPrev(start_key);
  }

  int count = 0;
  for (; iter->Valid() && iter->key().starts_with(prefix_key); !(spec.reversed) ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    GetDouble(&score_key, &score);
    if (count >= start) {
      if (spec.with_deletion) {
        std::string sub_key = InternalKey(ns_key, score_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
        batch->Delete(sub_key);
        batch->Delete(score_cf_handle_, iter->key());
        removed_subkey++;
      } else {
        if (mscores) mscores->emplace_back(MemberScore{score_key.ToString(), score});
      }
      *removed_cnt += 1;
    }
    if (count++ >= stop) break;
  }

  if (removed_subkey) {
    metadata.size -= removed_subkey;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::RangeByScore(const Slice &user_key, const RangeScoreSpec &spec, MemberScores *mscores,
                                   uint64_t *removed_cnt) {
  if (mscores) mscores->clear();

  uint64_t cnt = 0;
  if (!removed_cnt) removed_cnt = &cnt;
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  std::optional<LockGuard> lock_guard;
  if (spec.with_deletion) lock_guard.emplace(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  // let's get familiar with score first:
  //    a. score of zset's member is represented by double and it takes up 8 bytes in rocksdb
  //    b. to make positive double greater than native double in lexicographical order, score
  //       is required encoding before stored in rocksdb. encoding details see PutDouble()
  //    c. for convenience, user_score and inner_score respectively represent before and after encoding
  //
  // next lexicographical ordered inner_score of max:
  //    a. we can think of inner_score as a fixed 8-byte string. logically, the next lexicographical
  //       ordered inner_score of max_inner_score is 'max_inner_score + 1' if we assume no overflow.
  //       'max_inner_score + 1' means binary increment.
  //    b. realize binary increment 'max_inner_score + 1'
  //       use PutDouble() encoding max(max_user_score) to max_inner_score
  //       memcpy max_inner_score to u64(uint64_t)
  //       incr u64
  //       memcpy u64 to max_next_inner_score
  //    it may not be hard to understand about how to get max_next_inner_score
  //
  // directly generate max_next_user_score of max_next_inner_score:
  //    a. give a key argument first:
  //       for positive score, user_score is positively correlated with inner_score in lexicographical order
  //       for negative score, user_score is negatively correlated with inner_score in lexicographical order
  //       more details see PutDouble()
  //    b. get max_next_user_score of max_next_inner_score:
  //       for positive max_user_score, max_next_user_score is 'max_user_score + 1'
  //       for negative max_user_score, max_next_user_score is 'max_user_score - 1'
  // Note: fortunately, there is no overflow in fact. more details see binary encoding of double
  // binary encoding of double: https://en.wikipedia.org/wiki/Double-precision_floating-point_format

  // generate next possible score of max
  int64_t i64 = 0;
  double max_next_score = 0;
  if (spec.reversed && !spec.maxex) {
    memcpy(&i64, &spec.max, sizeof(spec.max));
    i64 = i64 >= 0 ? i64 + 1 : i64 - 1;
    memcpy(&max_next_score, &i64, sizeof(i64));
  }

  std::string start_score_bytes;
  PutDouble(&start_score_bytes, spec.reversed ? (spec.maxex ? spec.max : max_next_score) : spec.min);
  std::string start_key =
      InternalKey(ns_key, start_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  int pos = 0;
  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    iter->SeekForPrev(start_key);
    if (iter->Valid() && iter->key().starts_with(start_key)) {
      iter->Prev();
    }
  }

  for (; iter->Valid() && iter->key().starts_with(prefix_key); !spec.reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    double score = NAN;
    GetDouble(&score_key, &score);
    if (spec.reversed) {
      if ((spec.minex && score == spec.min) || score < spec.min) break;
      if ((spec.maxex && score == spec.max) || score > spec.max) continue;
    } else {
      if ((spec.minex && score == spec.min) || score < spec.min) continue;
      if ((spec.maxex && score == spec.max) || score > spec.max) break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    if (spec.with_deletion) {
      std::string sub_key = InternalKey(ns_key, score_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      batch->Delete(sub_key);
      batch->Delete(score_cf_handle_, iter->key());
    } else {
      if (mscores) mscores->emplace_back(MemberScore{score_key.ToString(), score});
    }
    *removed_cnt += 1;
    if (spec.count > 0 && mscores && mscores->size() >= static_cast<unsigned>(spec.count)) break;
  }

  if (spec.with_deletion && *removed_cnt > 0) {
    metadata.size -= *removed_cnt;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::RangeByLex(const Slice &user_key, const RangeLexSpec &spec, MemberScores *mscores,
                                 uint64_t *removed_cnt) {
  if (mscores) mscores->clear();

  uint64_t cnt = 0;
  if (!removed_cnt) removed_cnt = &cnt;
  *removed_cnt = 0;

  if (spec.offset > -1 && spec.count == 0) {
    return rocksdb::Status::OK();
  }

  std::string ns_key = AppendNamespacePrefix(user_key);

  std::optional<LockGuard> lock_guard;
  if (spec.with_deletion) {
    lock_guard.emplace(storage_->GetLockManager(), ns_key);
  }
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_member = spec.reversed ? spec.max : spec.min;
  std::string start_key = InternalKey(ns_key, start_member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  int pos = 0;
  auto iter = util::UniqueIterator(storage_, read_options);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());

  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    if (spec.max_infinite) {
      iter->SeekToLast();
    } else {
      iter->SeekForPrev(start_key);
    }
  }

  for (; iter->Valid() && iter->key().starts_with(prefix_key); (!spec.reversed ? iter->Next() : iter->Prev())) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice member = ikey.GetSubKey();
    if (spec.reversed) {
      if (member.ToString() < spec.min || (spec.minex && member == spec.min)) {
        break;
      }
      if ((spec.maxex && member == spec.max) || (!spec.max_infinite && member.ToString() > spec.max)) {
        continue;
      }
    } else {
      if (spec.minex && member == spec.min) continue;  // the min member was exclusive
      if ((spec.maxex && member == spec.max) || (!spec.max_infinite && member.ToString() > spec.max)) break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    if (spec.with_deletion) {
      std::string score_bytes = iter->value().ToString();
      score_bytes.append(member.data(), member.size());
      std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      batch->Delete(score_cf_handle_, score_key);
      batch->Delete(iter->key());
    } else {
      if (mscores) mscores->emplace_back(MemberScore{member.ToString(), DecodeDouble(iter->value().data())});
    }
    *removed_cnt += 1;
    if (spec.count > 0 && mscores && mscores->size() >= static_cast<unsigned>(spec.count)) break;
  }

  if (spec.with_deletion && *removed_cnt > 0) {
    metadata.size -= *removed_cnt;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
    return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Score(const Slice &user_key, const Slice &member, double *score) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();

  std::string score_bytes;
  std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(read_options, member_key, &score_bytes);
  if (!s.ok()) return s;
  *score = DecodeDouble(score_bytes.data());
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Remove(const Slice &user_key, const std::vector<Slice> &members, uint64_t *removed_cnt) {
  *removed_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  int removed = 0;
  std::unordered_set<std::string_view> mset;
  for (const auto &member : members) {
    if (!mset.insert(member.ToStringView()).second) {
      continue;
    }
    std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string score_bytes;
    s = storage_->Get(rocksdb::ReadOptions(), member_key, &score_bytes);
    if (s.ok()) {
      score_bytes.append(member.data(), member.size());
      std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      batch->Delete(member_key);
      batch->Delete(score_cf_handle_, score_key);
      removed++;
    }
  }
  if (removed > 0) {
    *removed_cnt = removed;
    metadata.size -= removed;
    std::string bytes;
    metadata.Encode(&bytes);
    batch->Put(metadata_cf_handle_, ns_key, bytes);
  }
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::Rank(const Slice &user_key, const Slice &member, bool reversed, int *member_rank,
                           double *member_score) {
  *member_rank = -1;
  *member_score = 0.0;

  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  std::string score_bytes;
  std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(read_options, member_key, &score_bytes);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  double target_score = DecodeDouble(score_bytes.data());
  std::string start_score_bytes;
  double start_score = !reversed ? kMinScore : kMaxScore;
  PutDouble(&start_score_bytes, start_score);
  std::string start_key =
      InternalKey(ns_key, start_score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  int rank = 0;
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(storage_, read_options, score_cf_handle_);
  iter->Seek(start_key);
  // see comment in rangebyscore()
  if (reversed && (!iter->Valid() || !iter->key().starts_with(prefix_key))) {
    iter->SeekForPrev(start_key);
  }
  for (; iter->Valid() && iter->key().starts_with(prefix_key); !reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice score_key = ikey.GetSubKey();
    double score = NAN;
    GetDouble(&score_key, &score);
    if (score == target_score && score_key == member) break;
    rank++;
  }

  *member_rank = rank;
  *member_score = target_score;
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Overwrite(const Slice &user_key, const MemberScores &mscores) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ZSetMetadata metadata;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet);
  batch->PutLogData(log_data.Encode());
  for (const auto &ms : mscores) {
    std::string score_bytes;
    std::string member_key = InternalKey(ns_key, ms.member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    PutDouble(&score_bytes, ms.score);
    batch->Put(member_key, score_bytes);
    score_bytes.append(ms.member);
    std::string score_key = InternalKey(ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    batch->Put(score_cf_handle_, score_key, Slice());
  }
  metadata.size = static_cast<uint32_t>(mscores.size());
  std::string bytes;
  metadata.Encode(&bytes);
  batch->Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status ZSet::InterStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                                 AggregateMethod aggregate_method, uint64_t *saved_cnt) {
  *saved_cnt = 0;
  std::vector<MemberScore> members;
  auto s = Inter(keys_weights, aggregate_method, &members);
  if (!s.ok()) return s;
  *saved_cnt = members.size();
  return Overwrite(dst, members);
}

rocksdb::Status ZSet::Inter(const std::vector<KeyWeight> &keys_weights, AggregateMethod aggregate_method,
                            std::vector<MemberScore> *members) {
  std::vector<std::string> lock_keys;
  lock_keys.reserve(keys_weights.size());
  for (const auto &key_weight : keys_weights) {
    std::string ns_key = AppendNamespacePrefix(key_weight.key);
    lock_keys.emplace_back(std::move(ns_key));
  }
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);

  std::map<std::string, double> dst_zset;
  std::map<std::string, size_t> member_counters;
  std::vector<MemberScore> target_mscores;
  uint64_t target_size = 0;
  RangeScoreSpec spec;
  auto s = RangeByScore(keys_weights[0].key, spec, &target_mscores, &target_size);
  if (!s.ok() || target_mscores.empty()) return s;

  for (const auto &ms : target_mscores) {
    double score = ms.score * keys_weights[0].weight;
    if (std::isnan(score)) score = 0;
    dst_zset[ms.member] = score;
    member_counters[ms.member] = 1;
  }

  for (size_t i = 1; i < keys_weights.size(); i++) {
    s = RangeByScore(keys_weights[i].key, spec, &target_mscores, &target_size);
    if (!s.ok() || target_mscores.empty()) return s;

    for (const auto &ms : target_mscores) {
      if (dst_zset.find(ms.member) == dst_zset.end()) continue;
      member_counters[ms.member]++;
      double score = ms.score * keys_weights[i].weight;
      if (std::isnan(score)) score = 0;
      switch (aggregate_method) {
        case kAggregateSum:
          dst_zset[ms.member] += score;
          if (std::isnan(dst_zset[ms.member])) {
            dst_zset[ms.member] = 0;
          }
          break;
        case kAggregateMin:
          if (dst_zset[ms.member] > score) {
            dst_zset[ms.member] = score;
          }
          break;
        case kAggregateMax:
          if (dst_zset[ms.member] < score) {
            dst_zset[ms.member] = score;
          }
          break;
      }
    }
  }
  if (members && !dst_zset.empty()) {
    members->reserve(dst_zset.size());
    for (const auto &iter : dst_zset) {
      if (member_counters[iter.first] != keys_weights.size()) continue;
      members->emplace_back(MemberScore{iter.first, iter.second});
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::UnionStore(const Slice &dst, const std::vector<KeyWeight> &keys_weights,
                                 AggregateMethod aggregate_method, uint64_t *saved_cnt) {
  *saved_cnt = 0;
  std::vector<MemberScore> members;
  auto s = Union(keys_weights, aggregate_method, &members);
  if (!s.ok()) return s;
  *saved_cnt = members.size();
  return Overwrite(dst, members);
}

rocksdb::Status ZSet::Union(const std::vector<KeyWeight> &keys_weights, AggregateMethod aggregate_method,
                            std::vector<MemberScore> *members) {
  std::vector<std::string> lock_keys;
  lock_keys.reserve(keys_weights.size());
  for (const auto &key_weight : keys_weights) {
    std::string ns_key = AppendNamespacePrefix(key_weight.key);
    lock_keys.emplace_back(std::move(ns_key));
  }
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);

  std::map<std::string, double> dst_zset;
  std::vector<MemberScore> target_mscores;
  uint64_t target_size = 0;
  RangeScoreSpec spec;
  for (const auto &key_weight : keys_weights) {
    // get all member
    auto s = RangeByScore(key_weight.key, spec, &target_mscores, &target_size);
    if (!s.ok() && !s.IsNotFound()) return s;
    for (const auto &ms : target_mscores) {
      double score = ms.score * key_weight.weight;
      if (std::isnan(score)) score = 0;
      if (dst_zset.find(ms.member) == dst_zset.end()) {
        dst_zset[ms.member] = score;
      } else {
        switch (aggregate_method) {
          case kAggregateSum:
            dst_zset[ms.member] += score;
            if (std::isnan(dst_zset[ms.member])) dst_zset[ms.member] = 0;
            break;
          case kAggregateMin:
            if (dst_zset[ms.member] > score) {
              dst_zset[ms.member] = score;
            }
            break;
          case kAggregateMax:
            if (dst_zset[ms.member] < score) {
              dst_zset[ms.member] = score;
            }
            break;
        }
      }
    }
  }
  if (members && !dst_zset.empty()) {
    members->reserve(dst_zset.size());
    for (const auto &iter : dst_zset) {
      members->emplace_back(MemberScore{iter.first, iter.second});
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Scan(const Slice &user_key, const std::string &cursor, uint64_t limit,
                           const std::string &member_prefix, std::vector<std::string> *members,
                           std::vector<double> *scores) {
  if (scores != nullptr) {
    std::vector<std::string> values;
    auto s = SubKeyScanner::Scan(kRedisZSet, user_key, cursor, limit, member_prefix, members, &values);
    if (!s.ok()) return s;

    for (const auto &value : values) {
      double target_score = DecodeDouble(value.data());
      scores->emplace_back(target_score);
    }
    return s;
  }
  return SubKeyScanner::Scan(kRedisZSet, user_key, cursor, limit, member_prefix, members);
}

rocksdb::Status ZSet::MGet(const Slice &user_key, const std::vector<Slice> &members,
                           std::map<std::string, double> *mscores) {
  mscores->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ns_key, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  std::string score_bytes;
  for (const auto &member : members) {
    std::string member_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    score_bytes.clear();
    s = storage_->Get(read_options, member_key, &score_bytes);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      continue;
    }
    double target_score = DecodeDouble(score_bytes.data());
    (*mscores)[member.ToString()] = target_score;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status ZSet::Rename(const std::string &key, const std::string &new_key) {
  std::string from_ns_key = AppendNamespacePrefix(key);
  std::string to_ns_key = AppendNamespacePrefix(new_key);

  ZSetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(from_ns_key, &metadata);
  if (!s.ok()) return s;
  std::string raw_value;
  metadata.Encode(&raw_value);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisZSet, {"kRedisZsetRename"});
  batch->PutLogData(log_data.Encode());

  batch->Delete(metadata_cf_handle_, from_ns_key);
  batch->Put(metadata_cf_handle_, to_ns_key, raw_value);

  // scan and rename subkey
  std::string prefix = InternalKey(from_ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix =
      InternalKey(from_ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = storage_->DefaultScanOptions();
  LatestSnapShot ss(storage_);
  read_options.snapshot = ss.GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey from_ikey(iter->key(), storage_->IsSlotIdEncoded());
    std::string to_sub_key =
        InternalKey(to_ns_key, from_ikey.GetSubKey(), from_ikey.GetVersion(), storage_->IsSlotIdEncoded()).Encode();

    std::string score_bytes;
    s = storage_->Get(read_options, iter->key(), &score_bytes);
    if (!s.ok()) return s;
    // put member key
    batch->Put(to_sub_key, score_bytes);
    score_bytes.append(from_ikey.GetSubKey().ToString());
    // put score key
    std::string score_key = InternalKey(to_ns_key, score_bytes, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    batch->Put(score_cf_handle_, score_key, Slice());
  }

  return storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

}  // namespace redis
