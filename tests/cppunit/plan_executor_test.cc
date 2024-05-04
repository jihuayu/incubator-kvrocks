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
#include "search/plan_executor.h"

#include <gtest/gtest.h>

#include <memory>

#include "config/config.h"
#include "search/executors/mock_executor.h"
#include "search/indexer.h"
#include "search/interval.h"
#include "search/ir.h"
#include "search/ir_plan.h"
#include "test_base.h"
#include "types/redis_json.h"

using namespace kqir;

static auto exe_end = ExecutorNode::Result(ExecutorNode::end);

static IndexMap MakeIndexMap() {
  auto f1 = FieldInfo("f1", std::make_unique<redis::SearchTagFieldMetadata>());
  auto f2 = FieldInfo("f2", std::make_unique<redis::SearchNumericFieldMetadata>());
  auto f3 = FieldInfo("f3", std::make_unique<redis::SearchNumericFieldMetadata>());
  auto ia = std::make_unique<IndexInfo>("ia", SearchMetadata());
  ia->ns = "search_ns";
  ia->metadata.on_data_type = SearchOnDataType::JSON;
  ia->prefixes.prefixes.emplace_back("test2:");
  ia->prefixes.prefixes.emplace_back("test4:");
  ia->Add(std::move(f1));
  ia->Add(std::move(f2));
  ia->Add(std::move(f3));

  auto& name = ia->name;
  IndexMap res;
  res.emplace(name, std::move(ia));
  return res;
}

static auto index_map = MakeIndexMap();

static auto NextRow(ExecutorContext& ctx) {
  auto n = ctx.Next();
  EXPECT_EQ(n.Msg(), Status::ok_msg);
  auto v = std::move(n).GetValue();
  EXPECT_EQ(v.index(), 1);
  return std::get<ExecutorNode::RowType>(std::move(v));
}

TEST(PlanExecutorTest, Mock) {
  auto op = std::make_unique<Mock>(std::vector<ExecutorNode::RowType>{});

  auto ctx = ExecutorContext(op.get());
  ASSERT_EQ(ctx.Next().GetValue(), exe_end);

  op = std::make_unique<Mock>(std::vector<ExecutorNode::RowType>{{"a"}, {"b"}, {"c"}});

  ctx = ExecutorContext(op.get());
  ASSERT_EQ(NextRow(ctx).key, "a");
  ASSERT_EQ(NextRow(ctx).key, "b");
  ASSERT_EQ(NextRow(ctx).key, "c");
  ASSERT_EQ(ctx.Next().GetValue(), exe_end);
}

static auto IndexI() -> const IndexInfo* { return index_map.at("ia").get(); }
static auto FieldI(const std::string& f) -> const FieldInfo* { return &index_map.at("ia")->fields.at(f); }

TEST(PlanExecutorTest, TopNSort) {
  std::vector<ExecutorNode::RowType> data{
      {"a", {{FieldI("f3"), "4"}}, IndexI()}, {"b", {{FieldI("f3"), "2"}}, IndexI()},
      {"c", {{FieldI("f3"), "7"}}, IndexI()}, {"d", {{FieldI("f3"), "3"}}, IndexI()},
      {"e", {{FieldI("f3"), "1"}}, IndexI()}, {"f", {{FieldI("f3"), "6"}}, IndexI()},
      {"g", {{FieldI("f3"), "8"}}, IndexI()},
  };
  {
    auto op = std::make_unique<TopNSort>(
        std::make_unique<Mock>(data),
        std::make_unique<SortByClause>(SortByClause::ASC, std::make_unique<FieldRef>("f3", FieldI("f3"))),
        std::make_unique<LimitClause>(0, 4));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "e");
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "d");
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
  {
    auto op = std::make_unique<TopNSort>(
        std::make_unique<Mock>(data),
        std::make_unique<SortByClause>(SortByClause::ASC, std::make_unique<FieldRef>("f3", FieldI("f3"))),
        std::make_unique<LimitClause>(1, 4));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "d");
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(NextRow(ctx).key, "f");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}

TEST(PlanExecutorTest, Filter) {
  std::vector<ExecutorNode::RowType> data{
      {"a", {{FieldI("f3"), "4"}}, IndexI()}, {"b", {{FieldI("f3"), "2"}}, IndexI()},
      {"c", {{FieldI("f3"), "7"}}, IndexI()}, {"d", {{FieldI("f3"), "3"}}, IndexI()},
      {"e", {{FieldI("f3"), "1"}}, IndexI()}, {"f", {{FieldI("f3"), "6"}}, IndexI()},
      {"g", {{FieldI("f3"), "8"}}, IndexI()},
  };
  {
    auto field = std::make_unique<FieldRef>("f3", FieldI("f3"));
    auto op = std::make_unique<Filter>(
        std::make_unique<Mock>(data),
        AndExpr::Create(Node::List<QueryExpr>(
            std::make_unique<NumericCompareExpr>(NumericCompareExpr::GT, field->CloneAs<FieldRef>(),
                                                 std::make_unique<NumericLiteral>(2)),
            std::make_unique<NumericCompareExpr>(NumericCompareExpr::LET, field->CloneAs<FieldRef>(),
                                                 std::make_unique<NumericLiteral>(6)))));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(NextRow(ctx).key, "d");
    ASSERT_EQ(NextRow(ctx).key, "f");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
  {
    auto field = std::make_unique<FieldRef>("f3", FieldI("f3"));
    auto op = std::make_unique<Filter>(
        std::make_unique<Mock>(data),
        OrExpr::Create(Node::List<QueryExpr>(
            std::make_unique<NumericCompareExpr>(NumericCompareExpr::GET, field->CloneAs<FieldRef>(),
                                                 std::make_unique<NumericLiteral>(6)),
            std::make_unique<NumericCompareExpr>(NumericCompareExpr::LT, field->CloneAs<FieldRef>(),
                                                 std::make_unique<NumericLiteral>(2)))));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "c");
    ASSERT_EQ(NextRow(ctx).key, "e");
    ASSERT_EQ(NextRow(ctx).key, "f");
    ASSERT_EQ(NextRow(ctx).key, "g");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }

  data = {{"a", {{FieldI("f1"), "cpp,java"}}, IndexI()},    {"b", {{FieldI("f1"), "python,cpp,c"}}, IndexI()},
          {"c", {{FieldI("f1"), "c,perl"}}, IndexI()},      {"d", {{FieldI("f1"), "rust,python"}}, IndexI()},
          {"e", {{FieldI("f1"), "java,kotlin"}}, IndexI()}, {"f", {{FieldI("f1"), "c,rust"}}, IndexI()},
          {"g", {{FieldI("f1"), "c,cpp,java"}}, IndexI()}};
  {
    auto field = std::make_unique<FieldRef>("f1", FieldI("f1"));
    auto op = std::make_unique<Filter>(
        std::make_unique<Mock>(data),
        AndExpr::Create(Node::List<QueryExpr>(
            std::make_unique<TagContainExpr>(field->CloneAs<FieldRef>(), std::make_unique<StringLiteral>("c")),
            std::make_unique<TagContainExpr>(field->CloneAs<FieldRef>(), std::make_unique<StringLiteral>("cpp")))));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "g");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
  {
    auto field = std::make_unique<FieldRef>("f1", FieldI("f1"));
    auto op = std::make_unique<Filter>(
        std::make_unique<Mock>(data),
        OrExpr::Create(Node::List<QueryExpr>(
            std::make_unique<TagContainExpr>(field->CloneAs<FieldRef>(), std::make_unique<StringLiteral>("rust")),
            std::make_unique<TagContainExpr>(field->CloneAs<FieldRef>(), std::make_unique<StringLiteral>("perl")))));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "c");
    ASSERT_EQ(NextRow(ctx).key, "d");
    ASSERT_EQ(NextRow(ctx).key, "f");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}

TEST(PlanExecutorTest, Limit) {
  std::vector<ExecutorNode::RowType> data{
      {"a", {{FieldI("f3"), "4"}}, IndexI()}, {"b", {{FieldI("f3"), "2"}}, IndexI()},
      {"c", {{FieldI("f3"), "7"}}, IndexI()}, {"d", {{FieldI("f3"), "3"}}, IndexI()},
      {"e", {{FieldI("f3"), "1"}}, IndexI()}, {"f", {{FieldI("f3"), "6"}}, IndexI()},
      {"g", {{FieldI("f3"), "8"}}, IndexI()},
  };
  {
    auto op = std::make_unique<Limit>(std::make_unique<Mock>(data), std::make_unique<LimitClause>(1, 2));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "c");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
  {
    auto field = std::make_unique<FieldRef>("f3", FieldI("f3"));
    auto op = std::make_unique<Limit>(std::make_unique<Mock>(data), std::make_unique<LimitClause>(0, 3));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "c");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}

TEST(PlanExecutorTest, Merge) {
  std::vector<ExecutorNode::RowType> data1{
      {"a", {{FieldI("f3"), "4"}}, IndexI()},
      {"b", {{FieldI("f3"), "2"}}, IndexI()},
  };
  std::vector<ExecutorNode::RowType> data2{{"c", {{FieldI("f3"), "7"}}, IndexI()},
                                           {"d", {{FieldI("f3"), "3"}}, IndexI()},
                                           {"e", {{FieldI("f3"), "1"}}, IndexI()}};
  {
    auto op =
        std::make_unique<Merge>(Node::List<PlanOperator>(std::make_unique<Mock>(data1), std::make_unique<Mock>(data2)));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(NextRow(ctx).key, "c");
    ASSERT_EQ(NextRow(ctx).key, "d");
    ASSERT_EQ(NextRow(ctx).key, "e");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
  {
    auto op = std::make_unique<Merge>(
        Node::List<PlanOperator>(std::make_unique<Mock>(decltype(data1){}), std::make_unique<Mock>(data1)));

    auto ctx = ExecutorContext(op.get());
    ASSERT_EQ(NextRow(ctx).key, "a");
    ASSERT_EQ(NextRow(ctx).key, "b");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}

class PlanExecutorTestC : public TestBase {
 protected:
  explicit PlanExecutorTestC() : json_(std::make_unique<redis::Json>(storage_.get(), "search_ns")) {}
  ~PlanExecutorTestC() override = default;

  void SetUp() override {}
  void TearDown() override {}

  std::unique_ptr<redis::Json> json_;
};

TEST_F(PlanExecutorTestC, FullIndexScan) {
  json_->Set("test1:a", "$", "{}");
  json_->Set("test1:b", "$", "{}");
  json_->Set("test2:c", "$", "{\"f3\": 6}");
  json_->Set("test3:d", "$", "{}");
  json_->Set("test4:e", "$", "{\"f3\": 7}");
  json_->Set("test4:f", "$", "{\"f3\": 2}");
  json_->Set("test4:g", "$", "{\"f3\": 8}");
  json_->Set("test5:h", "$", "{}");
  json_->Set("test5:i", "$", "{}");
  json_->Set("test5:g", "$", "{}");

  {
    auto op = std::make_unique<FullIndexScan>(std::make_unique<IndexRef>("ia", IndexI()));

    auto ctx = ExecutorContext(op.get(), storage_.get());
    ASSERT_EQ(NextRow(ctx).key, "test2:c");
    ASSERT_EQ(NextRow(ctx).key, "test4:e");
    ASSERT_EQ(NextRow(ctx).key, "test4:f");
    ASSERT_EQ(NextRow(ctx).key, "test4:g");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }

  {
    auto op = std::make_unique<Filter>(
        std::make_unique<FullIndexScan>(std::make_unique<IndexRef>("ia", IndexI())),
        std::make_unique<NumericCompareExpr>(NumericCompareExpr::GT, std::make_unique<FieldRef>("f3", FieldI("f3")),
                                             std::make_unique<NumericLiteral>(3)));

    auto ctx = ExecutorContext(op.get(), storage_.get());
    ASSERT_EQ(NextRow(ctx).key, "test2:c");
    ASSERT_EQ(NextRow(ctx).key, "test4:e");
    ASSERT_EQ(NextRow(ctx).key, "test4:g");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}

struct ScopedUpdate {
  redis::GlobalIndexer::RecordResult rr;
  std::string_view key;
  std::string ns;

  static auto Create(redis::GlobalIndexer& indexer, std::string_view key, const std::string& ns) {
    auto s = indexer.Record(key, ns);
    EXPECT_EQ(s.Msg(), Status::ok_msg);
    return *s;
  }

  ScopedUpdate(redis::GlobalIndexer& indexer, std::string_view key, const std::string& ns)
      : rr(Create(indexer, key, ns)), key(key), ns(ns) {}

  ScopedUpdate(const ScopedUpdate&) = delete;
  ScopedUpdate(ScopedUpdate&&) = delete;
  ScopedUpdate& operator=(const ScopedUpdate&) = delete;
  ScopedUpdate& operator=(ScopedUpdate&&) = delete;

  ~ScopedUpdate() {
    auto s = redis::GlobalIndexer::Update(rr, key, ns);
    EXPECT_EQ(s.Msg(), Status::ok_msg);
  }
};

std::vector<std::unique_ptr<ScopedUpdate>> ScopedUpdates(redis::GlobalIndexer& indexer,
                                                         const std::vector<std::string_view>& keys,
                                                         const std::string& ns) {
  std::vector<std::unique_ptr<ScopedUpdate>> sus;

  sus.reserve(keys.size());
  for (auto key : keys) {
    sus.emplace_back(std::make_unique<ScopedUpdate>(indexer, key, ns));
  }

  return sus;
}

TEST_F(PlanExecutorTestC, NumericFieldScan) {
  redis::GlobalIndexer indexer(storage_.get());
  indexer.Add(redis::IndexUpdater(IndexI()));

  {
    auto updates = ScopedUpdates(indexer, {"test2:a", "test2:b", "test2:c", "test2:d", "test2:e", "test2:f", "test2:g"},
                                 "search_ns");
    json_->Set("test2:a", "$", "{\"f2\": 6}");
    json_->Set("test2:b", "$", "{\"f2\": 3}");
    json_->Set("test2:c", "$", "{\"f2\": 8}");
    json_->Set("test2:d", "$", "{\"f2\": 14}");
    json_->Set("test2:e", "$", "{\"f2\": 1}");
    json_->Set("test2:f", "$", "{\"f2\": 3}");
    json_->Set("test2:g", "$", "{\"f2\": 9}");
  }

  {
    auto op = std::make_unique<NumericFieldScan>(std::make_unique<FieldRef>("f2", FieldI("f2")), Interval(3, 9),
                                                 SortByClause::ASC);

    auto ctx = ExecutorContext(op.get(), storage_.get());
    ASSERT_EQ(NextRow(ctx).key, "test2:b");
    ASSERT_EQ(NextRow(ctx).key, "test2:f");
    ASSERT_EQ(NextRow(ctx).key, "test2:a");
    ASSERT_EQ(NextRow(ctx).key, "test2:c");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }

  {
    auto op = std::make_unique<NumericFieldScan>(std::make_unique<FieldRef>("f2", FieldI("f2")), Interval(3, 9),
                                                 SortByClause::DESC);

    auto ctx = ExecutorContext(op.get(), storage_.get());
    ASSERT_EQ(NextRow(ctx).key, "test2:c");
    ASSERT_EQ(NextRow(ctx).key, "test2:a");
    ASSERT_EQ(NextRow(ctx).key, "test2:f");
    ASSERT_EQ(NextRow(ctx).key, "test2:b");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}

TEST_F(PlanExecutorTestC, TagFieldScan) {
  redis::GlobalIndexer indexer(storage_.get());
  indexer.Add(redis::IndexUpdater(IndexI()));

  {
    auto updates = ScopedUpdates(indexer, {"test2:a", "test2:b", "test2:c", "test2:d", "test2:e", "test2:f", "test2:g"},
                                 "search_ns");
    json_->Set("test2:a", "$", "{\"f1\": \"c,cpp,java\"}");
    json_->Set("test2:b", "$", "{\"f1\": \"python,c\"}");
    json_->Set("test2:c", "$", "{\"f1\": \"java,scala\"}");
    json_->Set("test2:d", "$", "{\"f1\": \"rust,python,perl\"}");
    json_->Set("test2:e", "$", "{\"f1\": \"python,cpp\"}");
    json_->Set("test2:f", "$", "{\"f1\": \"c,cpp\"}");
    json_->Set("test2:g", "$", "{\"f1\": \"cpp,rust\"}");
  }

  {
    auto op = std::make_unique<TagFieldScan>(std::make_unique<FieldRef>("f1", FieldI("f1")), "cpp");

    auto ctx = ExecutorContext(op.get(), storage_.get());
    ASSERT_EQ(NextRow(ctx).key, "test2:a");
    ASSERT_EQ(NextRow(ctx).key, "test2:e");
    ASSERT_EQ(NextRow(ctx).key, "test2:f");
    ASSERT_EQ(NextRow(ctx).key, "test2:g");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }

  {
    auto op = std::make_unique<TagFieldScan>(std::make_unique<FieldRef>("f1", FieldI("f1")), "python");

    auto ctx = ExecutorContext(op.get(), storage_.get());
    ASSERT_EQ(NextRow(ctx).key, "test2:b");
    ASSERT_EQ(NextRow(ctx).key, "test2:d");
    ASSERT_EQ(NextRow(ctx).key, "test2:e");
    ASSERT_EQ(ctx.Next().GetValue(), exe_end);
  }
}