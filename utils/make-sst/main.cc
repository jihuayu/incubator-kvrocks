#include <iostream>
#include <string>

#include "batch_writer.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "storage/redis_metadata.h"
#include "types.h"

using namespace rocksdb;

void ComposeNamespaceKey(const Slice &ns, const Slice &key, std::string *ns_key) {
  ns_key->clear();

  PutFixed8(ns_key, static_cast<uint8_t>(ns.size()));
  ns_key->append(ns.data(), ns.size());

  ns_key->append(key.data(), key.size());
}

int main() {
  auto bw = BatchWriter();
  for (size_t i = 0; i < 10000; i++) {
    bw.Write(makeString(std::to_string(i), "hello " + std::to_string(i), 0));
  }
  bw.FlushAll();
  return 0;
}