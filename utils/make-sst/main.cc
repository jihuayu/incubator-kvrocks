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


int main() {
  auto bw = BatchWriter();
  for (size_t i = 0; i < 100000000; i++) {
    bw.Write(makeString(std::to_string(i), "hello " + std::to_string(i), 0));
  }
  bw.FlushAll();
  return 0;
}