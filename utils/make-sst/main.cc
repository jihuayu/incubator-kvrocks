#include <iostream>
#include <string>
#include "rocksdb/iterator.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "storage/redis_metadata.h"
using namespace rocksdb;

int main(){
    Options options;
    rocksdb::DB* db = nullptr;
    // Get a db with block-based table without any change.
    rocksdb::DB::Open(rocksdb::Options(), "/tmp/kvrocks/db", &db);
    std::cout<<"1111111111111111111111"<<std::endl;
    auto iter = db->NewIterator(ReadOptions());
    std::cout<<"1111111111111111111111"<<std::endl;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::cout<<iter->key().ToString()<<std::endl;
        std::cout<<iter->value().ToString()<<std::endl;
    }
    std::cout<<"1111111111111111111111"<<std::endl;
    SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);
    // Path to where we will write the SST file
    std::string file_path = "./file1.sst";

    // Open the file for writing
    rocksdb::Status s = sst_file_writer.Open(file_path);
    if (!s.ok()) {
        printf("Error while opening file %s, Error: %s\n", file_path.c_str(),
            s.ToString().c_str());
        return 1;
    }

    for (int x = 0;x<10;x++) {
    std::string raw_value;
    Metadata metadata(kRedisString, false);
    metadata.Encode(&raw_value);
    raw_value.append("hello world");
    std::string key = "key"+std::to_string(x);
    s = sst_file_writer.Put(key, raw_value);
    if (!s.ok()) {
        printf("Error while adding Key: %s, Error: %s\n", key.c_str(),
            s.ToString().c_str());
        return 1;
    }
    }

// Close the file
    s = sst_file_writer.Finish();
    if (!s.ok()) {
        printf("Error while finishing file %s, Error: %s\n", file_path.c_str(),
            s.ToString().c_str());
        return 1;
    }
    return 0;
}