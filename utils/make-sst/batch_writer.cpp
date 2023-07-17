#include "batch_writer.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "storage/redis_metadata.h"

BatchWriter::BatchWriter(/* args */)
{
}

BatchWriter::~BatchWriter()
{
}

void BatchWriter::Write(std::pair<std::string, std::string> item)
{
    items.emplace(item);
}

void BatchWriter::FlushAll()
{
    Options options;
    SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);
    std::string file_path = "./file1.sst";
    auto s = sst_file_writer.Open(file_path);
    while (!items.empty()) {
        auto i = items.top();
        sst_file_writer.Put(i.first,i.second);
        items.pop();
    }

     s = sst_file_writer.Finish();
    if (!s.ok()) {
        printf("Error while finishing file %s, Error: %s\n", file_path.c_str(),
            s.ToString().c_str());
    }
}
