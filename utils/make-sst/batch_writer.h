#pragma once
#include <utility>
#include <string>
#include <queue>

class BatchWriter
{
private:
    std::priority_queue<std::pair<std::string, std::string>> items;
public:
    BatchWriter(/* args */);
    ~BatchWriter();
    void Write(std::pair<std::string, std::string> item);
    void FlushAll();
};

