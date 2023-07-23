#pragma once
#include <utility>
#include <string>
#include <queue>


struct CompareFirstElement {
  bool operator()(const std::pair<std::string, std::string>& lhs, const std::pair<std::string, std::string>& rhs) const {
    return lhs.first > rhs.first;
  }
};

class BatchWriter
{
private:
    std::priority_queue<std::pair<std::string, std::string>, 
    std::vector<std::pair<std::string, std::string>>, 
    CompareFirstElement> items_;
public:
    void Write(std::pair<std::string, std::string> item);
    void FlushAll();
};

