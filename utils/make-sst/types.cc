#include "types.h"
#include "storage/redis_metadata.h"

void ComposeNamespaceKey(const Slice &ns, const Slice &key, std::string *ns_key) {
  ns_key->clear();

  PutFixed8(ns_key, static_cast<uint8_t>(ns.size()));
  ns_key->append(ns.data(), ns.size());

  ns_key->append(key.data(), key.size());
}

std::pair<std::string, std::string> makeString(std::string key,std::string value,long expire){
        std::string bytes;
        Metadata metadata(kRedisString, false);
        metadata.expire = expire;
        metadata.Encode(&bytes);
        bytes.append(value.c_str(), value.length());
        std::string raw_key;
        ComposeNamespaceKey("__namespace",key,&raw_key);
        return std::make_pair(raw_key,bytes);
};