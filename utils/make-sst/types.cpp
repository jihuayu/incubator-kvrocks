#include "types.h"
#include "storage/redis_metadata.h"

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