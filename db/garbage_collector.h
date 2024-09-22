
#ifndef STORAGE_LEVELDB_DB_GARBAGE_COLLECTOR_H_
#define STORAGE_LEVELDB_DB_GARBAGE_COLLECTOR_H_

#include "stdint.h"
#include "db/vlog_reader.h"
#include <unordered_map>

namespace leveldb{
class VReader;
class DBImpl;
class VersionEdit;

class GarbageCollector
{
    public:
        GarbageCollector(DBImpl* db):vlog_number_(0), garbage_pos_(0), vlog_reader_(NULL), db_(db){}
        ~GarbageCollector(){delete vlog_reader_;}
        void SetVlog(uint64_t vlog_number, uint64_t garbage_beg_pos=0);
        void BeginGarbageCollect(VersionEdit* edit, bool* save_edit, uint64_t& read_size, uint64_t& rewrite_size, std::string rewriteSmallestKey = "", std::string rewritelargestKey = "");
        void SetRewriteKeyRange(std::string rewriteSmallestKey, std::string rewritelargestKey);
        struct rewriteKeyInfo{
            uint64_t rewriteCount;//代表该vlog文件重写kv的数量
            uint64_t rewriteSmallestKeySize;
            std::string rewriteSmallestKey;//代表该vlog文件中rewrite kv的最小值
            uint64_t rewritelargestKeySize;
            std::string rewritelargestKey;//代表该vlog文件中rewrite kv的最大值
        };
    private:
        uint64_t vlog_number_;
        uint64_t garbage_pos_;//vlog文件起始垃圾回收的地方
        log::VReader* vlog_reader_;
        DBImpl* db_;
        std::unordered_map<uint64_t, rewriteKeyInfo> rewriteKeyRange;

};

}

#endif
