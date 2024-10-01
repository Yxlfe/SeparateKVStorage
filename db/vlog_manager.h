#ifndef STORAGE_LEVELDB_DB_VLOG_MANAGER_H_
#define STORAGE_LEVELDB_DB_VLOG_MANAGER_H_

#include <algorithm>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include "db/vlog_reader.h"
#include <set>
#include <atomic>

namespace leveldb {
class VlogManager
{
    public:
        //zc
        //1、VlogInfo考虑加一个指标衡量valid kv分布的离散程度。离散程度 = 有序序列中用最大最小user key的delta / 
        //                                                        有序序列中用最大最小user key的value(offset)的差值
        //2、或者考虑invalid kv offset的累加和
        //上述两种想法的前提都是vlog中kv对有序
        struct VlogInfo{
            log::VReader* vlog_;
            // uint64_t invalid_count_;//代表该vlog文件垃圾kv的数量
            // uint64_t valid_size_;//代表该vlog文件垃圾kv的大小
            uint64_t invalid_size_;//代表该vlog文件垃圾kv的数量
            //有效kv由rewrite产生
            uint64_t valid_count_;//代表该vlog文件有效kv的数量
            uint64_t valid_smallest_key_size_;
            std::string valid_smallest_key_;//代表该vlog文件有效kv的最小值
            uint64_t valid_largest_key_size_;
            std::string valid_largest_key_;//代表该vlog文件有效kv的最大值
            double valid_key_density_;//代表该vlog文件有效kv的范围密度
            // u_char oversize;
        };

        //zc 自定义比较器，按值降序排序,如果值相同按照键升序排序，因此无法保证键的唯一性
        struct CompareByValueDesc {
            template <typename T>
            bool operator()(const T& lhs, const T& rhs) const {
                if (lhs.second != rhs.second) {
                    return lhs.second > rhs.second;  // 降序
                }
                return lhs.first < rhs.first;  // 如果值相等，则按键升序排序
            }
        };

        //zc 自定义比较器，按值降序排序,如果值相同按照键升序排序，可以保证键的唯一性
        // struct CompareByValueDesc {
        //     bool operator()(const std::pair<uint64_t, uint64_t>& lhs,
        //                 const std::pair<uint64_t, uint64_t>& rhs) const {
        //         if (lhs.first == rhs.first) {
        //             return false;  // vlog_numb 相同，视为相等
        //         }
        //         if (lhs.second != rhs.second) {
        //             return lhs.second > rhs.second;  // 按 invalid_size_ 降序排列
        //         }
        //         return lhs.first < rhs.first;  // 如果 invalid_size_ 相等，则按 vlog_numb 升序排列
        //     }
        // };



        VlogManager(uint64_t clean_threshold = 0, uint64_t min_clean_threshold = 0);
        ~VlogManager();

        void AddVlog(uint64_t vlog_numb, log::VReader* vlog);//vlog一定要是new出来的，vlog_manager的析构函数会delete它
        //del
        void RemoveCleaningVlog(uint64_t vlog_numb);
        void RemoveSortedCleaningVlog(uint64_t vlog_numb);

        log::VReader* GetVlog(uint64_t vlog_numb);
        // void AddDropCount(uint64_t vlog_numb);
        void AddDropInfo(uint64_t vlog_numb, uint64_t invalid_size_ = 0);
        void AddValidInfo(uint64_t vlog_numb, std::string current_user_key);
        void AddValidDensity(uint64_t vlog_numb);
        //return vlogNumber
        int64_t searchTargetKeyWithDensity(std::string targetKey);
        void DumpDropCount();
        //del
        bool HasVlogToClean();
        //zc
        bool HasVlogOverSizeToClean();
        bool IsCurrent_GCVlogs_Empty();
        uint64_t GetCurrent_GCVlogsSize();

        // uint64_t GetDropCount(uint64_t vlog_numb){return manager_[vlog_numb].invalid_size_;}
        uint64_t GetDropSize(uint64_t vlog_numb){return manager_[vlog_numb].invalid_size_;}        
        std::set<uint64_t> GetVlogsToClean(uint64_t clean_threshold);
        //del
        uint64_t GetVlogToClean();
        uint64_t GetSortedVlogToClean();
        void SetNowVlog(uint64_t vlog_numb);
        // void SetVlogOverSizeFlag(uint64_t vlog_numb);
        // u_char GetVlogOverSizeFlag(uint64_t vlog_numb);
        // bool Serialize(std::string& val);
        // bool Deserialize(std::string& val);
        bool SerializeVlogMetaData(std::string& val);
        bool DeserializeVlogMetaData(std::string& val);
        bool NeedRecover(uint64_t vlog_numb);
        //zc
        void DumpCurrentSortedCleaningVlogsBySize();
    private:
        std::unordered_map<uint64_t, VlogInfo> manager_;
        std::unordered_set<uint64_t> cleaning_vlog_set_;
        //zc 添加一个新的map来按invalid_size_排序存储vlog编号
        //优化std::set<std::pair<uint64_t, uint64_t>, CompareByValueDesc>的比较器，确保key唯一性，用find代替find_if优化查找性能
        std::set<std::pair<uint64_t, uint64_t>, CompareByValueDesc> sorted_cleaningVlogs_bySize_;
        //zc 当前GC线程回收的vlog编号集合
        std::set<std::pair<uint64_t, uint64_t>, CompareByValueDesc> current_sorted_cleaningVlogs_bySize_;
        uint64_t min_clean_threshold_;
        uint64_t clean_threshold_;
        uint64_t now_vlog_;
        // std::atomic<uint64_t> counter; // 原子计数器
};
}

#endif
