#ifndef SPKVDB_CACHE_H
#define SPKVDB_CACHE_H

#include <iostream>
#include <map>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <memory>
#include <unordered_map>
#include <limits>
#include <chrono>


using Key = std::string;
using Value = std::string;

struct ValueInfo
{
    Value value_;
    uint64_t update_frequency_;
    bool valid_flag_;
    ValueInfo(Value Value = "", uint64_t update_frequency = 0, bool valid_flag = true);
    ValueInfo(const ValueInfo& other);
};

size_t get_key_value_pair_size(const Key& key, const Value& value);

class SpkvShard {
public:
    SpkvShard(size_t max_capacity);
    bool insert(const Key& key,const ValueInfo&& value);
    bool updateValidFlag(const Key& key);
    bool get(const Key& key, Value& value) const;
    size_t current_size() const;
    size_t size() const;
    size_t get_update_count() const;
    void update_frequency();
    double get_update_frequency() const;
    double get_invalid_frequency() const;
    
    void clear();
    Key& get_max_key();
    std::unordered_map<Key, ValueInfo>& get_all_data();
    // void log_timing_info(const std::string& operation, 
    //                             const std::chrono::nanoseconds& total_time,
    //                             const std::chrono::nanoseconds& lock_time,
    //                             const std::chrono::nanoseconds& find_time,
    //                             const std::chrono::nanoseconds& size_calc_time,
    //                             const std::chrono::nanoseconds& insert_or_update_time,
    //                             const std::chrono::nanoseconds& max_key_time);

private:
    size_t max_capacity_;
    uint64_t current_size_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<Key, ValueInfo> data_;
    std::atomic<uint64_t> update_count_;
    std::atomic<uint64_t> invalid_count_;
    Key max_key_;
};

class SpkvCache {
public:
    SpkvCache(size_t max_cache_size, size_t shard_max_size, size_t preheat_max_capacity);
    void insert(const Key& key, const Value& value, std::unordered_map<Key, ValueInfo> &result);
    bool get(const Key& key, Value& value) const;
    bool updateInvaildData(const Key& key);
    void evict_all_shard(std::unordered_map<Key, ValueInfo> &result);
    void print_shards() const;

private:
    void handle_preheat_insert(const Key& key, const Value& value);
    void handle_post_preheat_insert(const Key& key, const Value& value);
    std::shared_ptr<SpkvShard> find_shard(const Key& key) const;
    Key& get_shard_max_key(const std::shared_ptr<SpkvShard>& shard) const;
    void update_shard_map(const std::shared_ptr<SpkvShard>& shard, const Key& new_key);
    bool is_key_exist_shard(const std::shared_ptr<SpkvShard>& shard, const Key& key);
    void handle_shard_full(std::shared_ptr<SpkvShard> shard, const Key& key, const Value& value);
    void evict_shard(std::unordered_map<Key, ValueInfo> &result);
    void preheat();

    std::shared_ptr<SpkvShard> preheat_shard_;
    std::map<Key, std::shared_ptr<SpkvShard>> shard_map_;
    mutable std::shared_mutex shards_mutex_;
    size_t max_cache_size_;
    size_t shard_max_size_;
    size_t preheat_max_capacity_;
    std::atomic<size_t> total_size_;
    std::atomic<bool> preheat_finished_;
};

#endif // SPKVDB_CACHE_H