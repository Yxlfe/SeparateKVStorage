#include "spkvdb_cache.h"

ValueInfo::ValueInfo(Value Value, uint64_t update_frequency, bool valid_flag)
    : value_(Value), update_frequency_(update_frequency), valid_flag_(valid_flag) {}

ValueInfo::ValueInfo(const ValueInfo& other)
    : value_(other.value_), update_frequency_(other.update_frequency_), valid_flag_(other.valid_flag_){}

uint64_t get_key_value_pair_size(const Key& key, const Value& value) {
    return key.size() + value.size();
}

SpkvShard::SpkvShard(uint64_t max_capacity)
    : max_capacity_(max_capacity), current_size_(0), update_count_(0), invalid_count_(0) {
        data_.reserve(200000);
    }

// bool SpkvShard::insert(const Key& key, const ValueInfo& value) {
//     std::unique_lock<std::shared_mutex> lock(mutex_);
//     auto it = data_.find(key);
//     if (it != data_.end()) {
//         uint64_t old_size = get_key_value_pair_size(it->first, it->second.value_);
//         uint64_t new_size = get_key_value_pair_size(key, value.value_);
//         if (current_size_ - old_size + new_size > max_capacity_) {
//             return false;
//         }
//         it->second = value;
//         current_size_ = current_size_ - old_size + new_size;
//         update_count_++;
//         it->second.update_frequency_++;
//         if(max_key_.empty() || key > max_key_)
//         {
//             max_key_ = key;
//         }
//         return true;
//     } else {
//         uint64_t pair_size = get_key_value_pair_size(key, value.value_);
//         if (current_size_ + pair_size > max_capacity_) {
//             return false;
//         }
//         data_[key] = value;
//         if(max_key_.empty() || key > max_key_)
//         {
//             max_key_ = key;
//         }
//         current_size_ += pair_size;
//         return true;
//     }
// }

// bool SpkvShard::insert(Key key, ValueInfo value) {
//     std::unique_lock<std::shared_mutex> lock(mutex_);
//     auto [it, inserted] = data_.emplace(std::move(key), std::move(value));
//     if (!inserted) {
//         // 键已存在，更新值
//         it->second = std::move(value);
//     }
//     // 更新 current_size_、max_key_ 等
//     return true;
// }

bool SpkvShard::insert(const Key& key,const ValueInfo&& value) {

    // Lock mutex

    std::unique_lock<std::shared_mutex> lock(mutex_);


    uint64_t pair_size = get_key_value_pair_size(key, value.value_);
    if (current_size_ + pair_size > max_capacity_) {
        return false;
    }

    // 尝试插入新元素

    auto result = data_.emplace(key, std::move(value));


    if (result.second) {
        // 插入成功，新键
        current_size_ += get_key_value_pair_size(key, result.first->second.value_);
    } else {
        //如果valid_flag_ == false, 证明该键值对已经失效, 之后GC回收的同样key都视为失效不再插入，该次插入视为成功
        if(result.first->second.valid_flag_ == true)
        {// 键已存在，更新值
            uint64_t old_size = get_key_value_pair_size(key, result.first->second.value_);
            uint64_t new_size = get_key_value_pair_size(key, value.value_);
            if((current_size_ + new_size - old_size) > max_capacity_)
            {
                return false;
            }
            current_size_ = current_size_ + new_size - old_size ;
            result.first->second = std::move(value);
            update_count_++;
            result.first->second.update_frequency_++;
        }
    }

    // 更新 max_key_

    if (key > max_key_) {
        max_key_ = key;
    }

    return true;
}

// bool SpkvShard::insert(const Key& key, const ValueInfo&& value) {
//     auto start_time = std::chrono::high_resolution_clock::now();

//     // Timing variables
//     auto lock_time = std::chrono::nanoseconds::zero();
//     auto find_time = std::chrono::nanoseconds::zero();
//     auto size_calc_time = std::chrono::nanoseconds::zero();
//     auto insert_time = std::chrono::nanoseconds::zero();
//     auto update_time = std::chrono::nanoseconds::zero();
//     auto max_key_time = std::chrono::nanoseconds::zero();

//     // Lock mutex
//     auto lock_start = std::chrono::high_resolution_clock::now();
//     std::unique_lock<std::shared_mutex> lock(mutex_);
//     auto lock_end = std::chrono::high_resolution_clock::now();
//     lock_time = lock_end - lock_start;

//     uint64_t pair_size = get_key_value_pair_size(key, value.value_);
//     if (current_size_ + pair_size > max_capacity_) {
//         return false;
//     }

//     // 尝试插入新元素
//     auto insert_start = std::chrono::high_resolution_clock::now();
//     auto result = data_.emplace(key, std::move(value));
//     auto insert_end = std::chrono::high_resolution_clock::now();
//     insert_time += insert_end - insert_start;

//     if (result.second) {
//         // 插入成功，新键
//         current_size_ += get_key_value_pair_size(key, result.first->second.value_);
//     } else {
//         // 键已存在，更新值
//         auto update_start = std::chrono::high_resolution_clock::now();
//         uint64_t old_size = get_key_value_pair_size(key, result.first->second.value_);
//         uint64_t new_size = get_key_value_pair_size(key, value.value_);
//         if((current_size_ - old_size + new_size) > max_capacity_)
//         {
//             return false;
//         }
//         current_size_ = current_size_ - old_size + new_size;
//         result.first->second = std::move(value);
//         update_count_++;
//         result.first->second.update_frequency_++;
//         auto update_end = std::chrono::high_resolution_clock::now();
//         update_time += update_end - update_start;
//     }

//     // 更新 max_key_
//     auto max_key_start = std::chrono::high_resolution_clock::now();
//     if (key > max_key_) {
//         max_key_ = key;
//     }
//     auto max_key_end = std::chrono::high_resolution_clock::now();
//     max_key_time += max_key_end - max_key_start;

//     auto end_time = std::chrono::high_resolution_clock::now();
//     auto total_time = end_time - start_time;


//     // Log timing information
//     log_timing_info("Insert (New Key)", total_time, lock_time, find_time,
//                     size_calc_time, insert_time, max_key_time);

//     return true;
// }

// void SpkvShard::log_timing_info(const std::string& operation, 
//                                 const std::chrono::nanoseconds& total_time,
//                                 const std::chrono::nanoseconds& lock_time,
//                                 const std::chrono::nanoseconds& find_time,
//                                 const std::chrono::nanoseconds& size_calc_time,
//                                 const std::chrono::nanoseconds& insert_or_update_time,
//                                 const std::chrono::nanoseconds& max_key_time) {
//     // You can adjust the logging mechanism as needed
//     std::cout << operation << " - Total Time: " << total_time.count() << " ns\n";
//     std::cout << "  Lock Time: " << lock_time.count() << " ns\n";
//     std::cout << "  Find Time: " << find_time.count() << " ns\n";
//     std::cout << "  Size Calculation Time: " << size_calc_time.count() << " ns\n";
//     std::cout << "  Insert/Update Time: " << insert_or_update_time.count() << " ns\n";
//     std::cout << "  Max Key Update Time: " << max_key_time.count() << " ns\n\n";
// }

bool SpkvShard::updateValidFlag(const Key& key)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it != data_.end() && it->second.valid_flag_ == true) {
        it->second.valid_flag_ = false;
        invalid_count_++;
        return true;
    }
    return false; // 未找到指定的键
}

bool SpkvShard::get(const Key& key, Value& value) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it != data_.end()) {
        value = it->second.value_;
        return true;
    }
    return false;
}

uint64_t SpkvShard::current_size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return current_size_;
}

uint64_t SpkvShard::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return data_.size();
}

uint64_t SpkvShard::get_update_count() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return update_count_.load();
}

void SpkvShard::update_frequency() {
    for (auto it = data_.begin(); it != data_.end(); it++) {
        update_count_.fetch_add(it->second.update_frequency_, std::memory_order_relaxed);
    }
}

Key& SpkvShard::get_max_key()
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return max_key_;
}

double SpkvShard::get_update_frequency() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    uint64_t current_pairs = data_.size();
    if (current_pairs == 0) return 0.0;
    return static_cast<double>(update_count_.load()) / static_cast<double>(current_pairs);
}

double SpkvShard::get_invalid_frequency() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    uint64_t current_pairs = data_.size();
    if (current_pairs == 0) return 0.0;
    return static_cast<double>(invalid_count_.load()) / static_cast<double>(current_pairs);
}

void SpkvShard::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    data_.clear();
    current_size_ = 0;
    update_count_ = 0;
}

std::unordered_map<Key, ValueInfo>& SpkvShard::get_all_data(){
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return data_;
}

SpkvCache::SpkvCache(uint64_t max_cache_size, uint64_t shard_max_size, uint64_t preheat_max_capacity)
    : max_cache_size_(max_cache_size),
      shard_max_size_(shard_max_size),
      preheat_max_capacity_(preheat_max_capacity),
      total_size_(0),
      preheat_finished_(false) {}

void SpkvCache::insert(const Key& key, const Value& value, std::unordered_map<Key, ValueInfo> &result){
    std::unique_lock<std::shared_mutex> lock(shards_mutex_);
    uint64_t pair_size = get_key_value_pair_size(key, value);
    uint64_t current_total_size = total_size_.load(std::memory_order_relaxed);
    if (current_total_size + pair_size > max_cache_size_) {
        evict_shard(result);
    }

    if (!preheat_finished_) {
        handle_preheat_insert(key, value);
    } else {
        handle_post_preheat_insert(key, value);
    }
}

bool SpkvCache::updateInvaildData(const Key& key)
{
    std::unique_lock<std::shared_mutex> lock(shards_mutex_);
    std::shared_ptr<SpkvShard> target_shard = find_shard(key);
    if (target_shard) {
        return target_shard->updateValidFlag(key);
    }
    return false;
}

bool SpkvCache::get(const Key& key, Value& value) const {
    std::shared_ptr<SpkvShard> target_shard = find_shard(key);
    if (target_shard) {
        return target_shard->get(key, value);
    }
    return false;
}

void SpkvCache::print_shards() const {
    std::shared_lock<std::shared_mutex> lock(shards_mutex_);
    std::cout << "\n--- SpkvCache Shards Information ---\n";
    std::cout << "  Total SpkvCache Size = " << total_size_.load(std::memory_order_relaxed) << " bytes\n";
    std::cout << "  Total SpkvCache Capablity: " << max_cache_size_ << " bytes\n";
    int shard_id = 1;
    for (const auto& [max_key, shard] : shard_map_) {
        std::cout << "SpkvShard " << shard_id++ << " [Max Key: " << max_key << "]\n";
        auto& data = shard->get_all_data();
        // for (const auto& pair : data) {
        //     std::cout << "  Key: " << pair.first << ", Value: " << pair.second.value_ << "\n";
        // }
        std::cout << "  Total Size: " << shard->current_size() << " bytes\n";
        std::cout << "  Update Count: " << shard->get_update_count() << "\n";
        std::cout << "  Update Frequency: " << shard->get_update_frequency() << "\n\n";
    }
    std::cout << "--- End of Shards Information ---\n";
}

void SpkvCache::handle_preheat_insert(const Key& key, const Value& value) {
    uint64_t pair_size = get_key_value_pair_size(key, value);

    if (preheat_shard_ == nullptr) {
        preheat_shard_ = std::make_shared<SpkvShard>(preheat_max_capacity_);
    }

    bool inserted = preheat_shard_->insert(key, ValueInfo(value));
    if (inserted) {
        total_size_.fetch_add(pair_size, std::memory_order_relaxed);
    } else {
        preheat();
        handle_post_preheat_insert(key, value);
    }
}

void SpkvCache::handle_post_preheat_insert(const Key& key, const Value& value) {
    std::shared_ptr<SpkvShard> target_shard = find_shard(key);

    if (target_shard) {
        uint64_t pair_size = get_key_value_pair_size(key, value);
        bool inserted = target_shard->insert(key, ValueInfo(value));

        if (inserted) {
            total_size_.fetch_add(pair_size, std::memory_order_relaxed);
            update_shard_map(target_shard, key);
        } else {
            handle_shard_full(target_shard, key, value);
        }
    } else if (!shard_map_.empty()) {
        auto end_shard = shard_map_.rbegin()->second;
        if (end_shard->insert(key, ValueInfo(value))) {
            update_shard_map(end_shard, key);
            total_size_.fetch_add(get_key_value_pair_size(key, value), std::memory_order_relaxed);
        } else {
            auto new_shard = std::make_shared<SpkvShard>(shard_max_size_);
            if (new_shard->insert(key, ValueInfo(value))) {
                shard_map_.emplace(key, new_shard);
                total_size_.fetch_add(get_key_value_pair_size(key, value), std::memory_order_relaxed);
            }
        }
    } else {
        // std::cout << "shard_map_.empty() = " << (shard_map_.empty() ? 1 : 0) << std::endl;
        auto new_shard = std::make_shared<SpkvShard>(shard_max_size_);
        if (new_shard->insert(key, ValueInfo(value))) {
            shard_map_.emplace(key, new_shard);
            total_size_.fetch_add(get_key_value_pair_size(key, value), std::memory_order_relaxed);
        }
    }
}

std::shared_ptr<SpkvShard> SpkvCache::find_shard(const Key& key) const {
    if (shard_map_.empty()) return nullptr;

    auto it = shard_map_.lower_bound(key);
    if (it == shard_map_.end()) {
        return nullptr;
    }
    return it->second;
}

Key& SpkvCache::get_shard_max_key(const std::shared_ptr<SpkvShard>& shard) const {
    auto& max_key = shard->get_max_key();
    // if (max_key.empty()) return "";
    return max_key;
}

void SpkvCache::update_shard_map(const std::shared_ptr<SpkvShard>& shard, const Key& new_key) {
    for (auto it = shard_map_.begin(); it != shard_map_.end(); ++it) {
        if (it->second == shard) {
            shard_map_.erase(it);
            break;
        }
    }
    Key& max_key = get_shard_max_key(shard);
    shard_map_.emplace(max_key, shard);
}

bool SpkvCache::is_key_exist_shard(const std::shared_ptr<SpkvShard>& shard, const Key& key) {
    for (auto it = shard_map_.begin(); it != shard_map_.end(); ++it) {
        if (it->second == shard && it->first >= key) {
            return true;
        }
    }
    return false;
}

void SpkvCache::handle_shard_full(std::shared_ptr<SpkvShard> shard, const Key& key, const Value& value) {
    std::unordered_map<Key, ValueInfo>& data = shard->get_all_data();

    std::vector<std::pair<Key, ValueInfo>> sorted_data(data.begin(), data.end());
    std::sort(sorted_data.begin(), sorted_data.end(),
              [](const std::pair<Key, ValueInfo>& a, const std::pair<Key, ValueInfo>& b) -> bool {
                  return a.first < b.first;
              });

    uint64_t mid = sorted_data.size() / 2;

    std::vector<std::pair<Key, ValueInfo>> shard1_data(sorted_data.begin(), sorted_data.begin() + mid);
    std::vector<std::pair<Key, ValueInfo>> shard2_data(sorted_data.begin() + mid, sorted_data.end());

    auto shard1 = std::make_shared<SpkvShard>(shard_max_size_);
    auto shard2 = std::make_shared<SpkvShard>(shard_max_size_);

    for (const auto& pair : shard1_data) {
        shard1->insert(pair.first, ValueInfo(pair.second));
    }
    for (const auto& pair : shard2_data) {
        shard2->insert(pair.first, ValueInfo(pair.second));
    }

    shard1->update_frequency();
    shard2->update_frequency();

    shard_map_.erase(get_shard_max_key(shard));
    shard_map_.emplace(get_shard_max_key(shard1), shard1);
    shard_map_.emplace(get_shard_max_key(shard2), shard2);

    if (is_key_exist_shard(shard1, key)) {
        if (shard1->insert(key, ValueInfo(value))) {
            update_shard_map(shard1, key);
            uint64_t pair_size = get_key_value_pair_size(key, value);
            total_size_.fetch_add(pair_size, std::memory_order_relaxed);
            // std::cout << "Inserted key \"" << key << "\" into shard1 after splitting.\n";
        }
    } else if (is_key_exist_shard(shard2, key)) {
        if (shard2->insert(key, ValueInfo(value))) {
            update_shard_map(shard2, key);
            uint64_t pair_size = get_key_value_pair_size(key, value);
            total_size_.fetch_add(pair_size, std::memory_order_relaxed);
            // std::cout << "Inserted key \"" << key << "\" into shard2 after splitting.\n";
        }
    } else {
        std::cerr << "Program Failed to insert key \"" << key << "\" into shard1-2 after splitting.\n";
    }
}

void SpkvCache::evict_all_shard(std::unordered_map<Key, ValueInfo> &result)
{
    if (shard_map_.empty()) return;

    // 遍历 shard_map_
    for (auto& it : shard_map_)
    {
        // 获取每个 shard 的数据
        auto& shard_data = it.second->get_all_data();
        for(auto iter = shard_data.begin(); iter != shard_data.end(); iter++)
        {
            if(iter->second.valid_flag_ == true)
            {
                // 将 shard_data 中的有效数据插入到 result 中
                result.emplace(std::move(iter->first), std::move(iter->second.value_));
            }
        }
        
    }
}


void SpkvCache::evict_shard(std::unordered_map<Key, ValueInfo> &result) {
    // std::unique_lock<std::shared_mutex> lock(shards_mutex_);
    if (shard_map_.empty()) return;

    // 找到更新频率最低的分片
    // 找到失效数据比例最高且更新频率最低的分片
    auto it = std::max_element(shard_map_.begin(), shard_map_.end(),
        [](const std::pair<Key, std::shared_ptr<SpkvShard>>& a,
           const std::pair<Key, std::shared_ptr<SpkvShard>>& b) {
            double invalid_f_a = a.second->get_invalid_frequency();
            double invalid_f_b = b.second->get_invalid_frequency();

            if (invalid_f_a != invalid_f_b) {
                return invalid_f_a < invalid_f_b; // 我们想要 invalid_f 最大的分片
            } else {
                double update_f_a = a.second->get_update_frequency();
                double update_f_b = b.second->get_update_frequency();
                return update_f_a > update_f_b; // 在 invalid_f 相等时，选择 update_f 最小的分片
            }
        });

    if (it != shard_map_.end()) {
        uint64_t shard_size = it->second->current_size();
        // std::cout << "Evicting shard with Max Key: \"" << it->first << "\"\n";

        // 打印要被淘汰的 shard 中所有键值对的信息
        auto& temp = it->second->get_all_data();
        auto iter = temp.begin();
        for (; iter != temp.end(); iter++)
        {
            if(iter->second.valid_flag_ == true)
            {
                result.emplace(std::move(iter->first), std::move(iter->second.value_));
            }
        }
        
        // // for (const auto& pair : result) {
        // //     std::cout << "  Evicting Key: " << pair.first << ", Value: " << pair.second.value_ << "\n";
        // // }

        // // 打印被淘汰的分片的更新频率信息
        // // double update_frequency = it->second->get_update_frequency();
        // std::cout << "  Size of Evicted SpkvShard: " << shard_size << "\n";
        // std::cout << "  Update Count: " << it->second->get_update_count() << "\n";
        // std::cout << "  Update Frequency of Evicted SpkvShard: " << it->second->get_update_frequency() << "\n";
        // std::cout << "  Total_size before SpkvShard: " << total_size_ << "\n";
        // 更新总大小并清除分片数据
        total_size_.fetch_sub(shard_size, std::memory_order_relaxed);
        it->second->clear();
        shard_map_.erase(it);
        // std::cout << "  Total_size after SpkvShard: " << total_size_ << "\n";
    }
}

void SpkvCache::preheat() {
    std::cout << "Preheating: Collecting and reorganizing shards...\n";
    if (preheat_finished_) {
        std::cout << "preheat_finished_ abort" << std::endl;
        return;
    }

    std::unordered_map<Key, ValueInfo>& aggregated_data = preheat_shard_->get_all_data();


    uint64_t total_keys = aggregated_data.size();
    auto it = aggregated_data.begin();
    for (uint64_t inserted_keys = 0; inserted_keys < total_keys && it != aggregated_data.end();) {
        auto new_shard = std::make_shared<SpkvShard>(shard_max_size_);
        while (it != aggregated_data.end()) {
            bool inserted = new_shard->insert(it->first, ValueInfo(it->second));
            if (inserted) {
                inserted_keys++;
            } else {
                break;
            }
            ++it;
        }
        Key max_key = get_shard_max_key(new_shard);
        shard_map_.emplace(max_key, new_shard);
    }

    uint64_t aggregated_size = 0;
    for (const auto& pair : aggregated_data) {
        aggregated_size += get_key_value_pair_size(pair.first, pair.second.value_);
    }
    total_size_.store(aggregated_size, std::memory_order_relaxed);

    std::cout << "Preheating completed. Total shards: " << shard_map_.size() << "\n";

    // **新增：打印所有分片的信息**
    std::cout << "\n--- SpkvCache Shards Information After Preheating ---\n";
    std::cout << "  Total SpkvCache Size = " << total_size_.load(std::memory_order_relaxed) << " bytes\n";
    int shard_id = 1;
    for (const auto& [max_key, shard] : shard_map_) {
        std::cout << "SpkvShard " << shard_id++ << " [Max Key: " << max_key << "]\n";
        auto data = shard->get_all_data();
        // for (const auto& pair : data) {
        //     std::cout << "  Key: " << pair.first << ", Value: " << pair.second.value_ << "\n";
        // }
        std::cout << "  Total Size: " << shard->current_size() << " bytes\n";
        std::cout << "  Update Count: " << shard->get_update_count() << "\n";
        std::cout << "  Update Frequency: " << shard->get_update_frequency() << "\n\n";
    }
    std::cout << "--- End of Shards Information After Preheating ---\n";
    
    preheat_shard_->clear();
    preheat_shard_.reset();

    preheat_finished_ = true;
}