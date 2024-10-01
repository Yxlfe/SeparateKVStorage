#include "db/vlog_reader.h"
#include "db/vlog_manager.h"
#include "util/coding.h"
#include <iostream>
#include <iomanip>

namespace leveldb {

    VlogManager::VlogManager(uint64_t clean_threshold, uint64_t min_clean_threshold):clean_threshold_(clean_threshold),min_clean_threshold_(min_clean_threshold),now_vlog_(0)
    {
    }

    VlogManager::~VlogManager()
    {
        DumpDropCount();
        std::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.begin();
        for(;iter != manager_.end();iter++)
        {
            delete iter->second.vlog_;
        }
    }

    void VlogManager::AddVlog(uint64_t vlog_numb, log::VReader* vlog)
    {
        VlogInfo v;
        v.vlog_ = vlog;
        v.invalid_size_ = 0;
        v.valid_count_ = 0;
        v.valid_smallest_key_size_ = 0;
        v.valid_largest_key_size_ = 0;
        v.valid_key_density_ = 0;
        // v.oversize = 0;

        // v.valid_count_ = 0;
        bool b = manager_.insert(std::make_pair(vlog_numb, v)).second;
        assert(b);
        now_vlog_ = vlog_numb;
    }
//得在单独加一个set nowlog接口,因为dbimpl->recover时最后addDropCount不一定就是now_vlog_
    void VlogManager::SetNowVlog(uint64_t vlog_numb)
    {
        now_vlog_ = vlog_numb;
    }

    // void VlogManager::SetVlogOverSizeFlag(uint64_t vlog_numb)
    // {
    //     manager_[vlog_numb].oversize = 1;
    // }

    // u_char VlogManager::GetVlogOverSizeFlag(uint64_t vlog_numb)
    // {
    //     return manager_[vlog_numb].oversize;
    // }

    void VlogManager::RemoveCleaningVlog(uint64_t vlog_numb)//与GetVlogsToClean对应
    {
        std::unordered_map<uint64_t, VlogInfo>::const_iterator iter = manager_.find (vlog_numb);
        delete iter->second.vlog_;
        manager_.erase(iter);
        cleaning_vlog_set_.erase(vlog_numb);
    }

    //zc
    void VlogManager::RemoveSortedCleaningVlog(uint64_t vlog_numb)
    {
        std::unordered_map<uint64_t, VlogInfo>::const_iterator iter = manager_.find (vlog_numb);
        delete iter->second.vlog_;
        manager_.erase(iter);
        // 查找当前sorted_cleaningVlogs_bySize_键为vlog_numb的元素
        auto it1 = std::find_if(sorted_cleaningVlogs_bySize_.begin(), sorted_cleaningVlogs_bySize_.end(), [vlog_numb](const std::pair<int, int>& elem) {
            return elem.first == vlog_numb;
        });

        // 如果找到了，删除旧的元素
        if (it1 != sorted_cleaningVlogs_bySize_.end()) {
            sorted_cleaningVlogs_bySize_.erase(it1);  // 删除元素
        }

        // 查找当前键为vlog_numb的元素
        auto it2 = std::find_if(current_sorted_cleaningVlogs_bySize_.begin(), current_sorted_cleaningVlogs_bySize_.end(), [vlog_numb](const std::pair<int, int>& elem) {
            return elem.first == vlog_numb;
        });

        // 如果找到了，删除旧的元素
        if (it2 != sorted_cleaningVlogs_bySize_.end()) {
            current_sorted_cleaningVlogs_bySize_.erase(it2);  // 删除元素
        }
    }

//优化std::set<std::pair<uint64_t, uint64_t>, CompareByValueDesc>的比较器，确保key唯一性，用find代替find_if优化查找性能
// void VlogManager::RemoveSortedCleaningVlog(uint64_t vlog_numb) {
//     // 查找并删除 manager_ 中的元素
//     auto iter = manager_.find(vlog_numb);
//     if (iter != manager_.end()) {
//         if (iter->second.vlog_ != nullptr) {
//             delete iter->second.vlog_;
//             iter->second.vlog_ = nullptr;
//         }
//         manager_.erase(iter);
//     } else {
//         // 可以在这里记录日志，表示未找到对应的 vlog_numb
//         std::cerr << "VlogManager::RemoveSortedCleaningVlog: vlog_numb " << vlog_numb << " not found in manager_." << std::endl;
//     }

//     // 构造用于查找的键
//     std::pair<uint64_t, uint64_t> key_to_erase(vlog_numb, 0);

//     // 在 sorted_cleaningVlogs_bySize_ 中查找并删除元素
//     auto it1 = sorted_cleaningVlogs_bySize_.find(key_to_erase);
//     if (it1 != sorted_cleaningVlogs_bySize_.end()) {
//         sorted_cleaningVlogs_bySize_.erase(it1);
//     }

//     // 在 current_sorted_cleaningVlogs_bySize_ 中查找并删除元素
//     auto it2 = current_sorted_cleaningVlogs_bySize_.find(key_to_erase);
//     if (it2 != current_sorted_cleaningVlogs_bySize_.end()) {
//         current_sorted_cleaningVlogs_bySize_.erase(it2);
//     }
// }


void VlogManager::DumpDropCount()
{
    // Iterate through the manager_ map
    for (auto iter = manager_.begin(); iter != manager_.end(); ++iter)
    {
        uint64_t vlogNum = iter->first; // The vlog number (key)
        VlogInfo& info = iter->second;  // The VlogInfo struct (value)

        // Output the necessary fields from VlogInfo
        std::cout << "Vlog Number: " << vlogNum << std::endl;
        std::cout << "Invalid Size: " << info.invalid_size_ << std::endl;
        std::cout << "Valid Count: " << info.valid_count_ << std::endl;
        std::cout << "Valid Smallest Key Size: " << info.valid_smallest_key_size_ << std::endl;
        std::cout << "Valid Smallest Key: " << info.valid_smallest_key_ << std::endl;
        std::cout << "Valid Largest Key Size: " << info.valid_largest_key_size_ << std::endl;
        std::cout << "Valid Largest Key: " << info.valid_largest_key_ << std::endl;
        std::cout << std::scientific << std::setprecision(6) << "Valid_Key_Density: " << info.valid_key_density_ << std::endl;
        std::cout << "----------------------------------------" << std::endl;
    }
}


    // void VlogManager::AddDropCount(uint64_t vlog_numb)
    // {
    //      std::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.find (vlog_numb);
    //      if(iter != manager_.end())
    //      {
    //         iter->second.invalid_count_++;
    //         //zc 这里可以考虑增加对挑选GC vlog文件到cleaning_vlog_set_中的标准的复杂化逻辑
    //         if(iter->second.invalid_count_ >= clean_threshold_ && vlog_numb != now_vlog_)
    //         {
    //             cleaning_vlog_set_.insert(vlog_numb);
    //             // std::cout << "zc cleaning_vlog = " << vlog_numb << std::endl;
    //         }
    //      }//否则说明该vlog已经clean过了
    // }
    
    void VlogManager::AddDropInfo(uint64_t vlog_numb, uint64_t invalid_size)
    {
        auto iter = manager_.find(vlog_numb);
        if (iter != manager_.end()) {
            // 更新VlogInfo中的invalid_size_
            iter->second.invalid_size_ += invalid_size;
            // iter->second.valid_smallest_key_size_ = valid_smallest_key.size();            
            // iter->second.valid_smallest_key_ = valid_smallest_key;
            // iter->second.valid_largest_key_size_ = valid_largest_key.size();    
            // iter->second.valid_largest_key_ = valid_largest_key;
            
            // if(iter->second.invalid_size_ >= clean_threshold_ && vlog_numb != now_vlog_)
            if(vlog_numb != now_vlog_ && iter->second.invalid_size_ > min_clean_threshold_)
            {
                // 查找当前键为vlog_numb的元素
                auto it = std::find_if(sorted_cleaningVlogs_bySize_.begin(), sorted_cleaningVlogs_bySize_.end(), [vlog_numb](const std::pair<int, int>& elem) {
                    return elem.first == vlog_numb;
                });

                // 如果找到了，先删除旧的元素
                if (it != sorted_cleaningVlogs_bySize_.end()) {
                    sorted_cleaningVlogs_bySize_.erase(it);  // 删除元素
                }

                sorted_cleaningVlogs_bySize_.insert(std::make_pair(vlog_numb, iter->second.invalid_size_));
                // std::cout << "VlogManager::AddDropInfo vlog_numb = "
                //           << vlog_numb
                //           << "invalid_size = "
                //           << iter->second.invalid_size_
                //           << std::endl;
             }

        }
    }

//优化std::set<std::pair<uint64_t, uint64_t>, CompareByValueDesc>的比较器，确保key唯一性
// void VlogManager::AddDropInfo(uint64_t vlog_numb, uint64_t invalid_size) {
//     auto iter = manager_.find(vlog_numb);
//     if (iter != manager_.end()) {
//         // 更新 invalid_size_
//         iter->second.invalid_size_ += invalid_size;

//         if (vlog_numb != now_vlog_ && iter->second.invalid_size_ > min_clean_threshold_) {
//             // 构造新元素
//             std::pair<uint64_t, uint64_t> new_elem = std::make_pair(vlog_numb, iter->second.invalid_size_);

//             // 尝试插入新元素
//             auto result = sorted_cleaningVlogs_bySize_.insert(new_elem);

//             if (!result.second) {
//                 // 插入失败，说明已存在相同的 vlog_numb，需先删除旧的元素
//                 sorted_cleaningVlogs_bySize_.erase(result.first);
//                 sorted_cleaningVlogs_bySize_.insert(new_elem);
//             }
//         }
//     }
// }



    void VlogManager::AddValidDensity(uint64_t vlog_numb)
    {
        auto iter = manager_.find(vlog_numb);
        if (iter != manager_.end()) {
            iter->second.valid_key_density_ = static_cast<double>(iter->second.valid_count_) 
                                            / static_cast<double>(countStringsInclusive(iter->second.valid_smallest_key_, iter->second.valid_largest_key_));
            // std::cout << "VlogManager::AddValidInfo add vlog number = " 
            //           << iter->first 
            //           << " ,valid_key_density_ = "
            //           << iter->second.valid_key_density_
            //           << std::endl;        
        }
    }

    int64_t VlogManager::searchTargetKeyWithDensity(std::string targetKey)
    {
        int64_t MaxDensityVlog = -1;
        double MaxDensity = 0;
        for (auto iter = manager_.begin(); iter != manager_.end(); iter++)
        {
            if(targetKey >= iter->second.valid_smallest_key_ && targetKey <= iter->second.valid_largest_key_)
            {
                if(iter->second.valid_key_density_ > MaxDensity)
                {
                    MaxDensity = iter->second.valid_key_density_;
                    MaxDensityVlog = iter->first;
                }
            }
        }

        return  MaxDensityVlog;
    }
    

    void VlogManager::AddValidInfo(uint64_t vlog_numb, std::string current_user_key)
    {
        auto iter = manager_.find(vlog_numb);
        if (iter != manager_.end()) {
            // 更新VlogInfo中的invalid_size_
            iter->second.valid_count_ ++;
            if(iter->second.valid_count_ == 0)
            {
                iter->second.valid_largest_key_ = current_user_key;
                iter->second.valid_smallest_key_ = current_user_key;
            }
            else 
            {
                if(current_user_key > iter->second.valid_largest_key_)
                {
                    iter->second.valid_largest_key_ = current_user_key;
                }
                else
                {
                    iter->second.valid_smallest_key_ = current_user_key;
                }

            }            

            iter->second.valid_smallest_key_size_ = current_user_key.size();
            iter->second.valid_largest_key_size_ = current_user_key.size();
        }
    }



    std::set<uint64_t> VlogManager::GetVlogsToClean(uint64_t clean_threshold)
    {
        std::set<uint64_t> res;
        std::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.begin();
        for(;iter != manager_.end();iter++)
        {
            if(iter->second.invalid_size_ >= clean_threshold && iter->first != now_vlog_)
                res.insert(iter->first);
        }
        return res;
    }

    uint64_t VlogManager::GetVlogToClean()
    {
       std::unordered_set<uint64_t>::iterator iter = cleaning_vlog_set_.begin();
       assert(iter != cleaning_vlog_set_.end());
       return *iter;
    }

    //zc
    uint64_t VlogManager::GetSortedVlogToClean()
    {
       auto iter = current_sorted_cleaningVlogs_bySize_.begin();
       assert(iter != current_sorted_cleaningVlogs_bySize_.end());
       return iter->first;
    }


    log::VReader* VlogManager::GetVlog(uint64_t vlog_numb)
    {
        std::unordered_map<uint64_t, VlogInfo>::const_iterator iter = manager_.find (vlog_numb);
        if(iter == manager_.end())
            return NULL;
        else
            return iter->second.vlog_;
    }

    bool VlogManager::HasVlogToClean()
    {
        return !cleaning_vlog_set_.empty();
    }

    bool VlogManager::IsCurrent_GCVlogs_Empty()
    {
        return !current_sorted_cleaningVlogs_bySize_.empty();
    }

    uint64_t VlogManager::GetCurrent_GCVlogsSize()
    {
        return current_sorted_cleaningVlogs_bySize_.size();
    }

    // Dump 接口，用于打印键值对
    void VlogManager::DumpCurrentSortedCleaningVlogsBySize(){
        std::cout << "----------------------------------------" << std::endl;
        std::cout << "DBImpl::BackgroundClean GCVlogsSize = "
        << GetCurrent_GCVlogsSize()
        << std::endl;
        uint64_t totalInvalidSize = 0;
        for (const auto& pair : current_sorted_cleaningVlogs_bySize_) {
            std::cout << "VlogNumber: " << pair.first << ", Total invalidSize: " << pair.second << std::endl;
            totalInvalidSize += pair.second;
        }
        std::cout << "DBImpl::BackgroundClean GCTotalSize = " 
                  << totalInvalidSize / 1024 / 1024 / 1024 
                  << " GB"
                  << std::endl;
    }

    bool VlogManager::HasVlogOverSizeToClean()
    {
        uint64_t sum = 0;
        bool found = false; // 用于标识是否找到超过阈值的情况

        for (const auto& element : sorted_cleaningVlogs_bySize_) {
            current_sorted_cleaningVlogs_bySize_.insert(element);
            sum += element.second;
            // std::cout << "Vlog Number, invalid size: (" << element.first << ", " << element.second << "), Current sum: " << sum << std::endl;
            if (sum > clean_threshold_) {
                // DumpCurrentSortedCleaningVlogsBySize();
                found = true; 
                break;
            }
        }

        if (!found) {
            current_sorted_cleaningVlogs_bySize_.clear();
        }

        return found;
    }

    // bool VlogManager::Serialize(std::string& val)
    // {
    //     val.clear();
    //     uint64_t size = manager_.size();
    //     if(size == 0)
    //         return false;

    //     std::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.begin();
    //     for(;iter != manager_.end();iter++)
    //     {
    //         char buf[8];
    //         EncodeFixed64(buf, (iter->second.invalid_count_ << 16) | iter->first);
    //         val.append(buf, 8);
    //     }
    //     return true;
    // }
    
    //zc modify
    bool VlogManager::SerializeVlogMetaData(std::string& val)
    {
        val.clear();
        uint64_t size = manager_.size();
        if(size == 0)
            return false;

        std::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.begin();
        for(;iter != manager_.end();iter++)
        {
            uint64_t size = 48 + iter->second.valid_smallest_key_size_ + iter->second.valid_largest_key_size_;
            uint64_t buf_offset = 0; 
            char buf[size];
            EncodeFixed64(buf + buf_offset, iter->first); 
            buf_offset += 8;
            EncodeFixed64(buf + buf_offset, iter->second.invalid_size_);
            buf_offset += 8;
            EncodeFixed64(buf + buf_offset, iter->second.valid_count_);
            buf_offset += 8;
            EncodeFixed64(buf + buf_offset, iter->second.valid_smallest_key_size_);
            buf_offset += 8;
            memcpy(buf + buf_offset, iter->second.valid_smallest_key_.data(), iter->second.valid_smallest_key_size_);
            buf_offset += iter->second.valid_smallest_key_size_;
            EncodeFixed64(buf + buf_offset, iter->second.valid_largest_key_size_);
            buf_offset += 8;
            memcpy(buf + buf_offset, iter->second.valid_largest_key_.data(), iter->second.valid_largest_key_size_);
            buf_offset += iter->second.valid_largest_key_size_;
            memcpy(buf + buf_offset, &(iter->second.valid_key_density_), sizeof(iter->second.valid_key_density_));
            val.append(buf, size);
        }
        return true;
    }

    bool VlogManager::DeserializeVlogMetaData(std::string& val)
    {
        Slice input(val);
        while(!input.empty())
        {
            uint64_t buf_offset = 0; 

            uint64_t vlog_numb = DecodeFixed64(input.data() + buf_offset); // Decode vlog number
            buf_offset += 8;

            uint64_t invalid_size = DecodeFixed64(input.data() + buf_offset); // Decode invalid_size
            buf_offset += 8;

            uint64_t valid_count = DecodeFixed64(input.data() + buf_offset); // Decode valid_size
            buf_offset += 8;

            uint64_t valid_smallest_key_size = DecodeFixed64(input.data() + buf_offset); // Decode valid_smallest_key_size
            buf_offset += 8;

            std::string valid_smallest_key;
            if (valid_smallest_key_size != 0)
            {
                valid_smallest_key = std::string(input.data() + buf_offset, valid_smallest_key_size);
                buf_offset += valid_smallest_key_size;
            }

            uint64_t valid_largest_key_size = DecodeFixed64(input.data() + buf_offset); // Decode valid_largest_key_size
            buf_offset += 8;

            std::string valid_largest_key;
            if (valid_largest_key_size != 0)
            {
                valid_largest_key = std::string(input.data() + buf_offset, valid_largest_key_size);
                buf_offset += valid_largest_key_size;
            }

            double valid_key_density = 0;
            memcpy(&valid_key_density, input.data() + buf_offset, sizeof(valid_key_density)); // Decode valid_key_density
            buf_offset += 8;

            if(manager_.count(vlog_numb) > 0)//检查manager_现在是否还有该vlog，因为有可能已经删除了
            {
                manager_[vlog_numb].invalid_size_ = invalid_size;
                manager_[vlog_numb].valid_count_ = valid_count;
                manager_[vlog_numb].valid_smallest_key_size_ = valid_smallest_key_size;
                manager_[vlog_numb].valid_smallest_key_ = valid_smallest_key;
                manager_[vlog_numb].valid_largest_key_size_ = valid_largest_key_size;
                manager_[vlog_numb].valid_largest_key_ = valid_largest_key;
                manager_[vlog_numb].valid_key_density_ = valid_key_density;
                if(invalid_size >= clean_threshold_ && vlog_numb != now_vlog_)
                {
                    // cleaning_vlog_set_.insert(vlog_numb);
                    // 更新排序map

                    // 首先删除旧的键值对（如果存在）
                    // 查找当前键为vlog_numb的元素
                    auto it = std::find_if(sorted_cleaningVlogs_bySize_.begin(), sorted_cleaningVlogs_bySize_.end(), [vlog_numb](const std::pair<int, int>& elem) {
                        return elem.first == vlog_numb;
                    });

                    // 如果找到了，先删除旧的元素
                    if (it != sorted_cleaningVlogs_bySize_.end()) {
                        sorted_cleaningVlogs_bySize_.erase(it);  // 删除元素
                    }

                    // 添加新的键值对
                    sorted_cleaningVlogs_bySize_.insert(std::make_pair(vlog_numb, invalid_size));
                }
            }
            input.remove_prefix(buf_offset);
        }
        return true;
    }

//优化std::set<std::pair<uint64_t, uint64_t>, CompareByValueDesc>的比较器，确保key唯一性，用find代替find_if优化查找性能
// bool VlogManager::DeserializeVlogMetaData(std::string& val)
// {
//     Slice input(val);
//     while(!input.empty())
//     {
//         uint64_t buf_offset = 0; 

//         uint64_t vlog_numb = DecodeFixed64(input.data() + buf_offset); // Decode vlog number
//         buf_offset += 8;

//         uint64_t invalid_size = DecodeFixed64(input.data() + buf_offset); // Decode invalid_size
//         buf_offset += 8;

//         uint64_t valid_count = DecodeFixed64(input.data() + buf_offset); // Decode valid_count
//         buf_offset += 8;

//         uint64_t valid_smallest_key_size = DecodeFixed64(input.data() + buf_offset); // Decode valid_smallest_key_size
//         buf_offset += 8;

//         std::string valid_smallest_key;
//         if (valid_smallest_key_size != 0)
//         {
//             valid_smallest_key = std::string(input.data() + buf_offset, valid_smallest_key_size);
//             buf_offset += valid_smallest_key_size;
//         }

//         uint64_t valid_largest_key_size = DecodeFixed64(input.data() + buf_offset); // Decode valid_largest_key_size
//         buf_offset += 8;

//         std::string valid_largest_key;
//         if (valid_largest_key_size != 0)
//         {
//             valid_largest_key = std::string(input.data() + buf_offset, valid_largest_key_size);
//             buf_offset += valid_largest_key_size;
//         }

//         double valid_key_density = 0;
//         memcpy(&valid_key_density, input.data() + buf_offset, sizeof(valid_key_density)); // Decode valid_key_density
//         buf_offset += 8;

//         if(manager_.count(vlog_numb) > 0) // 检查 manager_ 中是否存在该 vlog_numb
//         {
//             manager_[vlog_numb].invalid_size_ = invalid_size;
//             manager_[vlog_numb].valid_count_ = valid_count;
//             manager_[vlog_numb].valid_smallest_key_size_ = valid_smallest_key_size;
//             manager_[vlog_numb].valid_smallest_key_ = valid_smallest_key;
//             manager_[vlog_numb].valid_largest_key_size_ = valid_largest_key_size;
//             manager_[vlog_numb].valid_largest_key_ = valid_largest_key;
//             manager_[vlog_numb].valid_key_density_ = valid_key_density;

//             if(invalid_size >= clean_threshold_ && vlog_numb != now_vlog_)
//             {
//                 // 构造用于查找的键
//                 std::pair<uint64_t, uint64_t> key_to_erase(vlog_numb, 0);

//                 // 在 sorted_cleaningVlogs_bySize_ 中查找并删除旧的元素
//                 auto it = sorted_cleaningVlogs_bySize_.find(key_to_erase);
//                 if (it != sorted_cleaningVlogs_bySize_.end()) {
//                     sorted_cleaningVlogs_bySize_.erase(it);
//                 }

//                 // 插入新的键值对
//                 sorted_cleaningVlogs_bySize_.insert(std::make_pair(vlog_numb, invalid_size));
//             }
//         }
//         input.remove_prefix(buf_offset);
//     }
//     return true;
// }


    // bool VlogManager::Deserialize(std::string& val)
    // {
    //     Slice input(val);
    //     while(!input.empty())
    //     {
    //         uint64_t code = DecodeFixed64(input.data());
    //         uint64_t file_numb = code & 0xffff;
    //         size_t count = code>>16;
    //         if(manager_.count(file_numb) > 0)//检查manager_现在是否还有该vlog，因为有可能已经删除了
    //         {
    //             manager_[file_numb].invalid_count_ = count;
    //             if(count >= clean_threshold_ && file_numb != now_vlog_)
    //             {
    //                 cleaning_vlog_set_.insert(file_numb);
    //             }
    //         }
    //         input.remove_prefix(8);
    //     }
    //     return true;
    // }

    bool VlogManager::NeedRecover(uint64_t vlog_numb)
    {
        std::unordered_map<uint64_t, VlogInfo>::iterator iter = manager_.find(vlog_numb);
        if(iter != manager_.end())
        {
            assert(iter->second.invalid_size_ >= clean_threshold_);
            return true;
        }
        else
            return false;//不需要recoverclean,即没有清理一半的vlog
    }

}
