// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4<<20),
      max_open_files(1000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(2<<20),
      compression(kSnappyCompression),
      reuse_logs(false),
      filter_policy(NULL),
      clean_write_buffer_size(12),//垃圾回收的写缓冲区，必须要大于12
     // clean_write_buffer_size(12),
      clean_threshold(1024 * 1024 * 1024),//vlog文件垃圾记录总大小达到clean_threshold时开始进行垃圾回收
     // clean_threshold(1*124 * 1024),
      min_clean_threshold(clean_threshold/10),//zc vlog进行清理时，只有文件失效数据大小达到min_clean_threshold才会被清理
      // log_dropCount_threshold(100),//与持久化vloginfo有关的，合并后新产生log_dropCount_threshold条垃圾记录时持久化vloginfo
      log_dropSize_threshold(1024),//与持久化vloginfo有关的，合并后新产生垃圾记录的总大小大于log_dropSize_threshold时持久化vloginfo
      max_vlog_size(1024*1024*1024),
      preload_vlog_size(512*1024*1024){//vlog文件大小上限值
 //     max_vlog_size(124*1024*1024){
}

}  // namespace leveldb
