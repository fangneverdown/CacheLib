/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cachelib/cachebench/runner/FastShutdown.h"

#include <folly/logging/xlog.h>

#include <cstddef>
#include <cstring>
#include <iostream>
#include "cachelib/allocator/Util.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

FastShutdownStressor::FastShutdownStressor(const CacheConfig& cacheConfig,
                                           uint64_t numOps)
    : numOps_(numOps),
      cacheDir_{folly::sformat("/tmp/cache_bench_fss_{}", getpid())},
      cache_(std::make_unique<Cache<LruAllocator>>(
          cacheConfig, nullptr, cacheDir_)) {}

void FastShutdownStressor::start() {
  startTime_ = std::chrono::system_clock::now();
  constexpr uint32_t kSlabSize = 4 * 1024 * 1024;
 constexpr uint32_t kSliceSize = 32*1024;
  uint32_t nslabs = cache_->getCacheSize() / kSlabSize;
  uint32_t numSmallAllocs = kSlabSize / kSliceSize;
  using CacheType = Cache<LruAllocator>;
  uint64_t expectedAbortCount = 0;

  // Test with wait time 6 seconds first time and wait time 30 seconds second
  // time to interrupt slab release at different places.
  uint32_t waitTime = 6;
  std::vector<CacheType::WriteHandle> v;
  char data_from[kSliceSize];
  for (size_t i = 0; i < kSliceSize - 1; ++i) {
      data_from[i] = 'A' + i%25;
  }
  memset(data_from, 1, kSliceSize);
  std::cout << "allocating....\n";
  for (size_t i = 0; i < nslabs; i++) {
        for (size_t j = 0; j < numSmallAllocs; j++) {
          auto it = cache_->allocate(
              static_cast<uint8_t>(0),
              folly::sformat("key_{}", i * numSmallAllocs + j), kSliceSize);
          if (it) {
              memcpy(it->getMemory(), data_from, kSliceSize);
            cache_->insertOrReplace(it);
            // v.push_back(std::move(it));
          }
        }
        // ops_.fetch_add(numSmallAllocs, std::memory_order_relaxed);
      }
    
  // for (; waitTime <= numOps_ * 3; waitTime += 3) {
  for(size_t tested_times =0;tested_times<3;tested_times++){
    std::cout << "write num: " <<nslabs*numSmallAllocs << "\n";
    auto shutDownStartTime = std::chrono::system_clock::now();
    std::cout << "Shutting Down...\n";
    cache_->shutDown();
    // testThread_.join();
    endTime_ = std::chrono::system_clock::now();
    auto shutdown_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        endTime_ - shutDownStartTime);

    std::cout << "Shut down durtaion " << shutdown_duration.count() << "\n";
    std::cout << "Reattaching to cache...\n";
    cache_->reAttach();
      auto reAttachTime = std::chrono::system_clock::now();
    auto restart_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        reAttachTime - endTime_);
    std::cout << "restart durtaion " << restart_duration.count() << "\n";
  }

  //fslcheck 
  sleep(10000000);
  size_t check_count = 0;
  for (size_t i = 0; i < nslabs; i++) {
    for (size_t j = 0; j < numSmallAllocs; j++) {

      auto it = cache_->find(folly::sformat("key_{}", i * numSmallAllocs + j) );
      if(it){
        assert(memcmp(it->getMemory(), data_from, kSliceSize) == 0);
        check_count++;
      }
    }
    // ops_.fetch_add(numSmallAllocs, std::memory_order_relaxed);
  }
  std::cout << "read num: " <<check_count << "\n";
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
