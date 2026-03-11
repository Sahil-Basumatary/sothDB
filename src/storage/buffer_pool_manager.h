#pragma once

#include <list>
#include <mutex>
#include <unordered_map>
#include "common/config.h"
#include "common/types.h"
#include "storage/disk_manager.h"
#include "storage/lru_replacer.h"
#include "storage/page.h"

namespace sothdb {

class BufferPoolManager {
 public:
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();
    Page* FetchPage(page_id_t page_id);
    Page* NewPage(page_id_t* page_id);
    bool UnpinPage(page_id_t page_id, bool is_dirty);
    bool FlushPage(page_id_t page_id);
    void FlushAllPages();
    bool DeletePage(page_id_t page_id);
    size_t GetPoolSize() const { return pool_size_; }

 private:
    size_t pool_size_;
    Page* pages_;
    DiskManager* disk_manager_;
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    LRUReplacer replacer_;
    std::list<frame_id_t> free_list_;
    std::mutex latch_;
};

}  // namespace sothdb
