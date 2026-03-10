#pragma once

#include <list>
#include <mutex>
#include <unordered_map>
#include "common/types.h"

namespace sothdb {

class LRUReplacer {
 public:
    explicit LRUReplacer(size_t capacity);
    bool Victim(frame_id_t* frame_id);
    void Pin(frame_id_t frame_id);
    void Unpin(frame_id_t frame_id);
    size_t Size() const;

 private:
    size_t capacity_;
    std::list<frame_id_t> lru_list_;
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map_;
    mutable std::mutex mutex_;
};

}  // namespace sothdb
