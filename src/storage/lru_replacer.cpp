#include "storage/lru_replacer.h"

namespace sothdb {

LRUReplacer::LRUReplacer(size_t capacity) : capacity_(capacity) {}

bool LRUReplacer::Victim(frame_id_t* frame_id) {
    std::scoped_lock lock(mutex_);
    if (lru_list_.empty()) {
        return false;
    }
    *frame_id = lru_list_.back();
    lru_map_.erase(*frame_id);
    lru_list_.pop_back();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock lock(mutex_);
    auto it = lru_map_.find(frame_id);
    if (it == lru_map_.end()) {
        return;
    }
    lru_list_.erase(it->second);
    lru_map_.erase(it);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock lock(mutex_);
    if (lru_map_.count(frame_id) > 0) {
        return;
    }
    if (lru_list_.size() >= capacity_) {
        auto evicted = lru_list_.back();
        lru_list_.pop_back();
        lru_map_.erase(evicted);
    }
    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() const {
    std::scoped_lock lock(mutex_);
    return lru_list_.size();
}

}  // namespace sothdb
