#include "storage/buffer_pool_manager.h"

namespace sothdb {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size),
      disk_manager_(disk_manager),
      replacer_(pool_size) {
    pages_ = new Page[pool_size];
    for (size_t i = 0; i < pool_size; ++i) {
        free_list_.push_back(static_cast<frame_id_t>(i));
    }
}

BufferPoolManager::~BufferPoolManager() {
    FlushAllPages();
    delete[] pages_;
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::scoped_lock lock(latch_);
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        auto frame_id = it->second;
        auto& page = pages_[frame_id];
        page.IncrementPinCount();
        replacer_.Pin(frame_id);
        return &page;
    }
    frame_id_t victim_frame;
    if (!free_list_.empty()) {
        victim_frame = free_list_.front();
        free_list_.pop_front();
    } else if (replacer_.Victim(&victim_frame)) {
        auto& old_page = pages_[victim_frame];
        if (old_page.IsDirty()) {
            disk_manager_->WritePage(old_page.GetPageId(), old_page.GetData());
        }
        page_table_.erase(old_page.GetPageId());
    } else {
        return nullptr;
    }
    auto& new_page = pages_[victim_frame];
    new_page.Reset();
    disk_manager_->ReadPage(page_id, new_page.GetData());
    new_page.SetPageId(page_id);
    new_page.IncrementPinCount();
    page_table_[page_id] = victim_frame;
    return &new_page;
}

Page* BufferPoolManager::NewPage(page_id_t* page_id) {
    std::scoped_lock lock(latch_);
    frame_id_t frame;
    if (!free_list_.empty()) {
        frame = free_list_.front();
        free_list_.pop_front();
    } else if (replacer_.Victim(&frame)) {
        auto& old_page = pages_[frame];
        if (old_page.IsDirty()) {
            disk_manager_->WritePage(old_page.GetPageId(), old_page.GetData());
        }
        page_table_.erase(old_page.GetPageId());
    } else {
        return nullptr;
    }
    auto new_pid = disk_manager_->AllocatePage();
    auto& page = pages_[frame];
    page.Reset();
    page.SetPageId(new_pid);
    page.IncrementPinCount();
    page.SetDirty(true);
    page_table_[new_pid] = frame;
    *page_id = new_pid;
    return &page;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::scoped_lock lock(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    auto frame_id = it->second;
    auto& page = pages_[frame_id];
    if (page.GetPinCount() <= 0) {
        return false;
    }
    page.DecrementPinCount();
    if (is_dirty) {
        page.SetDirty(true);
    }
    if (page.GetPinCount() == 0) {
        replacer_.Unpin(frame_id);
    }
    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::scoped_lock lock(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }
    auto& page = pages_[it->second];
    disk_manager_->WritePage(page_id, page.GetData());
    page.SetDirty(false);
    return true;
}

void BufferPoolManager::FlushAllPages() {
    // TODO: should probably hold latch_ here
    for (auto& [pid, fid] : page_table_) {
        auto& page = pages_[fid];
        if (page.IsDirty()) {
            disk_manager_->WritePage(pid, page.GetData());
            page.SetDirty(false);
        }
    }
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::scoped_lock lock(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return true;
    }
    auto frame_id = it->second;
    auto& page = pages_[frame_id];
    if (page.GetPinCount() > 0) {
        return false;
    }
    page_table_.erase(it);
    replacer_.Pin(frame_id);
    page.Reset();
    free_list_.push_back(frame_id);
    return true;
}

}  // namespace sothdb
