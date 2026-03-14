#include "storage/disk_manager.h"
#include <cstring>
#include <stdexcept>

namespace sothdb {

DiskManager::DiskManager(const std::string& db_file) : file_name_(db_file) {
    db_file_.open(db_file, std::ios::in | std::ios::out | std::ios::binary);
    if (!db_file_.is_open()) {
        db_file_.clear();
        // file doesn't exist yet, create it
        db_file_.open(db_file, std::ios::out | std::ios::binary);
        db_file_.close();
        db_file_.open(db_file, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!db_file_.is_open()) {
        throw std::runtime_error("Failed to open database file: " + db_file);
    }
    auto file_sz = GetFileSize();
    next_page_id_.store(static_cast<page_id_t>(file_sz / PAGE_SIZE));
}

DiskManager::~DiskManager() {
    if (db_file_.is_open()) {
        db_file_.close();
    }
}

void DiskManager::WritePage(page_id_t page_id, const char* data) {
    std::scoped_lock lock(io_mutex_);
    auto offset = static_cast<std::streamoff>(page_id) * PAGE_SIZE;
    db_file_.seekp(offset);
    db_file_.write(data, PAGE_SIZE);
    if (db_file_.bad()) {
        throw std::runtime_error("WritePage failed for page " + std::to_string(page_id));
    }
    db_file_.flush();
}

void DiskManager::ReadPage(page_id_t page_id, char* data) {
    std::scoped_lock lock(io_mutex_);
    auto offset = static_cast<std::streamoff>(page_id) * PAGE_SIZE;
    auto file_sz = GetFileSize();
    if (static_cast<size_t>(offset) >= file_sz) {
        std::memset(data, 0, PAGE_SIZE);
        return;
    }
    db_file_.seekg(offset);
    db_file_.read(data, PAGE_SIZE);
    if (db_file_.bad()) {
        throw std::runtime_error("ReadPage failed for page " + std::to_string(page_id));
    }
    auto bytes_read = db_file_.gcount();
    if (bytes_read < static_cast<std::streamsize>(PAGE_SIZE)) {
        std::memset(data + bytes_read, 0, PAGE_SIZE - bytes_read);
    }
}

page_id_t DiskManager::AllocatePage() {
    return next_page_id_.fetch_add(1);
}

size_t DiskManager::GetFileSize() {
    db_file_.seekg(0, std::ios::end);
    return static_cast<size_t>(db_file_.tellg());
}

}  // namespace sothdb
