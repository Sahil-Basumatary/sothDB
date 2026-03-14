#pragma once

#include <fstream>
#include <mutex>
#include <string>
#include <atomic>
#include "common/config.h"
#include "common/types.h"

namespace sothdb {

class DiskManager {
 public:
    explicit DiskManager(const std::string& db_file);
    ~DiskManager();
    void WritePage(page_id_t page_id, const char* data);
    void ReadPage(page_id_t page_id, char* data);
    page_id_t AllocatePage();
    std::string GetFileName() const { return file_name_; }

 private:
    size_t GetFileSize();
    std::string file_name_;
    std::fstream db_file_;
    std::atomic<page_id_t> next_page_id_{0};
    std::mutex io_mutex_;
};

}  // namespace sothdb
