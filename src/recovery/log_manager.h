#pragma once

#include <limits>
#include <mutex>
#include <string>
#include <vector>
#include "common/config.h"
#include "common/types.h"
#include "recovery/log_record.h"

namespace sothdb {

class LogIterator {
 public:
    explicit LogIterator(const std::string& log_file);
    bool HasNext();
    LogRecord Next();

 private:
    std::vector<char> file_data_;
    uint32_t offset_{0};
    uint32_t file_size_{0};
};

class LogManager {
 public:
    explicit LogManager(const std::string& log_file);
    ~LogManager();
    LogManager(const LogManager&) = delete;
    LogManager& operator=(const LogManager&) = delete;
    lsn_t AppendLogRecord(LogRecord& record);
    void Flush(lsn_t up_to_lsn = std::numeric_limits<lsn_t>::max());
    lsn_t GetFlushedLsn() const { return flushed_lsn_; }
    lsn_t GetNextLsn() const { return next_lsn_; }
    LogIterator MakeIterator();

 private:
    void FlushBuffer();
    std::string log_file_;
    int fd_;
    std::vector<char> buffer_;
    uint32_t buffer_offset_{0};
    lsn_t next_lsn_{1};
    lsn_t flushed_lsn_{INVALID_LSN};
    std::mutex mutex_;
};

}  // namespace sothdb
