#include "recovery/log_manager.h"
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>

namespace sothdb {

// --- LogIterator ---

LogIterator::LogIterator(const std::string& log_file) {
    int fd = open(log_file.c_str(), O_RDONLY);
    if (fd < 0) return;
    auto size = lseek(fd, 0, SEEK_END);
    if (size <= 0) {
        close(fd);
        return;
    }
    file_size_ = static_cast<uint32_t>(size);
    file_data_.resize(file_size_);
    lseek(fd, 0, SEEK_SET);
    auto bytes_read = read(fd, file_data_.data(), file_size_);
    close(fd);
    if (bytes_read != static_cast<ssize_t>(file_size_)) {
        throw std::runtime_error("Failed to read WAL file for iteration");
    }
}

bool LogIterator::HasNext() {
    if (offset_ + sizeof(uint32_t) > file_size_) return false;
    uint32_t record_size;
    std::memcpy(&record_size, file_data_.data() + offset_, sizeof(record_size));
    // Validates full record is present — handles partial writes from crashes
    return record_size >= LOG_HEADER_SIZE && offset_ + record_size <= file_size_;
}

LogRecord LogIterator::Next() {
    uint32_t record_size;
    std::memcpy(&record_size, file_data_.data() + offset_, sizeof(record_size));
    auto record = LogRecord::Deserialize(file_data_.data() + offset_, record_size);
    offset_ += record_size;
    return record;
}

// --- LogManager ---

LogManager::LogManager(const std::string& log_file)
    : log_file_(log_file), buffer_(LOG_BUFFER_SIZE, 0) {
    fd_ = open(log_file.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open WAL file: " + log_file);
    }
    auto file_size = lseek(fd_, 0, SEEK_END);
    if (file_size > 0) {
        LogIterator iter(log_file);
        while (iter.HasNext()) {
            auto rec = iter.Next();
            if (rec.GetLsn() >= next_lsn_) {
                next_lsn_ = rec.GetLsn() + 1;
            }
        }
        flushed_lsn_ = next_lsn_ - 1;
    }
}

LogManager::~LogManager() {
    if (buffer_offset_ > 0) {
        FlushBuffer();
    }
    if (fd_ >= 0) {
        close(fd_);
    }
}

lsn_t LogManager::AppendLogRecord(LogRecord& record) {
    std::scoped_lock lock(mutex_);
    lsn_t lsn = next_lsn_++;
    record.SetLsn(lsn);
    auto data = record.Serialize();
    if (buffer_offset_ + data.size() > LOG_BUFFER_SIZE) {
        FlushBuffer();
    }
    // Record larger than the entire buffer — bypass and write directly
    if (data.size() > LOG_BUFFER_SIZE) {
        auto written = write(fd_, data.data(), data.size());
        if (written != static_cast<ssize_t>(data.size())) {
            throw std::runtime_error("Failed to write oversized log record");
        }
        fsync(fd_);
        flushed_lsn_ = lsn;
        return lsn;
    }
    std::memcpy(buffer_.data() + buffer_offset_, data.data(), data.size());
    buffer_offset_ += static_cast<uint32_t>(data.size());
    return lsn;
}

void LogManager::Flush(lsn_t up_to_lsn) {
    std::scoped_lock lock(mutex_);
    if (buffer_offset_ > 0 && flushed_lsn_ < up_to_lsn) {
        FlushBuffer();
    }
}

void LogManager::FlushBuffer() {
    if (buffer_offset_ == 0) return;
    auto written = write(fd_, buffer_.data(), buffer_offset_);
    if (written != static_cast<ssize_t>(buffer_offset_)) {
        throw std::runtime_error("Failed to flush WAL buffer to disk");
    }
    fsync(fd_);
    flushed_lsn_ = next_lsn_ - 1;
    buffer_offset_ = 0;
}

LogIterator LogManager::MakeIterator() {
    std::scoped_lock lock(mutex_);
    if (buffer_offset_ > 0) {
        FlushBuffer();
    }
    return LogIterator(log_file_);
}

}  // namespace sothdb
