#include "recovery/log_record.h"
#include <cstring>
#include <stdexcept>

namespace sothdb {

LogRecord LogRecord::MakeBegin(txn_id_t txn_id, lsn_t prev_lsn) {
    LogRecord rec;
    rec.type_ = LogRecordType::BEGIN;
    rec.txn_id_ = txn_id;
    rec.prev_lsn_ = prev_lsn;
    rec.size_ = LOG_HEADER_SIZE;
    return rec;
}

LogRecord LogRecord::MakeCommit(txn_id_t txn_id, lsn_t prev_lsn) {
    LogRecord rec;
    rec.type_ = LogRecordType::COMMIT;
    rec.txn_id_ = txn_id;
    rec.prev_lsn_ = prev_lsn;
    rec.size_ = LOG_HEADER_SIZE;
    return rec;
}

LogRecord LogRecord::MakeAbort(txn_id_t txn_id, lsn_t prev_lsn) {
    LogRecord rec;
    rec.type_ = LogRecordType::ABORT;
    rec.txn_id_ = txn_id;
    rec.prev_lsn_ = prev_lsn;
    rec.size_ = LOG_HEADER_SIZE;
    return rec;
}

LogRecord LogRecord::MakeInsert(txn_id_t txn_id, lsn_t prev_lsn,
                                page_id_t page_id, slot_id_t slot_id,
                                const char* data, uint16_t data_len) {
    LogRecord rec;
    rec.type_ = LogRecordType::INSERT;
    rec.txn_id_ = txn_id;
    rec.prev_lsn_ = prev_lsn;
    rec.page_id_ = page_id;
    rec.slot_id_ = slot_id;
    rec.new_data_.assign(data, data + data_len);
    rec.size_ = LOG_HEADER_SIZE + sizeof(page_id_t) + sizeof(slot_id_t)
                + sizeof(uint16_t) + data_len;
    return rec;
}

LogRecord LogRecord::MakeDelete(txn_id_t txn_id, lsn_t prev_lsn,
                                page_id_t page_id, slot_id_t slot_id,
                                const char* old_data, uint16_t data_len) {
    LogRecord rec;
    rec.type_ = LogRecordType::DELETE;
    rec.txn_id_ = txn_id;
    rec.prev_lsn_ = prev_lsn;
    rec.page_id_ = page_id;
    rec.slot_id_ = slot_id;
    rec.old_data_.assign(old_data, old_data + data_len);
    rec.size_ = LOG_HEADER_SIZE + sizeof(page_id_t) + sizeof(slot_id_t)
                + sizeof(uint16_t) + data_len;
    return rec;
}

LogRecord LogRecord::MakeUpdate(txn_id_t txn_id, lsn_t prev_lsn,
                                page_id_t page_id, slot_id_t slot_id,
                                const char* old_data, uint16_t old_len,
                                const char* new_data, uint16_t new_len) {
    LogRecord rec;
    rec.type_ = LogRecordType::UPDATE;
    rec.txn_id_ = txn_id;
    rec.prev_lsn_ = prev_lsn;
    rec.page_id_ = page_id;
    rec.slot_id_ = slot_id;
    rec.old_data_.assign(old_data, old_data + old_len);
    rec.new_data_.assign(new_data, new_data + new_len);
    rec.size_ = LOG_HEADER_SIZE + sizeof(page_id_t) + sizeof(slot_id_t)
                + sizeof(uint16_t) + old_len + sizeof(uint16_t) + new_len;
    return rec;
}

std::vector<char> LogRecord::Serialize() const {
    std::vector<char> buf(size_);
    char* ptr = buf.data();
    std::memcpy(ptr, &size_, sizeof(size_));
    ptr += sizeof(size_);
    std::memcpy(ptr, &lsn_, sizeof(lsn_));
    ptr += sizeof(lsn_);
    std::memcpy(ptr, &txn_id_, sizeof(txn_id_));
    ptr += sizeof(txn_id_);
    std::memcpy(ptr, &prev_lsn_, sizeof(prev_lsn_));
    ptr += sizeof(prev_lsn_);
    auto type_val = static_cast<uint8_t>(type_);
    std::memcpy(ptr, &type_val, sizeof(type_val));
    ptr += sizeof(type_val);
    if (type_ == LogRecordType::INSERT) {
        std::memcpy(ptr, &page_id_, sizeof(page_id_));
        ptr += sizeof(page_id_);
        std::memcpy(ptr, &slot_id_, sizeof(slot_id_));
        ptr += sizeof(slot_id_);
        auto len = static_cast<uint16_t>(new_data_.size());
        std::memcpy(ptr, &len, sizeof(len));
        ptr += sizeof(len);
        std::memcpy(ptr, new_data_.data(), len);
    } else if (type_ == LogRecordType::DELETE) {
        std::memcpy(ptr, &page_id_, sizeof(page_id_));
        ptr += sizeof(page_id_);
        std::memcpy(ptr, &slot_id_, sizeof(slot_id_));
        ptr += sizeof(slot_id_);
        auto len = static_cast<uint16_t>(old_data_.size());
        std::memcpy(ptr, &len, sizeof(len));
        ptr += sizeof(len);
        std::memcpy(ptr, old_data_.data(), len);
    } else if (type_ == LogRecordType::UPDATE) {
        std::memcpy(ptr, &page_id_, sizeof(page_id_));
        ptr += sizeof(page_id_);
        std::memcpy(ptr, &slot_id_, sizeof(slot_id_));
        ptr += sizeof(slot_id_);
        auto old_len = static_cast<uint16_t>(old_data_.size());
        std::memcpy(ptr, &old_len, sizeof(old_len));
        ptr += sizeof(old_len);
        std::memcpy(ptr, old_data_.data(), old_len);
        ptr += old_len;
        auto new_len = static_cast<uint16_t>(new_data_.size());
        std::memcpy(ptr, &new_len, sizeof(new_len));
        ptr += sizeof(new_len);
        std::memcpy(ptr, new_data_.data(), new_len);
    }
    return buf;
}

LogRecord LogRecord::Deserialize(const char* data, uint32_t size) {
    if (size < LOG_HEADER_SIZE) {
        throw std::runtime_error("Log record too small to contain header");
    }
    LogRecord rec;
    const char* ptr = data;
    std::memcpy(&rec.size_, ptr, sizeof(rec.size_));
    ptr += sizeof(rec.size_);
    if (rec.size_ != size) {
        throw std::runtime_error("Log record size mismatch");
    }
    std::memcpy(&rec.lsn_, ptr, sizeof(rec.lsn_));
    ptr += sizeof(rec.lsn_);
    std::memcpy(&rec.txn_id_, ptr, sizeof(rec.txn_id_));
    ptr += sizeof(rec.txn_id_);
    std::memcpy(&rec.prev_lsn_, ptr, sizeof(rec.prev_lsn_));
    ptr += sizeof(rec.prev_lsn_);
    uint8_t type_val;
    std::memcpy(&type_val, ptr, sizeof(type_val));
    rec.type_ = static_cast<LogRecordType>(type_val);
    ptr += sizeof(type_val);
    if (rec.type_ == LogRecordType::INSERT) {
        std::memcpy(&rec.page_id_, ptr, sizeof(rec.page_id_));
        ptr += sizeof(rec.page_id_);
        std::memcpy(&rec.slot_id_, ptr, sizeof(rec.slot_id_));
        ptr += sizeof(rec.slot_id_);
        uint16_t len;
        std::memcpy(&len, ptr, sizeof(len));
        ptr += sizeof(len);
        rec.new_data_.assign(ptr, ptr + len);
    } else if (rec.type_ == LogRecordType::DELETE) {
        std::memcpy(&rec.page_id_, ptr, sizeof(rec.page_id_));
        ptr += sizeof(rec.page_id_);
        std::memcpy(&rec.slot_id_, ptr, sizeof(rec.slot_id_));
        ptr += sizeof(rec.slot_id_);
        uint16_t len;
        std::memcpy(&len, ptr, sizeof(len));
        ptr += sizeof(len);
        rec.old_data_.assign(ptr, ptr + len);
    } else if (rec.type_ == LogRecordType::UPDATE) {
        std::memcpy(&rec.page_id_, ptr, sizeof(rec.page_id_));
        ptr += sizeof(rec.page_id_);
        std::memcpy(&rec.slot_id_, ptr, sizeof(rec.slot_id_));
        ptr += sizeof(rec.slot_id_);
        uint16_t old_len;
        std::memcpy(&old_len, ptr, sizeof(old_len));
        ptr += sizeof(old_len);
        rec.old_data_.assign(ptr, ptr + old_len);
        ptr += old_len;
        uint16_t new_len;
        std::memcpy(&new_len, ptr, sizeof(new_len));
        ptr += sizeof(new_len);
        rec.new_data_.assign(ptr, ptr + new_len);
    }
    return rec;
}

}  // namespace sothdb
