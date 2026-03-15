#pragma once

#include <cstdint>
#include <vector>
#include "common/config.h"
#include "common/types.h"

namespace sothdb {

static constexpr uint32_t LOG_HEADER_SIZE = 25;

enum class LogRecordType : uint8_t {
    INVALID = 0,
    BEGIN,
    COMMIT,
    ABORT,
    INSERT,
    DELETE,
    UPDATE,
};

class LogRecord {
 public:
    LogRecord() = default;
    static LogRecord MakeBegin(txn_id_t txn_id, lsn_t prev_lsn);
    static LogRecord MakeCommit(txn_id_t txn_id, lsn_t prev_lsn);
    static LogRecord MakeAbort(txn_id_t txn_id, lsn_t prev_lsn);
    static LogRecord MakeInsert(txn_id_t txn_id, lsn_t prev_lsn,
                                page_id_t page_id, slot_id_t slot_id,
                                const char* data, uint16_t data_len);
    static LogRecord MakeDelete(txn_id_t txn_id, lsn_t prev_lsn,
                                page_id_t page_id, slot_id_t slot_id,
                                const char* old_data, uint16_t data_len);
    static LogRecord MakeUpdate(txn_id_t txn_id, lsn_t prev_lsn,
                                page_id_t page_id, slot_id_t slot_id,
                                const char* old_data, uint16_t old_len,
                                const char* new_data, uint16_t new_len);
    std::vector<char> Serialize() const;
    static LogRecord Deserialize(const char* data, uint32_t size);
    uint32_t GetSize() const { return size_; }
    lsn_t GetLsn() const { return lsn_; }
    void SetLsn(lsn_t lsn) { lsn_ = lsn; }
    txn_id_t GetTxnId() const { return txn_id_; }
    lsn_t GetPrevLsn() const { return prev_lsn_; }
    LogRecordType GetType() const { return type_; }
    page_id_t GetPageId() const { return page_id_; }
    slot_id_t GetSlotId() const { return slot_id_; }
    const std::vector<char>& GetOldData() const { return old_data_; }
    const std::vector<char>& GetNewData() const { return new_data_; }

 private:
    uint32_t size_{0};
    lsn_t lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};
    lsn_t prev_lsn_{INVALID_LSN};
    LogRecordType type_{LogRecordType::INVALID};
    page_id_t page_id_{INVALID_PAGE_ID};
    slot_id_t slot_id_{0};
    std::vector<char> old_data_;
    std::vector<char> new_data_;
};

}  // namespace sothdb
