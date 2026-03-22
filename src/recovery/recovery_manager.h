#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "common/types.h"
#include "concurrency/transaction.h"
#include "recovery/log_manager.h"
#include "recovery/log_record.h"

namespace sothdb {

class BufferPoolManager;
class Page;

class RecoveryManager {
 public:
    RecoveryManager(LogManager* log_manager, BufferPoolManager* bpm);
    void LogBegin(Transaction* txn);
    void LogCommit(Transaction* txn);
    void LogAbort(Transaction* txn);
    lsn_t LogInsert(Transaction* txn, page_id_t page_id, slot_id_t slot_id,
                    const char* data, uint16_t len);
    lsn_t LogDelete(Transaction* txn, page_id_t page_id, slot_id_t slot_id,
                    const char* old_data, uint16_t len);
    lsn_t LogUpdate(Transaction* txn, page_id_t page_id, slot_id_t slot_id,
                    const char* old_data, uint16_t old_len,
                    const char* new_data, uint16_t new_len);
    void Recover();

 private:
    void Redo(const std::vector<LogRecord>& records,
              const std::unordered_set<txn_id_t>& committed_txns);
    void Undo(const std::unordered_map<lsn_t, LogRecord>& lsn_map,
              const std::unordered_map<txn_id_t, lsn_t>& active_last_lsn);
    void RedoInsert(Page* page, const LogRecord& record);
    void RedoDelete(Page* page, const LogRecord& record);
    void RedoUpdate(Page* page, const LogRecord& record);
    void UndoInsert(Page* page, const LogRecord& record);
    void UndoDelete(Page* page, const LogRecord& record);
    void UndoUpdate(Page* page, const LogRecord& record);
    LogManager* log_manager_;
    BufferPoolManager* bpm_;
};

}  // namespace sothdb
