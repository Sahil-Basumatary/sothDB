#include "recovery/recovery_manager.h"
#include <cstring>
#include "storage/buffer_pool_manager.h"
#include "storage/page.h"

namespace sothdb {

namespace {

void EnsurePageInitialized(Page* page) {
    uint16_t free_offset;
    std::memcpy(&free_offset, page->GetData() + 14, sizeof(uint16_t));
    if (free_offset == 0 && page->GetNumSlots() == 0) {
        page->Init(page->GetPageId());
    }
}

void WriteTupleAt(Page* page, slot_id_t slot_id,
                  const char* data, uint16_t len) {
    char* raw = page->GetData();
    uint16_t slot_pos = PAGE_HEADER_SIZE + slot_id * SLOT_ENTRY_SIZE;
    uint16_t tuple_offset;
    uint16_t tuple_length;
    std::memcpy(&tuple_offset, raw + slot_pos, sizeof(uint16_t));
    std::memcpy(&tuple_length, raw + slot_pos + 2, sizeof(uint16_t));
    if (len <= tuple_length) {
        std::memcpy(raw + tuple_offset, data, len);
        std::memcpy(raw + slot_pos + 2, &len, sizeof(uint16_t));
    } else {
        uint16_t free_offset;
        std::memcpy(&free_offset, raw + 14, sizeof(uint16_t));
        uint16_t new_offset = free_offset - len;
        std::memcpy(raw + new_offset, data, len);
        std::memcpy(raw + slot_pos, &new_offset, sizeof(uint16_t));
        std::memcpy(raw + slot_pos + 2, &len, sizeof(uint16_t));
        std::memcpy(raw + 14, &new_offset, sizeof(uint16_t));
    }
}

}  // namespace

RecoveryManager::RecoveryManager(LogManager* log_manager, BufferPoolManager* bpm)
    : log_manager_(log_manager), bpm_(bpm) {}

void RecoveryManager::LogBegin(Transaction* txn) {
    auto record = LogRecord::MakeBegin(txn->GetTxnId(), txn->GetPrevLsn());
    auto lsn = log_manager_->AppendLogRecord(record);
    txn->SetPrevLsn(lsn);
}

void RecoveryManager::LogCommit(Transaction* txn) {
    auto record = LogRecord::MakeCommit(txn->GetTxnId(), txn->GetPrevLsn());
    auto lsn = log_manager_->AppendLogRecord(record);
    txn->SetPrevLsn(lsn);
    txn->SetState(TransactionState::COMMITTED);
    log_manager_->Flush(lsn);
}

void RecoveryManager::LogAbort(Transaction* txn) {
    auto record = LogRecord::MakeAbort(txn->GetTxnId(), txn->GetPrevLsn());
    auto lsn = log_manager_->AppendLogRecord(record);
    txn->SetPrevLsn(lsn);
    txn->SetState(TransactionState::ABORTED);
    log_manager_->Flush(lsn);
}

lsn_t RecoveryManager::LogInsert(Transaction* txn, page_id_t page_id,
                                  slot_id_t slot_id, const char* data,
                                  uint16_t len) {
    auto record = LogRecord::MakeInsert(txn->GetTxnId(), txn->GetPrevLsn(),
                                        page_id, slot_id, data, len);
    auto lsn = log_manager_->AppendLogRecord(record);
    txn->SetPrevLsn(lsn);
    return lsn;
}

lsn_t RecoveryManager::LogDelete(Transaction* txn, page_id_t page_id,
                                  slot_id_t slot_id, const char* old_data,
                                  uint16_t len) {
    auto record = LogRecord::MakeDelete(txn->GetTxnId(), txn->GetPrevLsn(),
                                        page_id, slot_id, old_data, len);
    auto lsn = log_manager_->AppendLogRecord(record);
    txn->SetPrevLsn(lsn);
    return lsn;
}

lsn_t RecoveryManager::LogUpdate(Transaction* txn, page_id_t page_id,
                                  slot_id_t slot_id, const char* old_data,
                                  uint16_t old_len, const char* new_data,
                                  uint16_t new_len) {
    auto record = LogRecord::MakeUpdate(txn->GetTxnId(), txn->GetPrevLsn(),
                                        page_id, slot_id, old_data, old_len,
                                        new_data, new_len);
    auto lsn = log_manager_->AppendLogRecord(record);
    txn->SetPrevLsn(lsn);
    return lsn;
}

void RecoveryManager::Recover() {
    auto iter = log_manager_->MakeIterator();
    std::vector<LogRecord> records;
    std::unordered_set<txn_id_t> committed_txns;
    std::unordered_map<txn_id_t, lsn_t> active_last_lsn;
    std::unordered_map<lsn_t, LogRecord> lsn_map;
    while (iter.HasNext()) {
        auto record = iter.Next();
        auto txn_id = record.GetTxnId();
        auto lsn = record.GetLsn();
        switch (record.GetType()) {
            case LogRecordType::BEGIN:
                active_last_lsn[txn_id] = lsn;
                break;
            case LogRecordType::COMMIT:
                committed_txns.insert(txn_id);
                active_last_lsn.erase(txn_id);
                break;
            case LogRecordType::ABORT:
                active_last_lsn.erase(txn_id);
                break;
            default:
                active_last_lsn[txn_id] = lsn;
                lsn_map.emplace(lsn, record);
                break;
        }
        records.push_back(std::move(record));
    }
    Redo(records, committed_txns);
    Undo(lsn_map, active_last_lsn);
}

void RecoveryManager::Redo(const std::vector<LogRecord>& records,
                            const std::unordered_set<txn_id_t>& committed_txns) {
    for (const auto& record : records) {
        if (committed_txns.count(record.GetTxnId()) == 0) continue;
        auto type = record.GetType();
        if (type != LogRecordType::INSERT &&
            type != LogRecordType::DELETE &&
            type != LogRecordType::UPDATE) {
            continue;
        }
        auto* page = bpm_->FetchPage(record.GetPageId());
        if (page == nullptr) continue;
        if (page->GetLsn() >= record.GetLsn()) {
            bpm_->UnpinPage(record.GetPageId(), false);
            continue;
        }
        EnsurePageInitialized(page);
        switch (type) {
            case LogRecordType::INSERT: RedoInsert(page, record); break;
            case LogRecordType::DELETE: RedoDelete(page, record); break;
            case LogRecordType::UPDATE: RedoUpdate(page, record); break;
            default: break;
        }
        page->SetLsn(record.GetLsn());
        bpm_->UnpinPage(record.GetPageId(), true);
    }
}

void RecoveryManager::Undo(const std::unordered_map<lsn_t, LogRecord>& lsn_map,
                            const std::unordered_map<txn_id_t, lsn_t>& active_last_lsn) {
    for (const auto& [txn_id, last_lsn] : active_last_lsn) {
        auto lsn = last_lsn;
        while (lsn != INVALID_LSN) {
            auto it = lsn_map.find(lsn);
            if (it == lsn_map.end()) break;
            const auto& record = it->second;
            auto* page = bpm_->FetchPage(record.GetPageId());
            if (page != nullptr) {
                // Only undo if the operation actually made it to this page
                if (page->GetLsn() >= record.GetLsn()) {
                    switch (record.GetType()) {
                        case LogRecordType::INSERT: UndoInsert(page, record); break;
                        case LogRecordType::DELETE: UndoDelete(page, record); break;
                        case LogRecordType::UPDATE: UndoUpdate(page, record); break;
                        default: break;
                    }
                    bpm_->UnpinPage(record.GetPageId(), true);
                } else {
                    bpm_->UnpinPage(record.GetPageId(), false);
                }
            }
            lsn = record.GetPrevLsn();
        }
    }
}

void RecoveryManager::RedoInsert(Page* page, const LogRecord& record) {
    const auto& data = record.GetNewData();
    page->InsertTuple(data.data(), static_cast<uint16_t>(data.size()));
}

void RecoveryManager::RedoDelete(Page* page, const LogRecord& record) {
    page->DeleteTuple(record.GetSlotId());
}

void RecoveryManager::RedoUpdate(Page* page, const LogRecord& record) {
    const auto& new_data = record.GetNewData();
    WriteTupleAt(page, record.GetSlotId(),
                 new_data.data(), static_cast<uint16_t>(new_data.size()));
}

void RecoveryManager::UndoInsert(Page* page, const LogRecord& record) {
    page->DeleteTuple(record.GetSlotId());
}

void RecoveryManager::UndoDelete(Page* page, const LogRecord& record) {
    const auto& old_data = record.GetOldData();
    char* raw = page->GetData();
    uint16_t slot_pos = PAGE_HEADER_SIZE + record.GetSlotId() * SLOT_ENTRY_SIZE;
    uint16_t tuple_offset;
    std::memcpy(&tuple_offset, raw + slot_pos, sizeof(uint16_t));
    auto len = static_cast<uint16_t>(old_data.size());
    std::memcpy(raw + tuple_offset, old_data.data(), len);
    std::memcpy(raw + slot_pos + 2, &len, sizeof(uint16_t));
}

void RecoveryManager::UndoUpdate(Page* page, const LogRecord& record) {
    const auto& old_data = record.GetOldData();
    WriteTupleAt(page, record.GetSlotId(),
                 old_data.data(), static_cast<uint16_t>(old_data.size()));
}

}  // namespace sothdb
