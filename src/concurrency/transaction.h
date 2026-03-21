#pragma once

#include "common/config.h"
#include "common/types.h"

namespace sothdb {

enum class TransactionState : uint8_t {
    ACTIVE = 0,
    COMMITTED,
    ABORTED,
};

struct Transaction {
    txn_id_t txn_id_{INVALID_TXN_ID};
    TransactionState state_{TransactionState::ACTIVE};
    lsn_t prev_lsn_{INVALID_LSN};

    explicit Transaction(txn_id_t txn_id)
        : txn_id_(txn_id) {}

    txn_id_t GetTxnId() const { return txn_id_; }
    TransactionState GetState() const { return state_; }
    lsn_t GetPrevLsn() const { return prev_lsn_; }
    void SetState(TransactionState state) { state_ = state; }
    void SetPrevLsn(lsn_t lsn) { prev_lsn_ = lsn; }
};

}  // namespace sothdb
