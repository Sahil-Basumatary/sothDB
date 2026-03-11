#pragma once

#include <cstring>
#include <utility>
#include "common/config.h"
#include "common/types.h"

namespace sothdb {

static constexpr uint16_t PAGE_HEADER_SIZE = 16;
static constexpr uint16_t SLOT_ENTRY_SIZE = 4;
static constexpr uint16_t INVALID_SLOT = 0xFFFF;

struct SlotEntry {
    uint16_t offset;
    uint16_t length;
};

class Page {
 public:
    Page();
    void Init(page_id_t page_id);
    void Reset();
    slot_id_t InsertTuple(const char* data, uint16_t len);
    std::pair<const char*, uint16_t> GetTuple(slot_id_t slot_id) const;
    bool DeleteTuple(slot_id_t slot_id);
    uint16_t GetFreeSpace() const;
    page_id_t GetPageId() const;
    void SetPageId(page_id_t page_id);
    lsn_t GetLsn() const;
    void SetLsn(lsn_t lsn);
    uint16_t GetNumSlots() const;
    bool IsDirty() const { return is_dirty_; }
    void SetDirty(bool dirty) { is_dirty_ = dirty; }
    int GetPinCount() const { return pin_count_; }
    void IncrementPinCount() { ++pin_count_; }
    void DecrementPinCount() { --pin_count_; }
    char* GetData() { return data_; }
    const char* GetData() const { return data_; }

 private:
    void SetNumSlots(uint16_t num_slots);
    void SetFreeSpaceOffset(uint16_t offset);
    uint16_t GetFreeSpaceOffset() const;
    SlotEntry GetSlotEntry(slot_id_t slot_id) const;
    void SetSlotEntry(slot_id_t slot_id, SlotEntry entry);
    char data_[PAGE_SIZE]{};
    bool is_dirty_{false};
    int pin_count_{0};
};

} // namespace sothdb
