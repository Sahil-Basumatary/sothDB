#include "storage/page.h"
#include <stdexcept>

namespace sothdb {

// page header: page_id(4) | lsn(8) | num_slots(2) | free_space_offset(2) = 16 bytes

Page::Page() {
    Reset();
}

void Page::Init(page_id_t page_id) {
    Reset();
    SetPageId(page_id);
}

void Page::Reset() {
    std::memset(data_, 0, PAGE_SIZE);
    SetFreeSpaceOffset(PAGE_SIZE);
    is_dirty_ = false;
    pin_count_ = 0;
}

page_id_t Page::GetPageId() const {
    page_id_t pid;
    std::memcpy(&pid, data_, sizeof(page_id_t));
    return pid;
}

void Page::SetPageId(page_id_t page_id) {
    std::memcpy(data_, &page_id, sizeof(page_id_t));
}

lsn_t Page::GetLsn() const {
    lsn_t lsn;
    std::memcpy(&lsn, data_ + 4, sizeof(lsn_t));
    return lsn;
}

void Page::SetLsn(lsn_t lsn) {
    std::memcpy(data_ + 4, &lsn, sizeof(lsn_t));
}

uint16_t Page::GetNumSlots() const {
    uint16_t ns;
    std::memcpy(&ns, data_ + 12, sizeof(uint16_t));
    return ns;
}

void Page::SetNumSlots(uint16_t num_slots) {
    std::memcpy(data_ + 12, &num_slots, sizeof(uint16_t));
}

uint16_t Page::GetFreeSpaceOffset() const {
    uint16_t fso;
    std::memcpy(&fso, data_ + 14, sizeof(uint16_t));
    return fso;
}

void Page::SetFreeSpaceOffset(uint16_t offset) {
    std::memcpy(data_ + 14, &offset, sizeof(uint16_t));
}

SlotEntry Page::GetSlotEntry(slot_id_t slot_id) const {
    SlotEntry entry;
    auto slot_offset = PAGE_HEADER_SIZE + slot_id * SLOT_ENTRY_SIZE;
    std::memcpy(&entry.offset, data_ + slot_offset, sizeof(uint16_t));
    std::memcpy(&entry.length, data_ + slot_offset + 2, sizeof(uint16_t));
    return entry;
}

void Page::SetSlotEntry(slot_id_t slot_id, SlotEntry entry) {
    auto slot_offset = PAGE_HEADER_SIZE + slot_id * SLOT_ENTRY_SIZE;
    std::memcpy(data_ + slot_offset, &entry.offset, sizeof(uint16_t));
    std::memcpy(data_ + slot_offset + 2, &entry.length, sizeof(uint16_t));
}

uint16_t Page::GetFreeSpace() const {
    auto slot_dir_end = PAGE_HEADER_SIZE + GetNumSlots() * SLOT_ENTRY_SIZE;
    auto free_space_ptr = GetFreeSpaceOffset();
    if (free_space_ptr <= slot_dir_end) {
        return 0;
    }
    return free_space_ptr - slot_dir_end;
}

slot_id_t Page::InsertTuple(const char* data, uint16_t len) {
    if (len == 0) {
        return INVALID_SLOT;
    }
    uint16_t space_needed = len + SLOT_ENTRY_SIZE;
    if (GetFreeSpace() < space_needed) {
        return INVALID_SLOT;
    }
    auto free_offset = GetFreeSpaceOffset();
    uint16_t tuple_offset = free_offset - len;
    std::memcpy(data_ + tuple_offset, data, len);
    auto slot_id = GetNumSlots();
    SetSlotEntry(slot_id, {tuple_offset, len});
    SetNumSlots(slot_id + 1);
    SetFreeSpaceOffset(tuple_offset);
    return slot_id;
}

std::pair<const char*, uint16_t> Page::GetTuple(slot_id_t slot_id) const {
    if (slot_id >= GetNumSlots()) {
        return {nullptr, 0};
    }
    auto entry = GetSlotEntry(slot_id);
    if (entry.length == 0) {
        return {nullptr, 0};
    }
    return {data_ + entry.offset, entry.length};
}

bool Page::DeleteTuple(slot_id_t slot_id) {
    if (slot_id >= GetNumSlots()) {
        return false;
    }
    auto entry = GetSlotEntry(slot_id);
    if (entry.length == 0) {
        return false;
    }
    SetSlotEntry(slot_id, {entry.offset, 0});
    return true;
}

}  // namespace sothdb
