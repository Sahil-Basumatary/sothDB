#pragma once
#include <cstdint>
#include <cstring>
#include <type_traits>
#include "common/config.h"
#include "index/b_plus_tree_page.h"
namespace sothdb {
template <typename KeyType, typename KeyComparator>
class BPlusTreeInternalPage {
 public:
    static_assert(std::is_trivially_copyable_v<KeyType>);
    explicit BPlusTreeInternalPage(char* data) : data_(data), page_(data) {}
    void Init(uint16_t max_size) {
        page_.Init(BPlusTreePageType::INTERNAL, max_size);
    }
    bool IsInternalPage() const {
        return page_.IsInternalPage();
    }
    uint16_t GetSize() const {
        return page_.GetSize();
    }
    void SetSize(uint16_t size) {
        page_.SetSize(size);
    }
    uint16_t GetMaxSize() const {
        return page_.GetMaxSize();
    }
    KeyType KeyAt(uint16_t index) const {
        KeyType key;
        std::memcpy(&key, data_ + KeyOffset(index), sizeof(KeyType));
        return key;
    }
    void SetKeyAt(uint16_t index, const KeyType& key) {
        std::memcpy(data_ + KeyOffset(index), &key, sizeof(KeyType));
    }
    page_id_t ValueAt(uint16_t index) const {
        page_id_t child_page_id;
        std::memcpy(&child_page_id, data_ + ValueOffset(index), sizeof(page_id_t));
        return child_page_id;
    }
    void SetValueAt(uint16_t index, page_id_t child_page_id) {
        std::memcpy(data_ + ValueOffset(index), &child_page_id, sizeof(page_id_t));
    }
    page_id_t Lookup(const KeyType& key, const KeyComparator& comparator) const {
        if (GetSize() == 0) {
            return INVALID_PAGE_ID;
        }
        uint16_t left = 1;
        uint16_t right = GetSize();
        uint16_t child_index = 0;
        while (left < right) {
            auto mid = static_cast<uint16_t>(left + (right - left) / 2);
            if (comparator(KeyAt(mid), key) <= 0) {
                child_index = mid;
                left = static_cast<uint16_t>(mid + 1);
            } else {
                right = mid;
            }
        }
        return ValueAt(child_index);
    }
    uint16_t InsertAfter(page_id_t old_child, const KeyType& new_key, page_id_t new_child) {
        auto old_child_index = FindChildIndex(old_child);
        if (old_child_index == GetSize() || GetSize() >= GetMaxSize()) {
            return GetSize();
        }
        auto insert_index = static_cast<uint16_t>(old_child_index + 1);
        for (auto i = GetSize(); i > insert_index; --i) {
            SetKeyAt(i, KeyAt(static_cast<uint16_t>(i - 1)));
            SetValueAt(i, ValueAt(static_cast<uint16_t>(i - 1)));
        }
        SetKeyAt(insert_index, new_key);
        SetValueAt(insert_index, new_child);
        SetSize(static_cast<uint16_t>(GetSize() + 1));
        return GetSize();
    }
    void MoveHalfTo(BPlusTreeInternalPage* recipient) {
        auto original_size = GetSize();
        auto start = static_cast<uint16_t>(original_size / 2);
        auto recipient_size = recipient->GetSize();
        for (uint16_t i = start; i < original_size; ++i) {
            auto target = static_cast<uint16_t>(recipient_size + i - start);
            recipient->SetKeyAt(target, KeyAt(i));
            recipient->SetValueAt(target, ValueAt(i));
        }
        recipient->SetSize(static_cast<uint16_t>(recipient_size + original_size - start));
        SetSize(start);
    }
    void PopulateNewRoot(page_id_t old_child, const KeyType& new_key, page_id_t new_child) {
        SetKeyAt(0, KeyType{});
        SetValueAt(0, old_child);
        SetKeyAt(1, new_key);
        SetValueAt(1, new_child);
        SetSize(2);
    }
 private:
    static constexpr uint16_t MappingSize() {
        return sizeof(KeyType) + sizeof(page_id_t);
    }
    static constexpr uint16_t KeyOffset(uint16_t index) {
        return static_cast<uint16_t>(B_PLUS_TREE_PAGE_HEADER_SIZE + index * MappingSize());
    }
    static constexpr uint16_t ValueOffset(uint16_t index) {
        return static_cast<uint16_t>(KeyOffset(index) + sizeof(KeyType));
    }
    uint16_t FindChildIndex(page_id_t child_page_id) const {
        for (uint16_t i = 0; i < GetSize(); ++i) {
            if (ValueAt(i) == child_page_id) {
                return i;
            }
        }
        return GetSize();
    }
    char* data_;
    BPlusTreePage page_;
};
}  // namespace sothdb
