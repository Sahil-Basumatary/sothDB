#pragma once
#include <cstdint>
#include <cstring>
#include <type_traits>
#include "common/config.h"
#include "index/b_plus_tree_page.h"
namespace sothdb {
template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeLeafPage {
 public:
    static_assert(std::is_trivially_copyable_v<KeyType>);
    static_assert(std::is_trivially_copyable_v<ValueType>);
    explicit BPlusTreeLeafPage(char* data) : data_(data), page_(data) {}
    void Init(uint16_t max_size) {
        page_.Init(BPlusTreePageType::LEAF, max_size);
        SetNextPageId(INVALID_PAGE_ID);
    }
    bool IsLeafPage() const {
        return page_.IsLeafPage();
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
    page_id_t GetNextPageId() const {
        page_id_t next_page_id;
        std::memcpy(&next_page_id, data_ + NEXT_PAGE_ID_OFFSET, sizeof(page_id_t));
        return next_page_id;
    }
    void SetNextPageId(page_id_t next_page_id) {
        std::memcpy(data_ + NEXT_PAGE_ID_OFFSET, &next_page_id, sizeof(page_id_t));
    }
    KeyType KeyAt(uint16_t index) const {
        KeyType key;
        std::memcpy(&key, data_ + KeyOffset(index), sizeof(KeyType));
        return key;
    }
    ValueType ValueAt(uint16_t index) const {
        ValueType value;
        std::memcpy(&value, data_ + ValueOffset(index), sizeof(ValueType));
        return value;
    }
    void SetKeyAt(uint16_t index, const KeyType& key) {
        std::memcpy(data_ + KeyOffset(index), &key, sizeof(KeyType));
    }
    void SetValueAt(uint16_t index, const ValueType& value) {
        std::memcpy(data_ + ValueOffset(index), &value, sizeof(ValueType));
    }
    bool Lookup(const KeyType& key, ValueType* value, const KeyComparator& comparator) const {
        auto index = LowerBound(key, comparator);
        if (index >= GetSize() || comparator(KeyAt(index), key) != 0) {
            return false;
        }
        *value = ValueAt(index);
        return true;
    }
    bool Insert(const KeyType& key, const ValueType& value, const KeyComparator& comparator) {
        if (GetSize() >= GetMaxSize()) {
            return false;
        }
        auto insert_index = LowerBound(key, comparator);
        if (insert_index < GetSize() && comparator(KeyAt(insert_index), key) == 0) {
            return false;
        }
        for (auto i = GetSize(); i > insert_index; --i) {
            SetKeyAt(i, KeyAt(static_cast<uint16_t>(i - 1)));
            SetValueAt(i, ValueAt(static_cast<uint16_t>(i - 1)));
        }
        SetKeyAt(insert_index, key);
        SetValueAt(insert_index, value);
        SetSize(static_cast<uint16_t>(GetSize() + 1));
        return true;
    }
 private:
    static constexpr uint16_t NEXT_PAGE_ID_OFFSET = B_PLUS_TREE_PAGE_HEADER_SIZE;
    static constexpr uint16_t ARRAY_OFFSET = B_PLUS_TREE_PAGE_HEADER_SIZE + sizeof(page_id_t);
    static constexpr uint16_t MappingSize() {
        return sizeof(KeyType) + sizeof(ValueType);
    }
    static constexpr uint16_t KeyOffset(uint16_t index) {
        return static_cast<uint16_t>(ARRAY_OFFSET + index * MappingSize());
    }
    static constexpr uint16_t ValueOffset(uint16_t index) {
        return static_cast<uint16_t>(KeyOffset(index) + sizeof(KeyType));
    }
    uint16_t LowerBound(const KeyType& key, const KeyComparator& comparator) const {
        uint16_t left = 0;
        uint16_t right = GetSize();
        while (left < right) {
            auto mid = static_cast<uint16_t>(left + (right - left) / 2);
            if (comparator(KeyAt(mid), key) < 0) {
                left = static_cast<uint16_t>(mid + 1);
            } else {
                right = mid;
            }
        }
        return left;
    }
    char* data_;
    BPlusTreePage page_;
};
}  // namespace sothdb
