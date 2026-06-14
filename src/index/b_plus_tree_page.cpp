#include "index/b_plus_tree_page.h"
#include <cstring>

namespace sothdb {

BPlusTreePage::BPlusTreePage(char* data) : data_(data) {}

void BPlusTreePage::Init(BPlusTreePageType page_type, uint16_t max_size) {
    SetPageType(page_type);
    SetSize(0);
    SetMaxSize(max_size);
}

BPlusTreePageType BPlusTreePage::GetPageType() const {
    uint8_t page_type;
    std::memcpy(&page_type, data_ + B_PLUS_TREE_PAGE_TYPE_OFFSET, sizeof(uint8_t));
    return static_cast<BPlusTreePageType>(page_type);
}

void BPlusTreePage::SetPageType(BPlusTreePageType page_type) {
    auto raw_page_type = static_cast<uint8_t>(page_type);
    std::memcpy(data_ + B_PLUS_TREE_PAGE_TYPE_OFFSET, &raw_page_type, sizeof(uint8_t));
}

bool BPlusTreePage::IsLeafPage() const {
    return GetPageType() == BPlusTreePageType::LEAF;
}

bool BPlusTreePage::IsInternalPage() const {
    return GetPageType() == BPlusTreePageType::INTERNAL;
}

uint16_t BPlusTreePage::GetSize() const {
    uint16_t size;
    std::memcpy(&size, data_ + B_PLUS_TREE_PAGE_SIZE_OFFSET, sizeof(uint16_t));
    return size;
}

void BPlusTreePage::SetSize(uint16_t size) {
    std::memcpy(data_ + B_PLUS_TREE_PAGE_SIZE_OFFSET, &size, sizeof(uint16_t));
}

void BPlusTreePage::IncreaseSize(int16_t amount) {
    SetSize(static_cast<uint16_t>(GetSize() + amount));
}

uint16_t BPlusTreePage::GetMaxSize() const {
    uint16_t max_size;
    std::memcpy(&max_size, data_ + B_PLUS_TREE_PAGE_MAX_SIZE_OFFSET, sizeof(uint16_t));
    return max_size;
}

void BPlusTreePage::SetMaxSize(uint16_t max_size) {
    std::memcpy(data_ + B_PLUS_TREE_PAGE_MAX_SIZE_OFFSET, &max_size, sizeof(uint16_t));
}

}  // namespace sothdb
