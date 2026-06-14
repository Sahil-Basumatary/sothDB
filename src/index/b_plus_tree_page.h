#pragma once

#include <cstdint>
#include "common/types.h"

namespace sothdb {

enum class BPlusTreePageType : uint8_t {
    INVALID = 0,
    LEAF = 1,
    INTERNAL = 2,
};

static constexpr uint16_t B_PLUS_TREE_PAGE_TYPE_OFFSET = 12;
static constexpr uint16_t B_PLUS_TREE_PAGE_SIZE_OFFSET = 14;
static constexpr uint16_t B_PLUS_TREE_PAGE_MAX_SIZE_OFFSET = 16;
static constexpr uint16_t B_PLUS_TREE_PAGE_HEADER_SIZE = 18;

class BPlusTreePage {
 public:
    explicit BPlusTreePage(char* data);

    void Init(BPlusTreePageType page_type, uint16_t max_size);
    BPlusTreePageType GetPageType() const;
    void SetPageType(BPlusTreePageType page_type);
    bool IsLeafPage() const;
    bool IsInternalPage() const;
    uint16_t GetSize() const;
    void SetSize(uint16_t size);
    void IncreaseSize(int16_t amount);
    uint16_t GetMaxSize() const;
    void SetMaxSize(uint16_t max_size);

 private:
    char* data_;
};

}  // namespace sothdb
