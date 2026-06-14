#pragma once

#include "common/config.h"
#include "common/types.h"

namespace sothdb {

static constexpr slot_id_t INVALID_RID_SLOT_ID = static_cast<slot_id_t>(0xFFFF);

struct RID {
    page_id_t page_id{INVALID_PAGE_ID};
    slot_id_t slot_id{INVALID_RID_SLOT_ID};

    RID() = default;
    RID(page_id_t page_id, slot_id_t slot_id) : page_id(page_id), slot_id(slot_id) {}

    bool IsValid() const {
        return page_id != INVALID_PAGE_ID && slot_id != INVALID_RID_SLOT_ID;
    }

    friend bool operator==(const RID& lhs, const RID& rhs) {
        return lhs.page_id == rhs.page_id && lhs.slot_id == rhs.slot_id;
    }

    friend bool operator!=(const RID& lhs, const RID& rhs) {
        return !(lhs == rhs);
    }
};

}  // namespace sothdb
