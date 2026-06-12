#pragma once

#include <cstdint>

namespace sothdb {

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr int INVALID_PAGE_ID = -1;
static constexpr uint32_t DEFAULT_BUFFER_POOL_SIZE = 128;
static constexpr uint64_t INVALID_LSN = 0;
static constexpr int32_t INVALID_TXN_ID = -1;
static constexpr uint32_t LOG_BUFFER_SIZE = 4096 * 4;

}  // namespace sothdb
