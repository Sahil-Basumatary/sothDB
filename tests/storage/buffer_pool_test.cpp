#include "storage/buffer_pool_manager.h"
#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>
#include <string>

namespace sothdb {

class BufferPoolTest : public ::testing::Test {
 protected:
    void SetUp() override {
        test_file_ = "test_buffer_pool.db";
        std::filesystem::remove(test_file_);
        disk_mgr_ = new DiskManager(test_file_);
    }
    void TearDown() override {
        delete bpm_;
        delete disk_mgr_;
        std::filesystem::remove(test_file_);
    }
    std::string test_file_;
    DiskManager* disk_mgr_ = nullptr;
    BufferPoolManager* bpm_ = nullptr;
};

TEST_F(BufferPoolTest, NewPageAndFetch) {
    bpm_ = new BufferPoolManager(4, disk_mgr_);
    page_id_t pid;
    auto* page = bpm_->NewPage(&pid);
    ASSERT_NE(page, nullptr);
    EXPECT_EQ(pid, 0);
    const char* msg = "persist this";
    page->InsertTuple(msg, static_cast<uint16_t>(std::strlen(msg)));
    bpm_->UnpinPage(pid, true);
    bpm_->FlushPage(pid);
    for (int i = 0; i < 4; i++) {
        page_id_t tmp;
        bpm_->NewPage(&tmp);
        bpm_->UnpinPage(tmp, false);
    }
    auto* fetched = bpm_->FetchPage(pid);
    ASSERT_NE(fetched, nullptr);
    auto [ptr, len] = fetched->GetTuple(0);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(std::string(ptr, len), "persist this");
    bpm_->UnpinPage(pid, false);
}

TEST_F(BufferPoolTest, AllPinnedReturnsNull) {
    bpm_ = new BufferPoolManager(2, disk_mgr_);
    page_id_t p0, p1, p2;
    bpm_->NewPage(&p0);
    bpm_->NewPage(&p1);
    auto* page = bpm_->NewPage(&p2);
    EXPECT_EQ(page, nullptr);
}

TEST_F(BufferPoolTest, DirtyPageFlushedOnEviction) {
    bpm_ = new BufferPoolManager(1, disk_mgr_);
    page_id_t pid;
    auto* page = bpm_->NewPage(&pid);
    const char* data = "dirty data";
    page->InsertTuple(data, static_cast<uint16_t>(std::strlen(data)));
    bpm_->UnpinPage(pid, true);
    page_id_t pid2;
    bpm_->NewPage(&pid2);
    bpm_->UnpinPage(pid2, false);
    auto* fetched = bpm_->FetchPage(pid);
    ASSERT_NE(fetched, nullptr);
    auto [ptr, len] = fetched->GetTuple(0);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(std::string(ptr, len), "dirty data");
    bpm_->UnpinPage(pid, false);
}

}  // namespace sothdb
