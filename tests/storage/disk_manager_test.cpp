#include "storage/disk_manager.h"
#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>

namespace sothdb {

class DiskManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        test_file_ = "test_disk_manager.db";
        std::filesystem::remove(test_file_);
    }
    void TearDown() override {
        std::filesystem::remove(test_file_);
    }
    std::string test_file_;
};

TEST_F(DiskManagerTest, WriteAndReadRoundTrip) {
    DiskManager dm(test_file_);
    char write_buf[PAGE_SIZE];
    std::memset(write_buf, 0, PAGE_SIZE);
    const char* msg = "sothdb page data";
    std::memcpy(write_buf, msg, std::strlen(msg));
    auto pid = dm.AllocatePage();
    dm.WritePage(pid, write_buf);
    char read_buf[PAGE_SIZE];
    dm.ReadPage(pid, read_buf);
    EXPECT_EQ(std::memcmp(write_buf, read_buf, PAGE_SIZE), 0);
}

TEST_F(DiskManagerTest, PersistAcrossReopen) {
    char write_buf[PAGE_SIZE];
    std::memset(write_buf, 0, PAGE_SIZE);
    write_buf[0] = 'A';
    write_buf[100] = 'Z';
    {
        DiskManager dm(test_file_);
        auto pid = dm.AllocatePage();
        dm.WritePage(pid, write_buf);
    }
    DiskManager dm2(test_file_);
    char read_buf[PAGE_SIZE];
    dm2.ReadPage(0, read_buf);
    EXPECT_EQ(read_buf[0], 'A');
    EXPECT_EQ(read_buf[100], 'Z');
}

}  // namespace sothdb
