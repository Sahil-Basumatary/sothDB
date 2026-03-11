#include "storage/page.h"
#include <gtest/gtest.h>
#include <cstring>
#include <string>

namespace sothdb {

TEST(PageTest, InsertAndGetTuple) {
    Page page;
    page.Init(0);
    std::string data = "hello world";
    auto slot = page.InsertTuple(data.c_str(), static_cast<uint16_t>(data.size()));
    ASSERT_NE(slot, INVALID_SLOT);
    auto [ptr, len] = page.GetTuple(slot);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(len, data.size());
    EXPECT_EQ(std::string(ptr, len), data);
}

TEST(PageTest, InsertMultipleTuples) {
    Page page;
    page.Init(1);
    std::string t1 = "first";
    std::string t2 = "second tuple";
    std::string t3 = "third";
    auto s1 = page.InsertTuple(t1.c_str(), static_cast<uint16_t>(t1.size()));
    auto s2 = page.InsertTuple(t2.c_str(), static_cast<uint16_t>(t2.size()));
    auto s3 = page.InsertTuple(t3.c_str(), static_cast<uint16_t>(t3.size()));
    EXPECT_EQ(s1, 0);
    EXPECT_EQ(s2, 1);
    EXPECT_EQ(s3, 2);
    auto [p2, l2] = page.GetTuple(s2);
    EXPECT_EQ(std::string(p2, l2), t2);
}

TEST(PageTest, DeleteTuple) {
    Page page;
    page.Init(0);
    std::string data = "to be deleted";
    auto slot = page.InsertTuple(data.c_str(), static_cast<uint16_t>(data.size()));
    EXPECT_TRUE(page.DeleteTuple(slot));
    auto [ptr, len] = page.GetTuple(slot);
    EXPECT_EQ(ptr, nullptr);
}

TEST(PageTest, InsertFailsWhenFull) {
    Page page;
    page.Init(0);
    char big_tuple[PAGE_SIZE];
    std::memset(big_tuple, 'X', PAGE_SIZE);
    auto slot = page.InsertTuple(big_tuple, PAGE_SIZE);
    EXPECT_EQ(slot, INVALID_SLOT);
}

}  // namespace sothdb
