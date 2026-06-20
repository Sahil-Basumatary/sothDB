#include "index/b_plus_tree.h"
#include <gtest/gtest.h>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>
#include "common/rid.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
namespace sothdb {
struct BPlusTreeInt64Comparator {
    int operator()(int64_t lhs, int64_t rhs) const {
        if (lhs < rhs) {
            return -1;
        }
        if (lhs > rhs) {
            return 1;
        }
        return 0;
    }
};
class BPlusTreeTest : public ::testing::Test {
 protected:
    void TearDown() override {
        std::filesystem::remove(test_file_);
    }
    std::string test_file_{"test_b_plus_tree.db"};
};
TEST_F(BPlusTreeTest, EmptyTreeLookupReturnsFalse) {
    std::filesystem::remove(test_file_);
    DiskManager disk_manager(test_file_);
    BufferPoolManager buffer_pool_manager(4, &disk_manager);
    BPlusTree<int64_t, RID, BPlusTreeInt64Comparator> tree(
        "empty_index", &buffer_pool_manager, BPlusTreeInt64Comparator{});
    std::vector<RID> result;
    EXPECT_TRUE(tree.IsEmpty());
    EXPECT_FALSE(tree.GetValue(42, &result));
    EXPECT_TRUE(result.empty());
}
TEST_F(BPlusTreeTest, FindsValueFromRootLeaf) {
    std::filesystem::remove(test_file_);
    DiskManager disk_manager(test_file_);
    BufferPoolManager buffer_pool_manager(8, &disk_manager);
    BPlusTreeInt64Comparator comparator;
    BPlusTree<int64_t, RID, BPlusTreeInt64Comparator> tree(
        "leaf_index", &buffer_pool_manager, comparator);
    page_id_t leaf_page_id;
    auto* page = buffer_pool_manager.NewPage(&leaf_page_id);
    ASSERT_NE(page, nullptr);
    BPlusTreeLeafPage<int64_t, RID, BPlusTreeInt64Comparator> leaf(page->GetData());
    leaf.Init(16);
    EXPECT_TRUE(leaf.Insert(30, RID(3, 1), comparator));
    EXPECT_TRUE(leaf.Insert(10, RID(1, 4), comparator));
    EXPECT_TRUE(leaf.Insert(20, RID(2, 7), comparator));
    tree.UpdateRootPageId(leaf_page_id);
    buffer_pool_manager.UnpinPage(leaf_page_id, true);
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(20, &result));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], RID(2, 7));
    EXPECT_FALSE(tree.GetValue(99, &result));
    EXPECT_TRUE(result.empty());
}
TEST_F(BPlusTreeTest, TraversesInternalRootToMatchingLeaf) {
    std::filesystem::remove(test_file_);
    DiskManager disk_manager(test_file_);
    BufferPoolManager buffer_pool_manager(8, &disk_manager);
    BPlusTreeInt64Comparator comparator;
    BPlusTree<int64_t, RID, BPlusTreeInt64Comparator> tree(
        "internal_index", &buffer_pool_manager, comparator);
    page_id_t left_page_id;
    page_id_t right_page_id;
    page_id_t root_page_id;
    auto* left_page = buffer_pool_manager.NewPage(&left_page_id);
    auto* right_page = buffer_pool_manager.NewPage(&right_page_id);
    auto* root_page = buffer_pool_manager.NewPage(&root_page_id);
    ASSERT_NE(left_page, nullptr);
    ASSERT_NE(right_page, nullptr);
    ASSERT_NE(root_page, nullptr);
    BPlusTreeLeafPage<int64_t, RID, BPlusTreeInt64Comparator> left(left_page->GetData());
    BPlusTreeLeafPage<int64_t, RID, BPlusTreeInt64Comparator> right(right_page->GetData());
    BPlusTreeInternalPage<int64_t, BPlusTreeInt64Comparator> root(root_page->GetData());
    left.Init(16);
    right.Init(16);
    root.Init(16);
    EXPECT_TRUE(left.Insert(10, RID(1, 1), comparator));
    EXPECT_TRUE(left.Insert(30, RID(3, 3), comparator));
    EXPECT_TRUE(right.Insert(50, RID(5, 5), comparator));
    EXPECT_TRUE(right.Insert(70, RID(7, 7), comparator));
    root.PopulateNewRoot(left_page_id, 50, right_page_id);
    tree.UpdateRootPageId(root_page_id);
    buffer_pool_manager.UnpinPage(left_page_id, true);
    buffer_pool_manager.UnpinPage(right_page_id, true);
    buffer_pool_manager.UnpinPage(root_page_id, true);
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(30, &result));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], RID(3, 3));
    EXPECT_TRUE(tree.GetValue(70, &result));
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], RID(7, 7));
    EXPECT_FALSE(tree.GetValue(40, &result));
    EXPECT_TRUE(result.empty());
}
TEST_F(BPlusTreeTest, ReloadsRootPageIdFromHeaderPage) {
    std::filesystem::remove(test_file_);
    page_id_t header_page_id = INVALID_PAGE_ID;
    {
        DiskManager disk_manager(test_file_);
        BufferPoolManager buffer_pool_manager(8, &disk_manager);
        BPlusTreeInt64Comparator comparator;
        BPlusTree<int64_t, RID, BPlusTreeInt64Comparator> tree(
            "persisted_index", &buffer_pool_manager, comparator);
        header_page_id = tree.GetHeaderPageId();
        page_id_t leaf_page_id;
        auto* page = buffer_pool_manager.NewPage(&leaf_page_id);
        ASSERT_NE(page, nullptr);
        BPlusTreeLeafPage<int64_t, RID, BPlusTreeInt64Comparator> leaf(page->GetData());
        leaf.Init(16);
        EXPECT_TRUE(leaf.Insert(42, RID(4, 2), comparator));
        tree.UpdateRootPageId(leaf_page_id);
        buffer_pool_manager.UnpinPage(leaf_page_id, true);
        buffer_pool_manager.FlushAllPages();
    }
    {
        DiskManager disk_manager(test_file_);
        BufferPoolManager buffer_pool_manager(8, &disk_manager);
        BPlusTree<int64_t, RID, BPlusTreeInt64Comparator> tree(
            "persisted_index", &buffer_pool_manager, BPlusTreeInt64Comparator{},
            header_page_id);
        std::vector<RID> result;
        EXPECT_FALSE(tree.IsEmpty());
        EXPECT_TRUE(tree.GetValue(42, &result));
        ASSERT_EQ(result.size(), 1);
        EXPECT_EQ(result[0], RID(4, 2));
    }
}
}  // namespace sothdb
