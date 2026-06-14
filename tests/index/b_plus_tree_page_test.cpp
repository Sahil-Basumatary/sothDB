#include "index/b_plus_tree_page.h"
#include <gtest/gtest.h>
#include "common/rid.h"
#include "storage/page.h"

namespace sothdb {

TEST(BPlusTreePageTest, RidTracksHeapTupleLocation) {
    RID invalid;
    EXPECT_FALSE(invalid.IsValid());
    RID rid(7, 3);
    EXPECT_TRUE(rid.IsValid());
    EXPECT_EQ(rid.page_id, 7);
    EXPECT_EQ(rid.slot_id, 3);
    EXPECT_EQ(rid, RID(7, 3));
    EXPECT_NE(rid, RID(7, 4));
}

TEST(BPlusTreePageTest, HeaderUsesIndexRegionOnly) {
    Page page;
    page.Init(42);
    page.SetLsn(99);
    BPlusTreePage index_page(page.GetData());
    index_page.Init(BPlusTreePageType::LEAF, 128);
    EXPECT_EQ(page.GetPageId(), 42);
    EXPECT_EQ(page.GetLsn(), 99);
    EXPECT_TRUE(index_page.IsLeafPage());
    EXPECT_FALSE(index_page.IsInternalPage());
    EXPECT_EQ(index_page.GetSize(), 0);
    EXPECT_EQ(index_page.GetMaxSize(), 128);
    index_page.SetSize(5);
    index_page.IncreaseSize(2);
    EXPECT_EQ(index_page.GetSize(), 7);
    index_page.SetPageType(BPlusTreePageType::INTERNAL);
    EXPECT_TRUE(index_page.IsInternalPage());
}

}  // namespace sothdb
