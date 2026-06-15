#include "index/b_plus_tree_internal_page.h"
#include <gtest/gtest.h>
#include <cstdint>
#include "storage/page.h"
namespace sothdb {
struct Int64Comparator {
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
TEST(BPlusTreeInternalPageTest, PopulateNewRootPreservesStorageHeader) {
    Page page;
    page.Init(7);
    page.SetLsn(123);
    BPlusTreeInternalPage<int64_t, Int64Comparator> internal(page.GetData());
    internal.Init(8);
    internal.PopulateNewRoot(10, 50, 20);
    EXPECT_EQ(page.GetPageId(), 7);
    EXPECT_EQ(page.GetLsn(), 123);
    EXPECT_TRUE(internal.IsInternalPage());
    EXPECT_EQ(internal.GetSize(), 2);
    EXPECT_EQ(internal.GetMaxSize(), 8);
    EXPECT_EQ(internal.ValueAt(0), 10);
    EXPECT_EQ(internal.KeyAt(1), 50);
    EXPECT_EQ(internal.ValueAt(1), 20);
}
TEST(BPlusTreeInternalPageTest, LookupChoosesChildBySeparatorKeys) {
    Page page;
    page.Init(1);
    BPlusTreeInternalPage<int64_t, Int64Comparator> internal(page.GetData());
    internal.Init(8);
    internal.PopulateNewRoot(10, 50, 20);
    internal.InsertAfter(20, 80, 30);
    Int64Comparator comparator;
    EXPECT_EQ(internal.Lookup(1, comparator), 10);
    EXPECT_EQ(internal.Lookup(49, comparator), 10);
    EXPECT_EQ(internal.Lookup(50, comparator), 20);
    EXPECT_EQ(internal.Lookup(79, comparator), 20);
    EXPECT_EQ(internal.Lookup(80, comparator), 30);
    EXPECT_EQ(internal.Lookup(120, comparator), 30);
}
TEST(BPlusTreeInternalPageTest, InsertAfterPlacesNewChildAfterExistingPointer) {
    Page page;
    page.Init(2);
    BPlusTreeInternalPage<int64_t, Int64Comparator> internal(page.GetData());
    internal.Init(8);
    internal.PopulateNewRoot(10, 50, 20);
    EXPECT_EQ(internal.InsertAfter(10, 25, 15), 3);
    EXPECT_EQ(internal.ValueAt(0), 10);
    EXPECT_EQ(internal.KeyAt(1), 25);
    EXPECT_EQ(internal.ValueAt(1), 15);
    EXPECT_EQ(internal.KeyAt(2), 50);
    EXPECT_EQ(internal.ValueAt(2), 20);
    EXPECT_EQ(internal.InsertAfter(999, 70, 30), 3);
}
TEST(BPlusTreeInternalPageTest, MoveHalfToAppendsMovedEntriesToRecipient) {
    Page source_page;
    Page recipient_page;
    source_page.Init(3);
    recipient_page.Init(4);
    BPlusTreeInternalPage<int64_t, Int64Comparator> source(source_page.GetData());
    BPlusTreeInternalPage<int64_t, Int64Comparator> recipient(recipient_page.GetData());
    source.Init(8);
    recipient.Init(8);
    source.PopulateNewRoot(10, 20, 20);
    source.InsertAfter(20, 40, 30);
    source.InsertAfter(30, 60, 40);
    source.InsertAfter(40, 80, 50);
    source.MoveHalfTo(&recipient);
    EXPECT_EQ(source.GetSize(), 2);
    EXPECT_EQ(recipient.GetSize(), 3);
    EXPECT_EQ(source.ValueAt(0), 10);
    EXPECT_EQ(source.KeyAt(1), 20);
    EXPECT_EQ(source.ValueAt(1), 20);
    EXPECT_EQ(recipient.KeyAt(0), 40);
    EXPECT_EQ(recipient.ValueAt(0), 30);
    EXPECT_EQ(recipient.KeyAt(1), 60);
    EXPECT_EQ(recipient.ValueAt(1), 40);
    EXPECT_EQ(recipient.KeyAt(2), 80);
    EXPECT_EQ(recipient.ValueAt(2), 50);
}
}  // namespace sothdb
