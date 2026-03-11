#include "storage/lru_replacer.h"
#include <gtest/gtest.h>

namespace sothdb {

TEST(LRUReplacerTest, UnpinAndVictim) {
    LRUReplacer replacer(4);
    replacer.Unpin(0);
    replacer.Unpin(1);
    replacer.Unpin(2);
    EXPECT_EQ(replacer.Size(), 3);
    frame_id_t victim;
    EXPECT_TRUE(replacer.Victim(&victim));
    EXPECT_EQ(victim, 0);
    EXPECT_EQ(replacer.Size(), 2);
}

TEST(LRUReplacerTest, PinRemovesFromCandidates) {
    LRUReplacer replacer(4);
    replacer.Unpin(0);
    replacer.Unpin(1);
    replacer.Pin(0);
    EXPECT_EQ(replacer.Size(), 1);
    frame_id_t victim;
    EXPECT_TRUE(replacer.Victim(&victim));
    EXPECT_EQ(victim, 1);
}

TEST(LRUReplacerTest, EvictionOrder) {
    LRUReplacer replacer(10);
    replacer.Unpin(3);
    replacer.Unpin(7);
    replacer.Unpin(1);
    frame_id_t v;
    replacer.Victim(&v);
    EXPECT_EQ(v, 3);
    replacer.Victim(&v);
    EXPECT_EQ(v, 7);
    replacer.Victim(&v);
    EXPECT_EQ(v, 1);
}

}
