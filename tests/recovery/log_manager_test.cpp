#include "recovery/log_manager.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <string>

namespace sothdb {

class LogManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        wal_file_ = "test_log_manager.wal";
        std::filesystem::remove(wal_file_);
    }
    void TearDown() override {
        std::filesystem::remove(wal_file_);
    }
    std::string wal_file_;
};

TEST_F(LogManagerTest, AppendAndLsnOrdering) {
    LogManager lm(wal_file_);
    EXPECT_EQ(lm.GetNextLsn(), 1);
    auto begin = LogRecord::MakeBegin(1, INVALID_LSN);
    lsn_t lsn1 = lm.AppendLogRecord(begin);
    auto insert = LogRecord::MakeInsert(1, lsn1, 0, 0, "abc", 3);
    lsn_t lsn2 = lm.AppendLogRecord(insert);
    auto commit = LogRecord::MakeCommit(1, lsn2);
    lsn_t lsn3 = lm.AppendLogRecord(commit);
    EXPECT_EQ(lsn1, 1);
    EXPECT_LT(lsn1, lsn2);
    EXPECT_LT(lsn2, lsn3);
    EXPECT_EQ(lm.GetNextLsn(), lsn3 + 1);
}

TEST_F(LogManagerTest, FlushAndIterator) {
    LogManager lm(wal_file_);
    EXPECT_EQ(lm.GetFlushedLsn(), INVALID_LSN);
    auto begin = LogRecord::MakeBegin(10, INVALID_LSN);
    lsn_t lsn1 = lm.AppendLogRecord(begin);
    std::string payload = "hello wal";
    auto insert = LogRecord::MakeInsert(10, lsn1, 5, 2,
                                        payload.c_str(),
                                        static_cast<uint16_t>(payload.size()));
    lsn_t lsn2 = lm.AppendLogRecord(insert);
    auto commit = LogRecord::MakeCommit(10, lsn2);
    lsn_t lsn3 = lm.AppendLogRecord(commit);
    lm.Flush(lsn3);
    EXPECT_EQ(lm.GetFlushedLsn(), lsn3);
    auto iter = lm.MakeIterator();
    ASSERT_TRUE(iter.HasNext());
    auto r1 = iter.Next();
    EXPECT_EQ(r1.GetType(), LogRecordType::BEGIN);
    EXPECT_EQ(r1.GetLsn(), lsn1);
    ASSERT_TRUE(iter.HasNext());
    auto r2 = iter.Next();
    EXPECT_EQ(r2.GetType(), LogRecordType::INSERT);
    EXPECT_EQ(r2.GetPageId(), 5);
    EXPECT_EQ(std::string(r2.GetNewData().data(), r2.GetNewData().size()), payload);
    ASSERT_TRUE(iter.HasNext());
    auto r3 = iter.Next();
    EXPECT_EQ(r3.GetType(), LogRecordType::COMMIT);
    EXPECT_EQ(r3.GetLsn(), lsn3);
    EXPECT_FALSE(iter.HasNext());
}

TEST_F(LogManagerTest, ReopenResumesLsn) {
    lsn_t last_lsn;
    {
        LogManager lm(wal_file_);
        auto begin = LogRecord::MakeBegin(1, INVALID_LSN);
        lm.AppendLogRecord(begin);
        auto commit = LogRecord::MakeCommit(1, 1);
        last_lsn = lm.AppendLogRecord(commit);
    }
    LogManager lm2(wal_file_);
    EXPECT_EQ(lm2.GetNextLsn(), last_lsn + 1);
    EXPECT_EQ(lm2.GetFlushedLsn(), last_lsn);
    auto rec = LogRecord::MakeBegin(2, INVALID_LSN);
    lsn_t new_lsn = lm2.AppendLogRecord(rec);
    EXPECT_EQ(new_lsn, last_lsn + 1);
}

TEST_F(LogManagerTest, DestructorFlushesBuffer) {
    {
        LogManager lm(wal_file_);
        auto begin = LogRecord::MakeBegin(1, INVALID_LSN);
        lm.AppendLogRecord(begin);
        auto insert = LogRecord::MakeInsert(1, 1, 0, 0, "data", 4);
        lm.AppendLogRecord(insert);
    }
    LogIterator iter(wal_file_);
    int count = 0;
    while (iter.HasNext()) {
        iter.Next();
        count++;
    }
    EXPECT_EQ(count, 2);
}

TEST_F(LogManagerTest, BufferOverflowAutoFlush) {
    LogManager lm(wal_file_);
    std::string big(1024, 'Z');
    lsn_t prev = INVALID_LSN;
    for (int i = 0; i < 20; i++) {
        auto rec = LogRecord::MakeInsert(1, prev, 0, static_cast<slot_id_t>(i),
                                         big.c_str(),
                                         static_cast<uint16_t>(big.size()));
        prev = lm.AppendLogRecord(rec);
    }
    EXPECT_GT(lm.GetFlushedLsn(), INVALID_LSN);
    auto iter = lm.MakeIterator();
    int read_count = 0;
    lsn_t prev_lsn = 0;
    while (iter.HasNext()) {
        auto rec = iter.Next();
        EXPECT_GT(rec.GetLsn(), prev_lsn);
        prev_lsn = rec.GetLsn();
        read_count++;
    }
    EXPECT_EQ(read_count, 20);
}

}  // namespace sothdb
