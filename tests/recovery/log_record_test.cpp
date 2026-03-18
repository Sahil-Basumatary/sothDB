#include "recovery/log_record.h"
#include <gtest/gtest.h>
#include <cstring>
#include <string>

namespace sothdb {

TEST(LogRecordTest, BeginRoundTrip) {
    auto rec = LogRecord::MakeBegin(42, INVALID_LSN);
    rec.SetLsn(100);
    auto buf = rec.Serialize();
    ASSERT_EQ(buf.size(), LOG_HEADER_SIZE);
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::BEGIN);
    EXPECT_EQ(restored.GetTxnId(), 42);
    EXPECT_EQ(restored.GetLsn(), 100);
    EXPECT_EQ(restored.GetPrevLsn(), INVALID_LSN);
    EXPECT_EQ(restored.GetSize(), LOG_HEADER_SIZE);
}

TEST(LogRecordTest, CommitRoundTrip) {
    auto rec = LogRecord::MakeCommit(7, 100);
    rec.SetLsn(200);
    auto buf = rec.Serialize();
    ASSERT_EQ(buf.size(), LOG_HEADER_SIZE);
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::COMMIT);
    EXPECT_EQ(restored.GetTxnId(), 7);
    EXPECT_EQ(restored.GetLsn(), 200);
    EXPECT_EQ(restored.GetPrevLsn(), 100);
}

TEST(LogRecordTest, AbortRoundTrip) {
    auto rec = LogRecord::MakeAbort(13, 50);
    rec.SetLsn(75);
    auto buf = rec.Serialize();
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::ABORT);
    EXPECT_EQ(restored.GetTxnId(), 13);
    EXPECT_EQ(restored.GetLsn(), 75);
    EXPECT_EQ(restored.GetPrevLsn(), 50);
}

TEST(LogRecordTest, InsertRoundTrip) {
    std::string payload = "row data for insert";
    auto rec = LogRecord::MakeInsert(5, 10, 3, 1,
                                     payload.c_str(),
                                     static_cast<uint16_t>(payload.size()));
    rec.SetLsn(20);
    auto buf = rec.Serialize();
    uint32_t expected_size = LOG_HEADER_SIZE + sizeof(page_id_t)
                             + sizeof(slot_id_t) + sizeof(uint16_t)
                             + static_cast<uint32_t>(payload.size());
    ASSERT_EQ(buf.size(), expected_size);
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::INSERT);
    EXPECT_EQ(restored.GetTxnId(), 5);
    EXPECT_EQ(restored.GetLsn(), 20);
    EXPECT_EQ(restored.GetPrevLsn(), 10);
    EXPECT_EQ(restored.GetPageId(), 3);
    EXPECT_EQ(restored.GetSlotId(), 1);
    ASSERT_EQ(restored.GetNewData().size(), payload.size());
    EXPECT_EQ(std::string(restored.GetNewData().data(),
                           restored.GetNewData().size()), payload);
}

TEST(LogRecordTest, DeleteRoundTrip) {
    std::string old_payload = "row being deleted";
    auto rec = LogRecord::MakeDelete(9, 30, 7, 4,
                                     old_payload.c_str(),
                                     static_cast<uint16_t>(old_payload.size()));
    rec.SetLsn(40);
    auto buf = rec.Serialize();
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::DELETE);
    EXPECT_EQ(restored.GetTxnId(), 9);
    EXPECT_EQ(restored.GetLsn(), 40);
    EXPECT_EQ(restored.GetPrevLsn(), 30);
    EXPECT_EQ(restored.GetPageId(), 7);
    EXPECT_EQ(restored.GetSlotId(), 4);
    ASSERT_EQ(restored.GetOldData().size(), old_payload.size());
    EXPECT_EQ(std::string(restored.GetOldData().data(),
                           restored.GetOldData().size()), old_payload);
}

TEST(LogRecordTest, UpdateRoundTrip) {
    std::string old_val = "old value";
    std::string new_val = "new value that is longer";
    auto rec = LogRecord::MakeUpdate(2, 55, 10, 3,
                                     old_val.c_str(),
                                     static_cast<uint16_t>(old_val.size()),
                                     new_val.c_str(),
                                     static_cast<uint16_t>(new_val.size()));
    rec.SetLsn(60);
    auto buf = rec.Serialize();
    uint32_t expected_size = LOG_HEADER_SIZE + sizeof(page_id_t)
                             + sizeof(slot_id_t)
                             + sizeof(uint16_t) + static_cast<uint32_t>(old_val.size())
                             + sizeof(uint16_t) + static_cast<uint32_t>(new_val.size());
    ASSERT_EQ(buf.size(), expected_size);
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::UPDATE);
    EXPECT_EQ(restored.GetTxnId(), 2);
    EXPECT_EQ(restored.GetLsn(), 60);
    EXPECT_EQ(restored.GetPrevLsn(), 55);
    EXPECT_EQ(restored.GetPageId(), 10);
    EXPECT_EQ(restored.GetSlotId(), 3);
    EXPECT_EQ(std::string(restored.GetOldData().data(),
                           restored.GetOldData().size()), old_val);
    EXPECT_EQ(std::string(restored.GetNewData().data(),
                           restored.GetNewData().size()), new_val);
}

TEST(LogRecordTest, InsertEmptyPayload) {
    auto rec = LogRecord::MakeInsert(1, INVALID_LSN, 0, 0, nullptr, 0);
    rec.SetLsn(1);
    auto buf = rec.Serialize();
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::INSERT);
    EXPECT_TRUE(restored.GetNewData().empty());
}

TEST(LogRecordTest, UpdateEmptyOldAndNewData) {
    auto rec = LogRecord::MakeUpdate(1, INVALID_LSN, 5, 2,
                                     nullptr, 0, nullptr, 0);
    rec.SetLsn(1);
    auto buf = rec.Serialize();
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    EXPECT_EQ(restored.GetType(), LogRecordType::UPDATE);
    EXPECT_TRUE(restored.GetOldData().empty());
    EXPECT_TRUE(restored.GetNewData().empty());
}

TEST(LogRecordTest, PrevLsnChaining) {
    auto begin = LogRecord::MakeBegin(1, INVALID_LSN);
    begin.SetLsn(100);
    auto insert = LogRecord::MakeInsert(1, 100, 0, 0, "x", 1);
    insert.SetLsn(200);
    auto commit = LogRecord::MakeCommit(1, 200);
    commit.SetLsn(300);
    auto buf1 = begin.Serialize();
    auto buf2 = insert.Serialize();
    auto buf3 = commit.Serialize();
    auto r1 = LogRecord::Deserialize(buf1.data(), buf1.size());
    auto r2 = LogRecord::Deserialize(buf2.data(), buf2.size());
    auto r3 = LogRecord::Deserialize(buf3.data(), buf3.size());
    EXPECT_EQ(r1.GetPrevLsn(), INVALID_LSN);
    EXPECT_EQ(r2.GetPrevLsn(), 100);
    EXPECT_EQ(r3.GetPrevLsn(), 200);
}

TEST(LogRecordTest, LargePayloadRoundTrip) {
    std::string big(2048, 'A');
    auto rec = LogRecord::MakeInsert(1, INVALID_LSN, 0, 0,
                                     big.c_str(),
                                     static_cast<uint16_t>(big.size()));
    rec.SetLsn(1);
    auto buf = rec.Serialize();
    auto restored = LogRecord::Deserialize(buf.data(), buf.size());
    ASSERT_EQ(restored.GetNewData().size(), big.size());
    EXPECT_EQ(std::string(restored.GetNewData().data(),
                           restored.GetNewData().size()), big);
}

TEST(LogRecordTest, DeserializeTooSmallThrows) {
    char tiny[10] = {};
    EXPECT_THROW(LogRecord::Deserialize(tiny, sizeof(tiny)), std::runtime_error);
}

TEST(LogRecordTest, DeserializeSizeMismatchThrows) {
    auto rec = LogRecord::MakeBegin(1, INVALID_LSN);
    auto buf = rec.Serialize();
    EXPECT_THROW(LogRecord::Deserialize(buf.data(), buf.size() + 1),
                 std::runtime_error);
}

}  // namespace sothdb
