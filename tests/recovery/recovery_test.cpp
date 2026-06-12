#include "recovery/recovery_manager.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <string>
#include "concurrency/transaction.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include "storage/page.h"

namespace sothdb {

class RecoveryTest : public ::testing::Test {
 protected:
    void SetUp() override {
        db_file_ = "test_recovery.db";
        wal_file_ = "test_recovery.wal";
        std::filesystem::remove(db_file_);
        std::filesystem::remove(wal_file_);
    }
    void TearDown() override {
        std::filesystem::remove(db_file_);
        std::filesystem::remove(wal_file_);
    }
    static constexpr size_t kPoolSize = 10;
    std::string db_file_;
    std::string wal_file_;
};

TEST_F(RecoveryTest, CommittedInsertSurvivesCrash) {
    const std::string payload = "committed-row";
    page_id_t pid;
    slot_id_t slot;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn(1);
        rm.LogBegin(&txn);
        auto* page = bpm.NewPage(&pid);
        // Flush clean page to disk so it exists; sets dirty=false,
        // so BPM destructor won't write the post-insert state.
        bpm.FlushPage(pid);
        slot = page->InsertTuple(payload.c_str(),
                                 static_cast<uint16_t>(payload.size()));
        ASSERT_NE(slot, INVALID_SLOT);
        auto lsn = rm.LogInsert(&txn, pid, slot, payload.c_str(),
                                static_cast<uint16_t>(payload.size()));
        page->SetLsn(lsn);
        rm.LogCommit(&txn);
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [data, len] = page->GetTuple(slot);
        ASSERT_NE(data, nullptr);
        EXPECT_EQ(len, payload.size());
        EXPECT_EQ(std::string(data, len), payload);
        bpm.UnpinPage(pid, false);
    }
}

TEST_F(RecoveryTest, UncommittedInsertNotPresent) {
    const std::string payload = "uncommitted-row";
    page_id_t pid;
    slot_id_t slot;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn(1);
        rm.LogBegin(&txn);
        auto* page = bpm.NewPage(&pid);
        bpm.FlushPage(pid);
        slot = page->InsertTuple(payload.c_str(),
                                 static_cast<uint16_t>(payload.size()));
        ASSERT_NE(slot, INVALID_SLOT);
        auto lsn = rm.LogInsert(&txn, pid, slot, payload.c_str(),
                                static_cast<uint16_t>(payload.size()));
        page->SetLsn(lsn);
        // No commit — flush WAL so log records survive the crash
        lm.Flush();
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [data, len] = page->GetTuple(slot);
        EXPECT_TRUE(data == nullptr || len == 0);
        bpm.UnpinPage(pid, false);
    }
}

TEST_F(RecoveryTest, UncommittedInsertUndoneFromDisk) {
    const std::string payload = "dirty-uncommitted";
    page_id_t pid;
    slot_id_t slot;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn(1);
        rm.LogBegin(&txn);
        auto* page = bpm.NewPage(&pid);
        slot = page->InsertTuple(payload.c_str(),
                                 static_cast<uint16_t>(payload.size()));
        ASSERT_NE(slot, INVALID_SLOT);
        auto lsn = rm.LogInsert(&txn, pid, slot, payload.c_str(),
                                static_cast<uint16_t>(payload.size()));
        page->SetLsn(lsn);
        // Flush page WITH uncommitted data — forces actual undo during recovery
        bpm.FlushPage(pid);
        lm.Flush();
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [data, len] = page->GetTuple(slot);
        EXPECT_TRUE(data == nullptr || len == 0);
        bpm.UnpinPage(pid, false);
    }
}

TEST_F(RecoveryTest, MixedCommittedAndUncommitted) {
    const std::string committed_data = "committed-row";
    const std::string uncommitted_data = "uncommitted-row";
    page_id_t pid;
    slot_id_t committed_slot;
    slot_id_t uncommitted_slot;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn1(1);
        rm.LogBegin(&txn1);
        auto* page = bpm.NewPage(&pid);
        bpm.FlushPage(pid);
        committed_slot = page->InsertTuple(
            committed_data.c_str(),
            static_cast<uint16_t>(committed_data.size()));
        ASSERT_NE(committed_slot, INVALID_SLOT);
        auto lsn1 = rm.LogInsert(
            &txn1, pid, committed_slot, committed_data.c_str(),
            static_cast<uint16_t>(committed_data.size()));
        page->SetLsn(lsn1);
        rm.LogCommit(&txn1);
        Transaction txn2(2);
        rm.LogBegin(&txn2);
        uncommitted_slot = page->InsertTuple(
            uncommitted_data.c_str(),
            static_cast<uint16_t>(uncommitted_data.size()));
        ASSERT_NE(uncommitted_slot, INVALID_SLOT);
        auto lsn2 = rm.LogInsert(
            &txn2, pid, uncommitted_slot, uncommitted_data.c_str(),
            static_cast<uint16_t>(uncommitted_data.size()));
        page->SetLsn(lsn2);
        lm.Flush();
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [d1, l1] = page->GetTuple(committed_slot);
        ASSERT_NE(d1, nullptr);
        EXPECT_EQ(std::string(d1, l1), committed_data);
        auto [d2, l2] = page->GetTuple(uncommitted_slot);
        EXPECT_TRUE(d2 == nullptr || l2 == 0);
        bpm.UnpinPage(pid, false);
    }
}

TEST_F(RecoveryTest, CommittedUpdateSurvives) {
    const std::string original = "original";
    const std::string updated = "updated!";
    page_id_t pid;
    slot_id_t slot;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn1(1);
        rm.LogBegin(&txn1);
        auto* page = bpm.NewPage(&pid);
        slot = page->InsertTuple(original.c_str(),
                                 static_cast<uint16_t>(original.size()));
        ASSERT_NE(slot, INVALID_SLOT);
        auto lsn1 = rm.LogInsert(&txn1, pid, slot, original.c_str(),
                                 static_cast<uint16_t>(original.size()));
        page->SetLsn(lsn1);
        rm.LogCommit(&txn1);
        // Flush page with committed insert so it's durable on disk
        bpm.FlushPage(pid);
        Transaction txn2(2);
        rm.LogBegin(&txn2);
        auto lsn2 = rm.LogUpdate(&txn2, pid, slot,
                                 original.c_str(),
                                 static_cast<uint16_t>(original.size()),
                                 updated.c_str(),
                                 static_cast<uint16_t>(updated.size()));
        page->SetLsn(lsn2);
        rm.LogCommit(&txn2);
        // Update only in WAL, not flushed to data page
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [data, len] = page->GetTuple(slot);
        ASSERT_NE(data, nullptr);
        EXPECT_EQ(std::string(data, len), updated);
        bpm.UnpinPage(pid, false);
    }
}

TEST_F(RecoveryTest, CommittedDeleteSurvives) {
    const std::string payload = "to-be-deleted";
    page_id_t pid;
    slot_id_t slot;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn1(1);
        rm.LogBegin(&txn1);
        auto* page = bpm.NewPage(&pid);
        slot = page->InsertTuple(payload.c_str(),
                                 static_cast<uint16_t>(payload.size()));
        ASSERT_NE(slot, INVALID_SLOT);
        auto lsn1 = rm.LogInsert(&txn1, pid, slot, payload.c_str(),
                                 static_cast<uint16_t>(payload.size()));
        page->SetLsn(lsn1);
        rm.LogCommit(&txn1);
        bpm.FlushPage(pid);
        Transaction txn2(2);
        rm.LogBegin(&txn2);
        auto lsn2 = rm.LogDelete(&txn2, pid, slot, payload.c_str(),
                                 static_cast<uint16_t>(payload.size()));
        page->SetLsn(lsn2);
        rm.LogCommit(&txn2);
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [data, len] = page->GetTuple(slot);
        EXPECT_TRUE(data == nullptr || len == 0);
        bpm.UnpinPage(pid, false);
    }
}

TEST_F(RecoveryTest, AbortedTransactionIgnored) {
    const std::string payload = "aborted-row";
    page_id_t pid;
    slot_id_t slot;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn(1);
        rm.LogBegin(&txn);
        auto* page = bpm.NewPage(&pid);
        bpm.FlushPage(pid);
        slot = page->InsertTuple(payload.c_str(),
                                 static_cast<uint16_t>(payload.size()));
        ASSERT_NE(slot, INVALID_SLOT);
        auto lsn = rm.LogInsert(&txn, pid, slot, payload.c_str(),
                                static_cast<uint16_t>(payload.size()));
        page->SetLsn(lsn);
        rm.LogAbort(&txn);
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [data, len] = page->GetTuple(slot);
        EXPECT_TRUE(data == nullptr || len == 0);
        bpm.UnpinPage(pid, false);
    }
}

TEST_F(RecoveryTest, MultipleUncommittedOpsUndone) {
    const std::string data1 = "first-uncommitted";
    const std::string data2 = "second-uncommitted";
    page_id_t pid;
    slot_id_t slot1;
    slot_id_t slot2;
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        Transaction txn(1);
        rm.LogBegin(&txn);
        auto* page = bpm.NewPage(&pid);
        slot1 = page->InsertTuple(data1.c_str(),
                                  static_cast<uint16_t>(data1.size()));
        ASSERT_NE(slot1, INVALID_SLOT);
        auto lsn1 = rm.LogInsert(&txn, pid, slot1, data1.c_str(),
                                 static_cast<uint16_t>(data1.size()));
        page->SetLsn(lsn1);
        slot2 = page->InsertTuple(data2.c_str(),
                                  static_cast<uint16_t>(data2.size()));
        ASSERT_NE(slot2, INVALID_SLOT);
        auto lsn2 = rm.LogInsert(&txn, pid, slot2, data2.c_str(),
                                 static_cast<uint16_t>(data2.size()));
        page->SetLsn(lsn2);
        // Flush page WITH uncommitted data — tests prev_lsn chain undo
        bpm.FlushPage(pid);
        lm.Flush();
    }
    {
        DiskManager dm(db_file_);
        LogManager lm(wal_file_);
        BufferPoolManager bpm(kPoolSize, &dm);
        RecoveryManager rm(&lm, &bpm);
        rm.Recover();
        auto* page = bpm.FetchPage(pid);
        ASSERT_NE(page, nullptr);
        auto [d1, l1] = page->GetTuple(slot1);
        EXPECT_TRUE(d1 == nullptr || l1 == 0);
        auto [d2, l2] = page->GetTuple(slot2);
        EXPECT_TRUE(d2 == nullptr || l2 == 0);
        bpm.UnpinPage(pid, false);
    }
}

}  // namespace sothdb
