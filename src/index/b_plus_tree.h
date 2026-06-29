#pragma once
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/types.h"
#include "index/b_plus_tree_internal_page.h"
#include "index/b_plus_tree_leaf_page.h"
#include "index/b_plus_tree_page.h"
#include "storage/buffer_pool_manager.h"
namespace sothdb {
static constexpr uint16_t B_PLUS_TREE_HEADER_ROOT_PAGE_ID_OFFSET = 12;
template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTree {
 public:
    BPlusTree(std::string name, BufferPoolManager* buffer_pool_manager,
              KeyComparator comparator, page_id_t header_page_id = INVALID_PAGE_ID,
              uint16_t leaf_max_size = 0, uint16_t internal_max_size = 0)
        : name_(std::move(name)),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(std::move(comparator)),
          header_page_id_(header_page_id),
          leaf_max_size_(leaf_max_size == 0 ? LeafMaxSize() : leaf_max_size),
          internal_max_size_(internal_max_size == 0 ? InternalMaxSize() : internal_max_size) {
        if (header_page_id_ == INVALID_PAGE_ID) {
            page_id_t new_header_page_id;
            auto* header_page = buffer_pool_manager_->NewPage(&new_header_page_id);
            if (header_page == nullptr) {
                throw std::runtime_error("failed to allocate B+Tree header page");
            }
            header_page_id_ = new_header_page_id;
            WriteRootPageId(header_page, INVALID_PAGE_ID);
            buffer_pool_manager_->UnpinPage(header_page_id_, true);
        } else {
            auto* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
            if (header_page == nullptr) {
                throw std::runtime_error("failed to fetch B+Tree header page");
            }
            root_page_id_ = ReadRootPageId(header_page);
            buffer_pool_manager_->UnpinPage(header_page_id_, false);
        }
    }
    bool GetValue(const KeyType& key, std::vector<ValueType>* result) {
        if (result == nullptr) {
            return false;
        }
        result->clear();
        auto* leaf_page = FindLeafPage(key);
        if (leaf_page == nullptr) {
            return false;
        }
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> leaf(leaf_page->GetData());
        ValueType value;
        auto found = leaf.Lookup(key, &value, comparator_);
        if (found) {
            result->push_back(value);
        }
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
        return found;
    }
    bool Insert(const KeyType& key, const ValueType& value) {
        if (IsEmpty()) {
            StartNewTree(key, value);
            return true;
        }
        return InsertIntoLeaf(key, value);
    }
    bool IsEmpty() const {
        return root_page_id_ == INVALID_PAGE_ID;
    }
    page_id_t GetRootPageId() const {
        return root_page_id_;
    }
    page_id_t GetHeaderPageId() const {
        return header_page_id_;
    }
    void UpdateRootPageId(page_id_t root_page_id) {
        auto* header_page = buffer_pool_manager_->FetchPage(header_page_id_);
        if (header_page == nullptr) {
            throw std::runtime_error("failed to fetch B+Tree header page");
        }
        root_page_id_ = root_page_id;
        WriteRootPageId(header_page, root_page_id_);
        buffer_pool_manager_->UnpinPage(header_page_id_, true);
    }
 private:
    static constexpr uint16_t LeafMaxSize() {
        return static_cast<uint16_t>(
            (PAGE_SIZE - B_PLUS_TREE_PAGE_HEADER_SIZE - sizeof(page_id_t)) /
            (sizeof(KeyType) + sizeof(ValueType)));
    }
    static constexpr uint16_t InternalMaxSize() {
        return static_cast<uint16_t>(
            (PAGE_SIZE - B_PLUS_TREE_PAGE_HEADER_SIZE) /
            (sizeof(KeyType) + sizeof(page_id_t)));
    }
    void StartNewTree(const KeyType& key, const ValueType& value) {
        page_id_t root_page_id;
        auto* root_page = buffer_pool_manager_->NewPage(&root_page_id);
        if (root_page == nullptr) {
            throw std::runtime_error("failed to allocate B+Tree root page");
        }
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> leaf(root_page->GetData());
        leaf.Init(leaf_max_size_);
        leaf.Insert(key, value, comparator_);
        buffer_pool_manager_->UnpinPage(root_page_id, true);
        UpdateRootPageId(root_page_id);
    }
    bool InsertIntoLeaf(const KeyType& key, const ValueType& value) {
        auto* leaf_page = FindLeafPage(key);
        if (leaf_page == nullptr) {
            return false;
        }
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> leaf(leaf_page->GetData());
        auto inserted = leaf.Insert(key, value, comparator_);
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), inserted);
        return inserted;
    }
    static page_id_t ReadRootPageId(Page* header_page) {
        page_id_t root_page_id;
        std::memcpy(&root_page_id,
                    header_page->GetData() + B_PLUS_TREE_HEADER_ROOT_PAGE_ID_OFFSET,
                    sizeof(page_id_t));
        return root_page_id;
    }
    static void WriteRootPageId(Page* header_page, page_id_t root_page_id) {
        std::memcpy(header_page->GetData() + B_PLUS_TREE_HEADER_ROOT_PAGE_ID_OFFSET,
                    &root_page_id, sizeof(page_id_t));
    }
    Page* FindLeafPage(const KeyType& key) {
        if (IsEmpty()) {
            return nullptr;
        }
        auto next_page_id = root_page_id_;
        while (next_page_id != INVALID_PAGE_ID) {
            auto* page = buffer_pool_manager_->FetchPage(next_page_id);
            if (page == nullptr) {
                return nullptr;
            }
            BPlusTreePage tree_page(page->GetData());
            if (tree_page.IsLeafPage()) {
                return page;
            }
            if (!tree_page.IsInternalPage()) {
                buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
                return nullptr;
            }
            BPlusTreeInternalPage<KeyType, KeyComparator> internal(page->GetData());
            next_page_id = internal.Lookup(key, comparator_);
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        }
        return nullptr;
    }
    std::string name_;
    BufferPoolManager* buffer_pool_manager_;
    KeyComparator comparator_;
    page_id_t header_page_id_{INVALID_PAGE_ID};
    uint16_t leaf_max_size_;
    uint16_t internal_max_size_;
    page_id_t root_page_id_{INVALID_PAGE_ID};
};
}  // namespace sothdb
