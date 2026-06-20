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
              KeyComparator comparator, page_id_t header_page_id = INVALID_PAGE_ID)
        : name_(std::move(name)),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(std::move(comparator)),
          header_page_id_(header_page_id) {
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
    page_id_t root_page_id_{INVALID_PAGE_ID};
};
}  // namespace sothdb
