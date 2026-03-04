//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  result->clear();

  // Get root page ID
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.template As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;
  header_guard.Drop();

  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // Optimistic latch coupling
  bool success = false;
  while (!success) {
    // Start with root page
    auto page_guard = bpm_->ReadPage(root_page_id);
    auto page = page_guard.template As<BPlusTreePage>();

    while (!page->IsLeafPage()) {
      auto internal_page = page_guard.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

      // Find the child to follow
      // For key k and internal keys [1]=sep1, [2]=sep2, ...
      // if k < sep1 return child 0
      // if sep1 <= k < sep2 return child 1
      // if sep2 <= k return child 2 (last child)
      int child_index = 0;
      for (int i = 1; i < internal_page->GetSize(); i++) {
        if (comparator_(key, internal_page->KeyAt(i)) < 0) {
          child_index = i - 1;
          break;
        }
        child_index = i;
      }

      auto child_page_id = internal_page->ValueAt(child_index);

      // Try to get child page with CheckedReadPage
      auto child_guard_opt = bpm_->CheckedReadPage(child_page_id);

      if (!child_guard_opt.has_value()) {
        // Child is not safe, retry from root
        break;
      }

      // Child is safe, release parent latch
      page_guard.Drop();
      page_guard = std::move(child_guard_opt.value());
      page = page_guard.template As<BPlusTreePage>();
    }

    // Check if we reached the leaf page successfully
    if (page->IsLeafPage()) {
      // Now at leaf page, search for key
      auto leaf_page = page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

      // Binary search for key
      int left = 0;
      int right = leaf_page->GetSize() - 1;
      bool found = false;
      int found_index = -1;

      while (left <= right) {
        int mid = left + (right - left) / 2;
        int cmp = comparator_(leaf_page->KeyAt(mid), key);
        if (cmp == 0) {
          found = true;
          found_index = mid;
          break;
        } else if (cmp < 0) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }

      if (found) {
        result->push_back(leaf_page->ValueAt(found_index));
        success = true;
      } else {
        success = true;
      }
    }
    // If we broke out of the loop before reaching a leaf, we'll retry
  }

  return !result->empty();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // For write operations, we need to hold write latches on all pages in the access path
  // This is necessary because pages might split or merge during insertion

  // Declaration of context instance.
  Context ctx;

  // Acquire write lock on header page
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // Handle empty tree
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    page_id_t new_root_page = bpm_->NewPage();
    auto new_root_guard = bpm_->WritePage(new_root_page);
    auto new_leaf = new_root_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    new_leaf->Init(leaf_max_size_);
    new_leaf->SetKeyAt(0, key);
    new_leaf->SetValueAt(0, value);
    new_leaf->SetSize(1);
    header_page->root_page_id_ = new_root_page;
    new_root_guard.Drop();
    ctx.header_page_.value().Drop();
    ctx.header_page_ = std::nullopt;
    return true;
  }

  // Traverse down to leaf page, holding write latches on all pages
  page_id_t current_page_id = ctx.root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(current_page_id));
  auto current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();

  while (!current_page->IsLeafPage()) {
    auto internal_page = ctx.write_set_.back().template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

    // Find the child to follow
    int child_index = 0;
    for (int i = 1; i < internal_page->GetSize(); i++) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        child_index = i - 1;
        break;
      }
      child_index = i;
    }

    auto child_page_id = internal_page->ValueAt(child_index);
    ctx.write_set_.push_back(bpm_->WritePage(child_page_id));
    current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  }

  // Now at leaf page
  auto leaf_page = ctx.write_set_.back().template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  // Check for duplicate key
  int left = 0;
  int right = leaf_page->GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_(leaf_page->KeyAt(mid), key);
    if (cmp == 0) {
      // Duplicate key found
      while (!ctx.write_set_.empty()) {
        ctx.write_set_.pop_back();
      }
      if (ctx.header_page_.has_value()) {
        ctx.header_page_.value().Drop();
        ctx.header_page_ = std::nullopt;
      }
      return false;
    } else if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  // Check if leaf is full and needs splitting
  if (leaf_page->GetSize() >= leaf_page->GetMaxSize()) {
    // Split leaf page
    page_id_t new_leaf_id = bpm_->NewPage();
    auto new_leaf_guard = bpm_->WritePage(new_leaf_id);
    auto new_leaf = new_leaf_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    new_leaf->Init(leaf_max_size_);

    int mid = leaf_page->GetSize() / 2;
    int orig_size = leaf_page->GetSize();

    // Move second half to new leaf
    for (int i = mid; i < orig_size; i++) {
      new_leaf->SetKeyAt(i - mid, leaf_page->KeyAt(i));
      new_leaf->SetValueAt(i - mid, leaf_page->ValueAt(i));
    }
    new_leaf->SetSize(orig_size - mid);
    leaf_page->SetSize(mid);

    // Link leaves
    new_leaf->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_leaf_id);

    // Get first key of new leaf BEFORE dropping guard
    KeyType new_leaf_first_key = new_leaf->KeyAt(0);
    page_id_t new_leaf_page_id = new_leaf_id;

    // Determine where to insert (left holds the position from before split)
    if (comparator_(key, new_leaf_first_key) < 0) {
      // Insert into original leaf
      // Recalculate insert position since leaf size changed after split
      int insert_pos = left;
      if (insert_pos > mid) {
        insert_pos = mid;
      }
      for (int i = leaf_page->GetSize(); i > insert_pos; i--) {
        leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
        leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
      }
      leaf_page->SetKeyAt(insert_pos, key);
      leaf_page->SetValueAt(insert_pos, value);
      leaf_page->ChangeSizeBy(1);
    } else {
      // Insert into new leaf
      int new_pos = 0;
      while (new_pos < new_leaf->GetSize() && comparator_(new_leaf->KeyAt(new_pos), key) < 0) {
        new_pos++;
      }
      // Need to shift elements to make room and add the new element
      // First increase size to accommodate new element
      new_leaf->SetSize(new_leaf->GetSize() + 1);
      // Then shift elements
      for (int i = new_leaf->GetSize() - 1; i > new_pos; i--) {
        new_leaf->SetKeyAt(i, new_leaf->KeyAt(i - 1));
        new_leaf->SetValueAt(i, new_leaf->ValueAt(i - 1));
      }
      new_leaf->SetKeyAt(new_pos, key);
      new_leaf->SetValueAt(new_pos, value);
    }

    new_leaf_guard.Drop();
    ctx.write_set_.pop_back();  // Remove leaf from write_set

    // Insert into parent
    KeyType parent_key = new_leaf_first_key;
    page_id_t new_child_id = new_leaf_page_id;

    while (!ctx.write_set_.empty()) {
      auto parent_page = ctx.write_set_.back().template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      ctx.write_set_.pop_back();

      // Check if parent is full
      if (parent_page->GetSize() >= parent_page->GetMaxSize()) {
        // Split internal page
        page_id_t new_internal_id = bpm_->NewPage();
        auto new_internal_guard = bpm_->WritePage(new_internal_id);
        auto new_internal = new_internal_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        new_internal->Init(internal_max_size_);

        int parent_mid = parent_page->GetSize() / 2;
        int parent_size = parent_page->GetSize();

        // Move second half to new internal
        for (int i = parent_mid + 1; i < parent_size; i++) {
          new_internal->SetKeyAt(i - parent_mid, parent_page->KeyAt(i));
          new_internal->SetValueAt(i - parent_mid, parent_page->ValueAt(i));
        }
        new_internal->SetValueAt(parent_size - parent_mid - 1, parent_page->ValueAt(parent_size - 1));
        new_internal->SetSize(parent_mid);
        parent_page->SetSize(parent_mid);

        // Push up middle key
        KeyType push_up_key = parent_page->KeyAt(parent_mid);

        // Insert into appropriate parent
        if (comparator_(parent_key, push_up_key) <= 0) {
          int pos = parent_page->GetSize();
          while (pos > 1 && comparator_(parent_page->KeyAt(pos - 1), parent_key) > 0) {
            parent_page->SetKeyAt(pos, parent_page->KeyAt(pos - 1));
            parent_page->SetValueAt(pos, parent_page->ValueAt(pos - 1));
            pos--;
          }
          parent_page->SetKeyAt(pos, parent_key);
          parent_page->SetValueAt(pos, new_child_id);
          parent_page->ChangeSizeBy(1);
          new_internal->SetKeyAt(0, push_up_key);
          new_internal->ChangeSizeBy(1);
        } else {
          new_internal->SetKeyAt(0, push_up_key);
          new_internal->SetValueAt(0, parent_page->ValueAt(parent_mid));
          new_internal->ChangeSizeBy(1);
          int pos = new_internal->GetSize();
          while (pos > 1 && comparator_(new_internal->KeyAt(pos - 1), parent_key) > 0) {
            new_internal->SetKeyAt(pos, new_internal->KeyAt(pos - 1));
            new_internal->SetValueAt(pos, new_internal->ValueAt(pos - 1));
            pos--;
          }
          new_internal->SetKeyAt(pos, parent_key);
          new_internal->SetValueAt(pos, new_child_id);
          new_internal->ChangeSizeBy(1);
        }

        new_internal_guard.Drop();

        // Continue up
        parent_key = new_internal->KeyAt(0);
        new_child_id = new_internal_id;
      } else {
        // Parent not full
        int pos = parent_page->GetSize();
        while (pos > 1 && comparator_(parent_page->KeyAt(pos - 1), parent_key) > 0) {
          parent_page->SetKeyAt(pos, parent_page->KeyAt(pos - 1));
          parent_page->SetValueAt(pos, parent_page->ValueAt(pos - 1));
          pos--;
        }
        parent_page->SetKeyAt(pos, parent_key);
        parent_page->SetValueAt(pos, new_child_id);
        parent_page->ChangeSizeBy(1);
        break;
      }
    }

    // Create new root if needed
    if (ctx.write_set_.empty() && ctx.header_page_.has_value()) {
      page_id_t new_root_id = bpm_->NewPage();
      auto new_root_guard = bpm_->WritePage(new_root_id);
      auto new_root = new_root_guard.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      new_root->Init(internal_max_size_);

      auto header = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
      page_id_t old_root_id = header->root_page_id_;

      new_root->SetKeyAt(0, KeyType{});
      new_root->SetKeyAt(1, parent_key);
      new_root->SetValueAt(0, old_root_id);
      new_root->SetValueAt(1, new_child_id);
      new_root->SetSize(1);

      header->root_page_id_ = new_root_id;
      new_root_guard.Drop();
    }
  } else {
    // Insert into leaf (left is the position from earlier)
    for (int i = leaf_page->GetSize(); i > left; i--) {
      leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
      leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
    }
    leaf_page->SetKeyAt(left, key);
    leaf_page->SetValueAt(left, value);
    leaf_page->ChangeSizeBy(1);
  }

  // Release locks
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_back();
  }
  if (ctx.header_page_.has_value()) {
    ctx.header_page_.value().Drop();
    ctx.header_page_ = std::nullopt;
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // For write operations, we need to hold write latches on all pages in the access path
  // This is necessary because pages might merge or redistribute during deletion

  // Declaration of context instance.
  Context ctx;

  // Acquire write lock on header page
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // Handle empty tree
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    ctx.header_page_.value().Drop();
    ctx.header_page_ = std::nullopt;
    return;
  }

  // Traverse down to leaf page, holding write latches on all pages
  page_id_t current_page_id = ctx.root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(current_page_id));
  auto current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();

  while (!current_page->IsLeafPage()) {
    auto internal_page = ctx.write_set_.back().template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

    // Find the child to follow
    int child_index = 0;
    for (int i = 1; i < internal_page->GetSize(); i++) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        child_index = i - 1;
        break;
      }
      child_index = i;
    }

    auto child_page_id = internal_page->ValueAt(child_index);
    ctx.write_set_.push_back(bpm_->WritePage(child_page_id));
    current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  }

  // Now at leaf page
  auto leaf_page = ctx.write_set_.back().template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  // Find the key
  int left = 0;
  int right = leaf_page->GetSize() - 1;
  int found_index = -1;

  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp = comparator_(leaf_page->KeyAt(mid), key);
    if (cmp == 0) {
      found_index = mid;
      break;
    } else if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  // If key not found, just release locks and return
  if (found_index == -1) {
    while (!ctx.write_set_.empty()) {
      ctx.write_set_.pop_back();
    }
    if (ctx.header_page_.has_value()) {
      ctx.header_page_.value().Drop();
      ctx.header_page_ = std::nullopt;
    }
    return;
  }

  // If key found, delete it - shift elements
  for (int i = found_index; i < leaf_page->GetSize() - 1; i++) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
  }
  leaf_page->ChangeSizeBy(-1);

  // Release all locks
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_back();
  }
  if (ctx.header_page_.has_value()) {
    ctx.header_page_.value().Drop();
    ctx.header_page_ = std::nullopt;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.template As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;
  header_guard.Drop();

  if (root_page_id == INVALID_PAGE_ID) {
    return End();
  }

  // Optimistic latch coupling
  while (true) {
    // Find the leftmost leaf page
    page_id_t current_page_id = root_page_id;
    auto page_guard = bpm_->ReadPage(current_page_id);
    auto page = page_guard.template As<BPlusTreePage>();

    while (!page->IsLeafPage()) {
      auto internal_page = page_guard.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      // Go to the leftmost child (index 0)
      current_page_id = internal_page->ValueAt(0);

      // Try to get child page with CheckedReadPage
      auto child_guard_opt = bpm_->CheckedReadPage(current_page_id);

      if (!child_guard_opt.has_value()) {
        // Child is not safe, retry from root
        break;
      }

      // Child is safe, release parent latch
      page_guard.Drop();
      page_guard = std::move(child_guard_opt.value());
      page = page_guard.template As<BPlusTreePage>();
    }

    // Check if we reached the leaf page successfully
    if (page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(bpm_, std::move(page_guard), 0, root_page_id);
    }
    // If we broke out of the loop before reaching a leaf, we'll retry
  }
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.template As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;
  header_guard.Drop();

  if (root_page_id == INVALID_PAGE_ID) {
    return End();
  }

  // Optimistic latch coupling
  while (true) {
    // Traverse down to the leaf page containing the key
    page_id_t current_page_id = root_page_id;
    auto page_guard = bpm_->ReadPage(current_page_id);
    auto page = page_guard.template As<BPlusTreePage>();

    while (!page->IsLeafPage()) {
      auto internal_page = page_guard.template As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

      // Find the child to follow
      int child_index = 0;
      for (int i = 1; i < internal_page->GetSize(); i++) {
        if (comparator_(key, internal_page->KeyAt(i)) < 0) {
          child_index = i - 1;
          break;
        }
        child_index = i;
      }

      current_page_id = internal_page->ValueAt(child_index);

      // Try to get child page with CheckedReadPage
      auto child_guard_opt = bpm_->CheckedReadPage(current_page_id);

      if (!child_guard_opt.has_value()) {
        // Child is not safe, retry from root
        break;
      }

      // Child is safe, release parent latch
      page_guard.Drop();
      page_guard = std::move(child_guard_opt.value());
      page = page_guard.template As<BPlusTreePage>();
    }

    // Check if we reached the leaf page successfully
    if (page->IsLeafPage()) {
      // At leaf page, find the key
      auto leaf_page = page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

      // Binary search for key or the position where it should be
      int left = 0;
      int right = leaf_page->GetSize() - 1;
      int index = leaf_page->GetSize();  // Default to end

      while (left <= right) {
        int mid = left + (right - left) / 2;
        int cmp = comparator_(leaf_page->KeyAt(mid), key);
        if (cmp == 0) {
          index = mid;
          break;
        } else if (cmp < 0) {
          left = mid + 1;
        } else {
          right = mid - 1;
        }
      }

      // If we didn't find an exact match, use left (which is the position where key should be inserted)
      if (index == leaf_page->GetSize()) {
        index = left;
      }

      return INDEXITERATOR_TYPE(bpm_, std::move(page_guard), index, root_page_id);
    }
    // If we broke out of the loop before reaching a leaf, we'll retry
  }
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE();
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.template As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
