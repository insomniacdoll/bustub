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
  // Read header page to get root id
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_id = header->root_page_id_;
  header_guard.Drop();
  
  // Check if tree is empty
  if (root_id == INVALID_PAGE_ID) {
    return false;
  }
  
  // Read-mode traversal from root to leaf
  page_id_t current_page_id = root_id;
  bool found_leaf = false;
  ReadPageGuard leaf_guard;
  
  while (current_page_id != INVALID_PAGE_ID) {
    auto page_guard = bpm_->ReadPage(current_page_id);
    auto page = page_guard.template As<BPlusTreePage>();
    
    if (page->IsLeafPage()) {
      // Found leaf - save guard and stop traversal
      leaf_guard = std::move(page_guard);
      found_leaf = true;
      break;
    }
    
    // Internal page - find child to traverse
    auto internal = page_guard.template As<InternalPage>();
    int child_index = FindChildIndexRead(internal, key);
    current_page_id = internal->ValueAt(child_index);
    
    // Drop current page guard before moving to child
    page_guard.Drop();
  }
  
  // Should have found leaf in a valid tree
  BUSTUB_ASSERT(found_leaf, "GetValue: leaf not found in valid tree");
  
  auto leaf = leaf_guard.template As<LeafPage>();
  
  // Binary search for key in leaf
  int index = LowerBoundRead(leaf, key);
  
  // Key not found
  if (index >= leaf->GetSize() || comparator_(leaf->KeyAt(index), key) != 0) {
    return false;
  }
  
  // Key found but tombstoned - logically deleted
  if (leaf->IsTombstone(index)) {
    return false;
  }
  
  // Key found and valid - add to result (do NOT clear result vector)
  result->push_back(leaf->ValueAt(index));
  return true;
}

/**
 * @brief Find child index in internal page for given key (read mode).
 * 
 * Returns index of child pointer that may contain the key.
 * Uses binary search on keys [1, size-1].
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::FindChildIndexRead(const InternalPage *internal, const KeyType &key) {
  // Binary search on keys [1, size-1]
  int left = 1;
  int right = internal->GetSize() - 1;
  
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator_(internal->KeyAt(mid), key) <= 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  
  // Child index is right (or 0 if key < all separators)
  return right;
}

/**
 * @brief Find lower bound index in leaf page for given key (read mode).
 * 
 * Returns first index where key >= search key.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::LowerBoundRead(const LeafPage *leaf, const KeyType &key) {
  int left = 0;
  int right = leaf->GetSize();
  
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator_(leaf->KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  
  return left;
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
  // Initialize context with header write guard
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  
  // Handle empty tree - create new root leaf
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    CreateNewRootLeaf(&ctx, key, value);
    return true;
  }
  
  // Write-mode traversal from root to leaf
  FindLeafPageWrite(key, ctx);
  
  // Get leaf from write_set_
  auto &leaf_guard = ctx.write_set_.back();
  auto leaf = leaf_guard.template AsMut<LeafPage>();
  
  // Binary search for insertion position
  int index = LowerBoundWrite(leaf, key);
  
  // Check if key already exists
  if (index < leaf->GetSize() && comparator_(leaf->KeyAt(index), key) == 0) {
    // Key exists - check if tombstoned
    if (leaf->IsTombstone(index)) {
      // Key is tombstoned - resurrect it (remove tombstone, update value)
      leaf->RemoveTombstone(index);
      leaf->SetValueAt(index, value);
      return true;
    } else {
      // Key exists and not tombstoned - duplicate key, insertion failed
      return false;
    }
  }
  
  // Key does not exist - proceed with insertion
  
  // Check if leaf has room
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    // Leaf has room - insert directly
    bool first_changed = ShiftEntriesRight(leaf, index);
    leaf->SetKeyAt(index, key);
    leaf->SetValueAt(index, value);
    
    // Update parent separator if first key changed
    if (first_changed && leaf->GetSize() > 0 && ctx.write_set_.size() >= 2) {
      UpdateParentSeparator(ctx, leaf->KeyAt(0));
    }
    
    return true;
  }
  
  // Leaf is full - need to split
  auto [new_leaf_id, initial_sep] = SplitLeaf(leaf);
  
  // Determine which leaf to insert into
  KeyType final_sep;
  if (comparator_(key, initial_sep) < 0) {
    // Insert into left (original) leaf
    // Recalculate position in left leaf after split
    int pos = LowerBoundWrite(leaf, key);
    bool first_changed = ShiftEntriesRight(leaf, pos);
    leaf->SetKeyAt(pos, key);
    leaf->SetValueAt(pos, value);
    
    final_sep = initial_sep;
    
    // Update parent separator if first key changed
    if (first_changed && leaf->GetSize() > 0 && ctx.write_set_.size() >= 2) {
      UpdateParentSeparator(ctx, leaf->KeyAt(0));
    }
  } else {
    // Insert into right (new) leaf
    // Acquire write guard for new leaf (SplitLeaf dropped it)
    auto new_leaf_guard = bpm_->WritePage(new_leaf_id);
    auto new_leaf = new_leaf_guard.template AsMut<LeafPage>();
    
    // Recalculate position in new leaf after split
    int pos = LowerBoundWrite(new_leaf, key);
    // ShiftEntriesRight may return true if first key changed, but we don't need to update
    // parent separator because final_sep is already set to new_leaf->KeyAt(0) below
    ShiftEntriesRight(new_leaf, pos);
    new_leaf->SetKeyAt(pos, key);
    new_leaf->SetValueAt(pos, value);
    
    final_sep = new_leaf->KeyAt(0);
    
    new_leaf_guard.Drop();
  }
  
  // Pop leaf from write_set_ before InsertIntoParent
  page_id_t leaf_id = ctx.write_set_.back().GetPageId();
  ctx.write_set_.pop_back();
  
  // Insert separator into parent (may cause recursive splits)
  InsertIntoParent(&ctx, leaf_id, final_sep, new_leaf_id);
  
  return true;
}

/**
 * @brief Create a new root leaf page for an empty tree.
 * 
 * Pushes the root guard to write_set_ for RAII cleanup.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateNewRootLeaf(Context *ctx, const KeyType &key, const ValueType &value) {
  // Allocate new leaf page
  page_id_t root_id = bpm_->NewPage();
  BUSTUB_ASSERT(root_id != INVALID_PAGE_ID, "failed to allocate root leaf page");
  
  // Acquire write guard and initialize
  auto root_guard = bpm_->WritePage(root_id);
  auto root = root_guard.template AsMut<LeafPage>();
  root->Init(leaf_max_size_);
  
  // Insert the key-value pair
  root->SetKeyAt(0, key);
  root->SetValueAt(0, value);
  root->SetSize(1);
  root->SetNextPageId(INVALID_PAGE_ID);
  
  // Update header
  auto header = ctx->header_page_->template AsMut<BPlusTreeHeaderPage>();
  header->root_page_id_ = root_id;
  ctx->root_page_id_ = root_id;
  
  // Push root guard into write_set_ for RAII cleanup
  ctx->write_set_.push_back(std::move(root_guard));
  
  // Guard will be dropped when Context destructor runs
}

/**
 * @brief Insert a separator into parent page after child split.
 * 
 * May cause recursive splits up to root.
 * If write_set_ is empty, creates new root internal page.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(Context *ctx, page_id_t old_child_id, const KeyType &sep_key, page_id_t new_child_id) {
  // If write_set_ is empty, we split the root - need to create new root
  if (ctx->write_set_.empty()) {
    CreateNewRootInternal(ctx, old_child_id, sep_key, new_child_id);
    return;
  }
  
  // Get parent from write_set_
  auto &parent_guard = ctx->write_set_.back();
  auto parent = parent_guard.template AsMut<InternalPage>();
  
  // Find position for new child
  int old_child_index = parent->ValueIndex(old_child_id);
  
  // Assert that old child was found in parent
  BUSTUB_ASSERT(old_child_index >= 0, "old child not found in parent");
  
  int insert_index = old_child_index + 1;
  
  // Assert that insert_index is valid (must be >= 1 for internal key)
  BUSTUB_ASSERT(insert_index >= 1 && insert_index <= parent->GetSize(),
                "invalid internal insert index");
  
  // Check if parent has room
  if (parent->GetSize() < parent->GetMaxSize()) {
    // Parent has room - insert directly
    InsertIntoInternalPage(parent, insert_index, sep_key, new_child_id);
    return;
  }
  
  // Parent is full - need to split
  auto [new_internal_id, new_sep] = SplitInternal(parent, insert_index, sep_key, new_child_id);
  
  // Pop parent from write_set_
  page_id_t parent_id = ctx->write_set_.back().GetPageId();
  ctx->write_set_.pop_back();
  
  // Recursively insert into grandparent
  InsertIntoParent(ctx, parent_id, new_sep, new_internal_id);
}

/**
 * @brief Create a new root internal page after root split.
 * 
 * Pushes the root guard to write_set_ for RAII cleanup.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateNewRootInternal(Context *ctx, page_id_t old_child_id, const KeyType &sep_key, page_id_t new_child_id) {
  // Allocate new internal page for root
  page_id_t new_root_id = bpm_->NewPage();
  BUSTUB_ASSERT(new_root_id != INVALID_PAGE_ID, "failed to allocate new root internal page");
  
  // Acquire write guard and initialize
  auto new_root_guard = bpm_->WritePage(new_root_id);
  auto new_root = new_root_guard.template AsMut<InternalPage>();
  new_root->Init(internal_max_size_);
  
  // Set up new root with two children
  // value[0] = old_child_id (left child)
  // key[1] = sep_key (separator)
  // value[1] = new_child_id (right child)
  // size = 2 (two child pointers)
  new_root->SetValueAt(0, old_child_id);
  new_root->SetKeyAt(1, sep_key);
  new_root->SetValueAt(1, new_child_id);
  new_root->SetSize(2);
  
  // Update header
  auto header = ctx->header_page_->template AsMut<BPlusTreeHeaderPage>();
  header->root_page_id_ = new_root_id;
  ctx->root_page_id_ = new_root_id;
  
  // Push new root guard into write_set_ for RAII cleanup
  ctx->write_set_.push_back(std::move(new_root_guard));
  
  // Guard will be dropped when Context destructor runs
}

/**
 * @brief Insert a key and child into internal page at given index.
 * 
 * PRECONDITION: insert_index >= 1 (cannot insert key at index 0)
 * PRECONDITION: internal page is not full
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalPage(InternalPage *internal, int insert_index, const KeyType &key, page_id_t child_id) {
  int old_size = internal->GetSize();
  
  // PRECONDITION: internal page is not full
  BUSTUB_ASSERT(old_size < internal->GetMaxSize(), "internal page is full");
  
  // PRECONDITION: insert_index >= 1 (cannot insert key at index 0)
  BUSTUB_ASSERT(insert_index >= 1 && insert_index <= old_size,
                "invalid internal insert index");
  
  // Shift values right to make room
  // Target slot is old_size (after increment)
  // INCLUSIVE: i = old_size, ..., insert_index+1
  for (int i = old_size; i >= insert_index + 1; i--) {
    internal->SetValueAt(i, internal->ValueAt(i - 1));
  }
  
  // Shift keys right to make room
  // Target slot is old_size (after increment)
  // INCLUSIVE: i = old_size, ..., insert_index+1
  // Note: key[0] is never touched
  for (int i = old_size; i >= insert_index + 1; i--) {
    internal->SetKeyAt(i, internal->KeyAt(i - 1));
  }
  
  // Insert new key and child at insert_index
  internal->SetKeyAt(insert_index, key);
  internal->SetValueAt(insert_index, child_id);
  
  // Increment size
  internal->ChangeSizeBy(1);
}

/**
 * @brief Split a full internal page after inserting a new entry.
 * 
 * Uses materialized arrays to avoid complex case analysis.
 * 
 * Guarantees:
 *   - key[0] is NEVER read or written
 *   - promoted key is correctly removed from both child pages
 *   - right page value[0] is correct
 *   - recursive parent insertion receives correct separator
 *   - split midpoint is valid (>= 1 and < total_size)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal,
                                   int insert_index,
                                   const KeyType &insert_key,
                                   page_id_t insert_child_id)
    -> std::pair<page_id_t, KeyType> {
  int old_size = internal->GetSize();
  int total_size = old_size + 1;
  
  // PRECONDITION: insert_index is valid
  BUSTUB_ASSERT(insert_index >= 1 && insert_index <= old_size,
                "invalid internal insert index");
  
  // PRECONDITION: internal page is full
  BUSTUB_ASSERT(old_size == internal->GetMaxSize(),
                "SplitInternal should only be called on full internal page");
  
  // Materialize all keys and values into temporary arrays
  // This avoids complex case analysis and ensures correctness
  std::vector<KeyType> keys(total_size);
  std::vector<page_id_t> values(total_size);
  
  // Copy old values and insert new value at insert_index
  // INCLUSIVE: i = 0, ..., insert_index-1 (before insertion point)
  for (int i = 0; i < insert_index; i++) {
    values[i] = internal->ValueAt(i);
  }
  
  // Insert new value at insert_index
  values[insert_index] = insert_child_id;
  
  // Copy remaining old values after insertion point
  // INCLUSIVE: i = insert_index+1, ..., total_size-1
  for (int i = insert_index + 1; i < total_size; i++) {
    values[i] = internal->ValueAt(i - 1);
  }
  
  // Copy old keys and insert new key at insert_index
  // IMPORTANT: key[0] is invalid and should NOT be read from internal
  // We leave keys[0] uninitialized (it will never be used)
  
  // Copy keys before insertion point
  // INCLUSIVE: i = 1, ..., insert_index-1
  for (int i = 1; i < insert_index; i++) {
    keys[i] = internal->KeyAt(i);
  }
  
  // Insert new key at insert_index
  keys[insert_index] = insert_key;
  
  // Copy keys after insertion point
  // INCLUSIVE: i = insert_index+1, ..., total_size-1
  for (int i = insert_index + 1; i < total_size; i++) {
    keys[i] = internal->KeyAt(i - 1);
  }
  
  // Calculate split point
  // Split at mid = total_size / 2
  // keys[mid] will be promoted to parent
  int mid = total_size / 2;
  
  // Assert that split midpoint is valid
  BUSTUB_ASSERT(mid >= 1 && mid < total_size, "invalid internal split midpoint");
  
  KeyType promote_key = keys[mid];
  
  // Allocate new internal page
  page_id_t new_internal_id = bpm_->NewPage();
  BUSTUB_ASSERT(new_internal_id != INVALID_PAGE_ID,
                "failed to allocate new internal page");
  
  auto new_internal_guard = bpm_->WritePage(new_internal_id);
  auto new_internal = new_internal_guard.template AsMut<InternalPage>();
  new_internal->Init(internal->GetMaxSize());
  
  // Left page (original internal) gets:
  //   values[0..mid-1] (mid child pointers)
  //   keys[1..mid-1] (mid-1 separators)
  //   size = mid
  internal->SetSize(mid);
  
  // Copy values to left page
  // INCLUSIVE: i = 0, ..., mid-1
  for (int i = 0; i < mid; i++) {
    internal->SetValueAt(i, values[i]);
  }
  
  // Copy keys to left page
  // INCLUSIVE: i = 1, ..., mid-1
  // IMPORTANT: key[0] is NOT set (remains invalid)
  for (int i = 1; i < mid; i++) {
    internal->SetKeyAt(i, keys[i]);
  }
  
  // Right page (new internal) gets:
  //   values[mid..total_size-1] (total_size - mid child pointers)
  //   keys[mid+1..total_size-1] rebased to key[1..right_size-1]
  //   size = total_size - mid
  // Note: keys[mid] is promoted and NOT stored in either child page
  int right_size = total_size - mid;
  new_internal->SetSize(right_size);
  
  // Copy values to right page
  // INCLUSIVE: i = 0, ..., right_size-1
  // values[mid] becomes value[0] of right page
  for (int i = 0; i < right_size; i++) {
    new_internal->SetValueAt(i, values[mid + i]);
  }
  
  // Copy keys to right page
  // keys[mid+1..total_size-1] become key[1..right_size-1]
  // INCLUSIVE: i = 1, ..., right_size-1
  // IMPORTANT: key[0] is NOT set (remains invalid)
  for (int i = 1; i < right_size; i++) {
    new_internal->SetKeyAt(i, keys[mid + i]);
  }
  
  // CRITICAL: Never set or read new_internal->KeyAt(0)
  // key[0] remains invalid in both left and right pages
  
  // Drop new internal guard
  new_internal_guard.Drop();
  
  return {new_internal_id, promote_key};
}

/**
 * @brief Shift entries right to make room for insertion at pos.
 * 
 * Returns true if first key changed due to tombstone overflow.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftEntriesRight(LeafPage *leaf, int pos) -> bool {
  auto tomb_indexes = leaf->GetTombstoneIndexes();
  
  // Increment size
  leaf->ChangeSizeBy(1);
  
  // Shift entries right
  // INCLUSIVE: i = leaf->GetSize()-1, ..., pos+1
  for (int i = leaf->GetSize() - 1; i >= pos + 1; i--) {
    leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
    leaf->SetValueAt(i, leaf->ValueAt(i - 1));
  }
  
  // Adjust tombstone indexes (increment if >= pos)
  std::vector<size_t> adjusted;
  for (size_t idx : tomb_indexes) {
    adjusted.push_back(idx >= static_cast<size_t>(pos) ? idx + 1 : idx);
  }
  
  return leaf->RebuildTombstones(adjusted);
}

/**
 * @brief Split a full leaf page.
 * 
 * Returns (new_leaf_id, first_key) where first_key is the first key of new leaf.
 * CRITICAL: Saves first_key BEFORE dropping new leaf guard.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf) -> std::pair<page_id_t, KeyType> {
  // Allocate new leaf page
  page_id_t new_leaf_id = bpm_->NewPage();
  BUSTUB_ASSERT(new_leaf_id != INVALID_PAGE_ID, "failed to allocate new leaf page");
  
  // Acquire write guard for new page
  auto new_leaf_guard = bpm_->WritePage(new_leaf_id);
  auto new_leaf = new_leaf_guard.template AsMut<LeafPage>();
  new_leaf->Init(leaf->GetMaxSize());
  
  int mid = leaf->GetSize() / 2;
  int orig_size = leaf->GetSize();
  
  auto tomb_indexes = leaf->GetTombstoneIndexes();
  
  // Move entries from mid to orig_size-1 to new leaf
  // INCLUSIVE: i = mid, ..., orig_size-1
  for (int i = mid; i <= orig_size - 1; i++) {
    new_leaf->SetKeyAt(i - mid, leaf->KeyAt(i));
    new_leaf->SetValueAt(i - mid, leaf->ValueAt(i));
  }
  
  new_leaf->SetSize(orig_size - mid);
  leaf->SetSize(mid);
  
  // Partition tombstones
  std::vector<size_t> left_tombs;
  std::vector<size_t> right_tombs;
  for (size_t idx : tomb_indexes) {
    if (idx < static_cast<size_t>(mid)) {
      left_tombs.push_back(idx);
    } else {
      right_tombs.push_back(idx - static_cast<size_t>(mid));
    }
  }
  
  // Rebuild tombstones (no overflow expected)
  leaf->RebuildTombstones(left_tombs);
  new_leaf->RebuildTombstones(right_tombs);
  
  // Update linked list
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_id);
  
  // CRITICAL: Save first_key BEFORE dropping guard
  KeyType first_key = new_leaf->KeyAt(0);
  
  // Drop new leaf guard - caller will re-acquire if needed
  new_leaf_guard.Drop();
  
  return {new_leaf_id, first_key};
}

/**
 * @brief Find leaf page containing key using write-mode traversal.
 * 
 * Fills ctx.write_set_ with root-to-leaf path.
 * PRECONDITION: ctx.header_page_ and ctx.root_page_id_ are initialized.
 * 
 * IMPORTANT: Guard is pushed to write_set_ BEFORE accessing page data,
 * to avoid hidden coupling with moved guard.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindLeafPageWrite(const KeyType &key, Context &ctx) {
  BUSTUB_ASSERT(ctx.header_page_.has_value(), "header_page_ not initialized");
  BUSTUB_ASSERT(ctx.root_page_id_ != INVALID_PAGE_ID, "root_page_id_ is invalid");
  
  page_id_t current_page_id = ctx.root_page_id_;
  
  // Traverse from root to leaf, acquiring write guards
  while (current_page_id != INVALID_PAGE_ID) {
    auto page_guard = bpm_->WritePage(current_page_id);
    
    // Push guard into write_set_ BEFORE accessing page data
    ctx.write_set_.push_back(std::move(page_guard));
    
    // Access page from write_set_.back() (not from moved guard)
    auto page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
    
    if (page->IsLeafPage()) {
      // Found leaf - stop traversal
      return;
    }
    
    // Internal page - find child to traverse
    auto internal = ctx.write_set_.back().template AsMut<InternalPage>();
    int child_index = FindChildIndexWrite(internal, key);
    current_page_id = internal->ValueAt(child_index);
  }
  
  BUSTUB_ASSERT(false, "FindLeafPageWrite: traversal ended without finding leaf");
}

/**
 * @brief Find child index in internal page for given key (write mode).
 * 
 * Uses binary search on keys [1, size-1].
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::FindChildIndexWrite(InternalPage *internal, const KeyType &key) {
  // Binary search on keys [1, size-1]
  int left = 1;
  int right = internal->GetSize() - 1;
  
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator_(internal->KeyAt(mid), key) <= 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  
  return right;
}

/**
 * @brief Find lower bound index in leaf page for given key (write mode).
 * 
 * Returns first index where key >= search key.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::LowerBoundWrite(LeafPage *leaf, const KeyType &key) {
  int left = 0;
  int right = leaf->GetSize();
  
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator_(leaf->KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  
  return left;
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
  // Initialize context with header write guard
  // This guard must cover entire mutation operation
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  
  // Check if tree is empty
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    // RAII cleanup: ctx destructor will drop header_page_ guard
    return;
  }
  
  // Write-mode traversal from root to leaf
  FindLeafPageWrite(key, ctx);
  
  auto &leaf_guard = ctx.write_set_.back();
  auto leaf = leaf_guard.template AsMut<LeafPage>();
  
  // Binary search for key
  int index = LowerBoundWrite(leaf, key);
  
  // Key not found - return silently
  // RAII cleanup: ctx destructor will drop all guards in write_set_ and header_page_
  if (index >= leaf->GetSize() || comparator_(leaf->KeyAt(index), key) != 0) {
    return;
  }
  
  // Key already tombstoned - return silently (idempotent)
  // RAII cleanup: ctx destructor will drop all guards
  if (leaf->IsTombstone(index)) {
    return;
  }
  
  // Record state before tombstone
  int old_size = leaf->GetSize();
  KeyType old_first_key = (old_size > 0) ? leaf->KeyAt(0) : KeyType();
  
  // Add tombstone (may trigger physical deletion)
  leaf->AddTombstone(index);
  
  // Handle physical deletion consequences
  int new_size = leaf->GetSize();
  if (new_size < old_size) {
    // Physical deletion occurred
    
    // CRITICAL: Separator update MUST happen BEFORE rebalance
    if (new_size > 0) {
      KeyType new_first_key = leaf->KeyAt(0);
      if (comparator_(new_first_key, old_first_key) != 0) {
        // First key changed - update separator NOW
        if (ctx.write_set_.size() >= 2) {
          UpdateParentSeparator(ctx, new_first_key);
        }
      }
    }
    
    // Check if leaf is underfull - rebalance AFTER separator update
    if (new_size < leaf->GetMinSize()) {
      if (ctx.write_set_.size() == 1) {
        AdjustRoot(&ctx);
      } else {
        CoalesceOrRedistribute(&ctx);
      }
      // AFTER rebalance: write_set_.back() is parent (or empty)
      // DO NOT call any helper that assumes leaf is at back
    }
  }
  
  // RAII cleanup: ctx destructor will drop all remaining guards
  // - header_page_ guard
  // - any remaining guards in write_set_
}

/**
 * @brief Update parent separator for leaf's first key change.
 * 
 * PRECONDITION: write_set_.size() >= 2
 * PRECONDITION: leaf at back is not root
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentSeparator(Context &ctx, const KeyType &new_first_key) {
  // PRECONDITION: write_set_.size() >= 2
  BUSTUB_ASSERT(ctx.write_set_.size() >= 2, "write_set_ too small for separator update");
  
  auto &parent_guard = ctx.write_set_[ctx.write_set_.size() - 2];
  auto parent = parent_guard.template AsMut<InternalPage>();
  
  auto &leaf_guard = ctx.write_set_.back();
  page_id_t leaf_id = leaf_guard.GetPageId();
  int leaf_index = parent->ValueIndex(leaf_id);
  
  // Only update if leaf is not child 0
  if (leaf_index > 0) {
    parent->SetKeyAt(leaf_index, new_first_key);
  }
}

/**
 * @brief Calculate expected leaf size after redistribution.
 * 
 * Accounts for potential tombstone overflow.
 * Uses conditional to avoid unsigned underflow.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CalculateExpectedSizeAfterRedistribution(LeafPage *leaf, bool borrow_is_tomb) -> int {
  auto recipient_tombs = leaf->GetTombstoneIndexes();
  size_t tomb_capacity = leaf->GetTombstoneCapacity();
  
  // PRECONDITION: page must be in valid state
  BUSTUB_ASSERT(recipient_tombs.size() <= tomb_capacity,
                "recipient tombstone count exceeds capacity");
  
  size_t combined_tomb_count = recipient_tombs.size() + (borrow_is_tomb ? 1 : 0);
  
  // FIXED: Use conditional to avoid unsigned underflow
  size_t overflow_deletions = 0;
  if (combined_tomb_count > tomb_capacity) {
    overflow_deletions = combined_tomb_count - tomb_capacity;
  }
  
  // POSTCONDITION: redistribution adds at most one tombstone
  BUSTUB_ASSERT(overflow_deletions <= 1,
                "redistribution should overflow at most one tombstone");
  
  int expected_size = leaf->GetSize() + 1 - static_cast<int>(overflow_deletions);
  return expected_size;
}

/**
 * @brief Rebalance an underfull page.
 * 
 * Tries redistribution first, then merge.
 * Leaves parent at write_set_.back() after coalesce.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceOrRedistribute(Context *ctx) {
  auto &page_guard = ctx->write_set_.back();
  auto page = page_guard.template AsMut<BPlusTreePage>();
  bool is_leaf = page->IsLeafPage();
  page_id_t page_id = page_guard.GetPageId();
  
  auto &parent_guard = ctx->write_set_[ctx->write_set_.size() - 2];
  auto parent = parent_guard.template AsMut<InternalPage>();
  int page_index = parent->ValueIndex(page_id);
  
  if (is_leaf) {
    auto leaf = page_guard.template AsMut<LeafPage>();
    
    // Try redistribution from left (with precheck)
    if (page_index > 0) {
      page_id_t left_id = parent->ValueAt(page_index - 1);
      auto left_guard = bpm_->WritePage(left_id);
      auto left = left_guard.template AsMut<LeafPage>();
      
      if (left->GetSize() > left->GetMinSize()) {
        int last_idx = left->GetSize() - 1;
        bool borrow_is_tomb = left->IsTombstone(last_idx);
        
        int expected_size = CalculateExpectedSizeAfterRedistribution(leaf, borrow_is_tomb);
        
        if (expected_size >= leaf->GetMinSize()) {
          RedistributeLeafFromLeft(leaf, parent, page_index, std::move(left_guard), left);
          return;
        }
      }
      
      if (left->GetSize() + leaf->GetSize() <= left->GetMaxSize()) {
        CoalesceLeafWithLeft(ctx, parent, page_index, std::move(left_guard), left);
        return;
      }
      
      left_guard.Drop();
    }
    
    // Try redistribution from right or merge with right
    if (page_index + 1 < parent->GetSize()) {
      page_id_t right_id = parent->ValueAt(page_index + 1);
      auto right_guard = bpm_->WritePage(right_id);
      auto right = right_guard.template AsMut<LeafPage>();
      
      if (right->GetSize() > right->GetMinSize()) {
        bool borrow_is_tomb = right->IsTombstone(0);
        
        int expected_size = CalculateExpectedSizeAfterRedistribution(leaf, borrow_is_tomb);
        
        if (expected_size >= leaf->GetMinSize()) {
          RedistributeLeafFromRight(leaf, parent, page_index, std::move(right_guard), right);
          return;
        }
      }
      
      if (leaf->GetSize() + right->GetSize() <= leaf->GetMaxSize()) {
        CoalesceLeafWithRight(ctx, parent, page_index, std::move(right_guard), right);
        return;
      }
      
      right_guard.Drop();
    }
    
    BUSTUB_ASSERT(false, "Cannot redistribute or merge leaf");
  } else {
    auto internal = page_guard.template AsMut<InternalPage>();
    
    if (page_index > 0) {
      page_id_t left_id = parent->ValueAt(page_index - 1);
      auto left_guard = bpm_->WritePage(left_id);
      auto left = left_guard.template AsMut<InternalPage>();
      
      if (left->GetSize() > left->GetMinSize()) {
        RedistributeInternalFromLeft(internal, parent, page_index, std::move(left_guard), left);
        return;
      }
      
      if (left->GetSize() + internal->GetSize() <= left->GetMaxSize()) {
        CoalesceInternalWithLeft(ctx, parent, page_index, std::move(left_guard), left);
        return;
      }
      
      left_guard.Drop();
    }
    
    if (page_index + 1 < parent->GetSize()) {
      page_id_t right_id = parent->ValueAt(page_index + 1);
      auto right_guard = bpm_->WritePage(right_id);
      auto right = right_guard.template AsMut<InternalPage>();
      
      if (right->GetSize() > right->GetMinSize()) {
        RedistributeInternalFromRight(internal, parent, page_index, std::move(right_guard), right);
        return;
      }
      
      if (internal->GetSize() + right->GetSize() <= internal->GetMaxSize()) {
        CoalesceInternalWithRight(ctx, parent, page_index, std::move(right_guard), right);
        return;
      }
      
      right_guard.Drop();
    }
    
    BUSTUB_ASSERT(false, "Cannot redistribute or merge internal");
  }
}

/**
 * @brief Adjust root page after deletion.
 * 
 * PRECONDITION: ctx->header_page_ is initialized.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(Context *ctx) {
  BUSTUB_ASSERT(ctx->header_page_.has_value(), "header_page_ not initialized");
  
  auto header = ctx->header_page_->template AsMut<BPlusTreeHeaderPage>();
  page_id_t root_id = header->root_page_id_;
  
  auto &root_guard = ctx->write_set_.back();
  auto root = root_guard.template AsMut<BPlusTreePage>();
  
  if (root->IsLeafPage()) {
    auto root_leaf = root_guard.template AsMut<LeafPage>();
    
    if (root_leaf->GetSize() == 0) {
      // Empty root - delete it
      header->root_page_id_ = INVALID_PAGE_ID;
      ctx->root_page_id_ = INVALID_PAGE_ID;
      root_guard.Drop();
      ctx->write_set_.pop_back();
      bpm_->DeletePage(root_id);
      return;
    }
  } else {
    auto root_internal = root_guard.template AsMut<InternalPage>();
    
    if (root_internal->GetSize() == 1) {
      // Root has only one child - make child the new root
      page_id_t child_id = root_internal->ValueAt(0);
      
      header->root_page_id_ = child_id;
      ctx->root_page_id_ = child_id;
      root_guard.Drop();
      ctx->write_set_.pop_back();
      bpm_->DeletePage(root_id);
      return;
    }
  }
}

/*****************************************************************************
 * LEAF REBALANCING
 *****************************************************************************/

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeLeafFromLeft(LeafPage *leaf, InternalPage *parent, int leaf_index,
                                              WritePageGuard left_guard, LeafPage *left) {
  int last_idx = left->GetSize() - 1;
  KeyType borrow_key = left->KeyAt(last_idx);
  ValueType borrow_value = left->ValueAt(last_idx);
  bool borrow_is_tomb = left->IsTombstone(last_idx);
  
  auto leaf_tombs = leaf->GetTombstoneIndexes();
  auto left_tombs = left->GetTombstoneIndexes();
  
  leaf->ChangeSizeBy(1);
  
  for (int i = leaf->GetSize() - 1; i >= 1; i--) {
    leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
    leaf->SetValueAt(i, leaf->ValueAt(i - 1));
  }
  
  leaf->SetKeyAt(0, borrow_key);
  leaf->SetValueAt(0, borrow_value);
  
  std::vector<size_t> combined;
  for (size_t idx : leaf_tombs) {
    combined.push_back(idx + 1);
  }
  if (borrow_is_tomb) {
    combined.push_back(0);
  }
  
  leaf->RebuildTombstones(combined);
  
  left->ChangeSizeBy(-1);
  
  std::vector<size_t> combined_left;
  for (size_t idx : left_tombs) {
    if (idx < static_cast<size_t>(last_idx)) {
      combined_left.push_back(idx);
    }
  }
  left->RebuildTombstones(combined_left);
  
  if (leaf->GetSize() > 0) {
    parent->SetKeyAt(leaf_index, leaf->KeyAt(0));
  }
  
  left_guard.Drop();
  
  BUSTUB_ASSERT(leaf->GetSize() >= leaf->GetMinSize(),
                "RedistributeLeafFromLeft did not fix leaf underflow");
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeLeafFromRight(LeafPage *leaf, InternalPage *parent, int leaf_index,
                                               WritePageGuard right_guard, LeafPage *right) {
  KeyType borrow_key = right->KeyAt(0);
  ValueType borrow_value = right->ValueAt(0);
  bool borrow_is_tomb = right->IsTombstone(0);
  
  auto leaf_tombs = leaf->GetTombstoneIndexes();
  auto right_tombs = right->GetTombstoneIndexes();
  
  int insert_pos = leaf->GetSize();
  leaf->ChangeSizeBy(1);
  leaf->SetKeyAt(insert_pos, borrow_key);
  leaf->SetValueAt(insert_pos, borrow_value);
  
  std::vector<size_t> combined;
  for (size_t idx : leaf_tombs) {
    combined.push_back(idx);
  }
  if (borrow_is_tomb) {
    combined.push_back(static_cast<size_t>(insert_pos));
  }
  
  bool first_changed = leaf->RebuildTombstones(combined);
  
  for (int i = 0; i <= right->GetSize() - 2; i++) {
    right->SetKeyAt(i, right->KeyAt(i + 1));
    right->SetValueAt(i, right->ValueAt(i + 1));
  }
  right->ChangeSizeBy(-1);
  
  std::vector<size_t> combined_right;
  for (size_t idx : right_tombs) {
    if (idx > 0) {
      combined_right.push_back(idx - 1);
    }
  }
  right->RebuildTombstones(combined_right);
  
  parent->SetKeyAt(leaf_index + 1, right->KeyAt(0));
  
  if (first_changed && leaf->GetSize() > 0 && leaf_index > 0) {
    parent->SetKeyAt(leaf_index, leaf->KeyAt(0));
  }
  
  right_guard.Drop();
  
  BUSTUB_ASSERT(leaf->GetSize() >= leaf->GetMinSize(),
                "RedistributeLeafFromRight did not fix leaf underflow");
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceLeafWithLeft(Context *ctx, InternalPage *parent, int leaf_index,
                                           WritePageGuard left_guard, LeafPage *left) {
  auto &leaf_guard = ctx->write_set_.back();
  auto leaf = leaf_guard.template AsMut<LeafPage>();
  page_id_t leaf_id = leaf_guard.GetPageId();
  
  auto left_tombs = left->GetTombstoneIndexes();
  auto leaf_tombs = leaf->GetTombstoneIndexes();
  
  int insert_offset = left->GetSize();
  
  for (int i = 0; i <= leaf->GetSize() - 1; i++) {
    left->SetKeyAt(insert_offset + i, leaf->KeyAt(i));
    left->SetValueAt(insert_offset + i, leaf->ValueAt(i));
  }
  left->SetSize(left->GetSize() + leaf->GetSize());
  
  std::vector<size_t> combined;
  for (size_t idx : left_tombs) {
    combined.push_back(idx);
  }
  for (size_t idx : leaf_tombs) {
    combined.push_back(idx + static_cast<size_t>(insert_offset));
  }
  
  bool first_changed = left->RebuildTombstones(combined);
  
  left->SetNextPageId(leaf->GetNextPageId());
  
  int left_index = leaf_index - 1;
  if (first_changed && left->GetSize() > 0 && left_index > 0) {
    parent->SetKeyAt(left_index, left->KeyAt(0));
  }
  
  left_guard.Drop();
  
  leaf_guard.Drop();
  ctx->write_set_.pop_back();
  bpm_->DeletePage(leaf_id);
  
  for (int i = leaf_index; i <= parent->GetSize() - 2; i++) {
    parent->SetKeyAt(i, parent->KeyAt(i + 1));
    parent->SetValueAt(i, parent->ValueAt(i + 1));
  }
  parent->ChangeSizeBy(-1);
  
  if (parent->GetSize() < parent->GetMinSize()) {
    if (ctx->write_set_.size() == 1) {
      AdjustRoot(ctx);
    } else {
      CoalesceOrRedistribute(ctx);
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceLeafWithRight(Context *ctx, InternalPage *parent, int leaf_index,
                                            WritePageGuard right_guard, LeafPage *right) {
  auto &leaf_guard = ctx->write_set_.back();
  auto leaf = leaf_guard.template AsMut<LeafPage>();
  
  page_id_t right_id = right_guard.GetPageId();
  
  auto leaf_tombs = leaf->GetTombstoneIndexes();
  auto right_tombs = right->GetTombstoneIndexes();
  
  int insert_offset = leaf->GetSize();
  
  for (int i = 0; i <= right->GetSize() - 1; i++) {
    leaf->SetKeyAt(insert_offset + i, right->KeyAt(i));
    leaf->SetValueAt(insert_offset + i, right->ValueAt(i));
  }
  leaf->SetSize(leaf->GetSize() + right->GetSize());
  
  std::vector<size_t> combined;
  for (size_t idx : leaf_tombs) {
    combined.push_back(idx);
  }
  for (size_t idx : right_tombs) {
    combined.push_back(idx + static_cast<size_t>(insert_offset));
  }
  
  bool first_changed = leaf->RebuildTombstones(combined);
  
  leaf->SetNextPageId(right->GetNextPageId());
  
  if (first_changed && leaf->GetSize() > 0 && leaf_index > 0) {
    parent->SetKeyAt(leaf_index, leaf->KeyAt(0));
  }
  
  right_guard.Drop();
  bpm_->DeletePage(right_id);
  
  for (int i = leaf_index + 1; i <= parent->GetSize() - 2; i++) {
    parent->SetKeyAt(i, parent->KeyAt(i + 1));
    parent->SetValueAt(i, parent->ValueAt(i + 1));
  }
  parent->ChangeSizeBy(-1);
  
  leaf_guard.Drop();
  ctx->write_set_.pop_back();
  
  if (parent->GetSize() < parent->GetMinSize()) {
    if (ctx->write_set_.size() == 1) {
      AdjustRoot(ctx);
    } else {
      CoalesceOrRedistribute(ctx);
    }
  }
}

/*****************************************************************************
 * INTERNAL REBALANCING
 *****************************************************************************/

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeInternalFromLeft(InternalPage *internal, InternalPage *parent, int page_index,
                                                  WritePageGuard left_guard, InternalPage *left) {
  int last_idx = left->GetSize() - 1;
  page_id_t borrow_value = left->ValueAt(last_idx);
  KeyType parent_key = parent->KeyAt(page_index);
  KeyType last_key = left->KeyAt(last_idx);
  
  internal->ChangeSizeBy(1);
  int new_size = internal->GetSize();
  
  // Shift values right
  // INCLUSIVE: i = new_size-1, ..., 1
  for (int i = new_size - 1; i >= 1; i--) {
    internal->SetValueAt(i, internal->ValueAt(i - 1));
  }
  
  // Shift keys right - NEVER read KeyAt(0)
  // INCLUSIVE: i = new_size-1, ..., 2
  for (int i = new_size - 1; i >= 2; i--) {
    internal->SetKeyAt(i, internal->KeyAt(i - 1));
  }
  
  // Set KeyAt(1) to parent_key (separator)
  internal->SetKeyAt(1, parent_key);
  
  // Insert borrowed value at position 0
  internal->SetValueAt(0, borrow_value);
  
  // Remove from left
  left->ChangeSizeBy(-1);
  
  // Update parent separator
  parent->SetKeyAt(page_index, last_key);
  
  left_guard.Drop();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeInternalFromRight(InternalPage *internal, InternalPage *parent, int page_index,
                                                   WritePageGuard right_guard, InternalPage *right) {
  // Save old size BEFORE any modification
  int old_size = right->GetSize();
  
  page_id_t borrow_value = right->ValueAt(0);
  KeyType parent_key = parent->KeyAt(page_index + 1);
  KeyType new_sep = right->KeyAt(1);
  
  internal->ChangeSizeBy(1);
  internal->SetValueAt(internal->GetSize() - 1, borrow_value);
  internal->SetKeyAt(internal->GetSize() - 1, parent_key);
  
  // Shift right left
  // Value shift: INCLUSIVE i = 0, ..., old_size-2
  for (int i = 0; i <= old_size - 2; i++) {
    right->SetValueAt(i, right->ValueAt(i + 1));
  }
  
  // Key shift: INCLUSIVE i = 1, ..., old_size-2
  // NEVER read KeyAt(old_size)
  for (int i = 1; i <= old_size - 2; i++) {
    right->SetKeyAt(i, right->KeyAt(i + 1));
  }
  
  right->ChangeSizeBy(-1);
  
  // Update parent separator
  parent->SetKeyAt(page_index + 1, new_sep);
  
  right_guard.Drop();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceInternalWithLeft(Context *ctx, InternalPage *parent, int page_index,
                                              WritePageGuard left_guard, InternalPage *left) {
  auto &internal_guard = ctx->write_set_.back();
  auto internal = internal_guard.template AsMut<InternalPage>();
  page_id_t internal_id = internal_guard.GetPageId();
  
  int insert_offset = left->GetSize();
  KeyType separator = parent->KeyAt(page_index);
  
  // Set separator at boundary
  left->SetKeyAt(insert_offset, separator);
  
  // Move values
  // INCLUSIVE: i = 0, ..., internal->GetSize()-1
  for (int i = 0; i <= internal->GetSize() - 1; i++) {
    left->SetValueAt(insert_offset + i, internal->ValueAt(i));
  }
  
  // Move keys - NEVER read KeyAt(0)
  // INCLUSIVE: i = 1, ..., internal->GetSize()-1
  for (int i = 1; i <= internal->GetSize() - 1; i++) {
    left->SetKeyAt(insert_offset + i, internal->KeyAt(i));
  }
  
  left->SetSize(left->GetSize() + internal->GetSize());
  
  left_guard.Drop();
  
  internal_guard.Drop();
  ctx->write_set_.pop_back();
  bpm_->DeletePage(internal_id);
  
  for (int i = page_index; i <= parent->GetSize() - 2; i++) {
    parent->SetKeyAt(i, parent->KeyAt(i + 1));
    parent->SetValueAt(i, parent->ValueAt(i + 1));
  }
  parent->ChangeSizeBy(-1);
  
  if (parent->GetSize() < parent->GetMinSize()) {
    if (ctx->write_set_.size() == 1) {
      AdjustRoot(ctx);
    } else {
      CoalesceOrRedistribute(ctx);
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceInternalWithRight(Context *ctx, InternalPage *parent, int page_index,
                                               WritePageGuard right_guard, InternalPage *right) {
  auto &internal_guard = ctx->write_set_.back();
  auto internal = internal_guard.template AsMut<InternalPage>();
  
  page_id_t right_id = right_guard.GetPageId();
  
  int insert_offset = internal->GetSize();
  KeyType separator = parent->KeyAt(page_index + 1);
  
  // Set separator at boundary
  internal->SetKeyAt(insert_offset, separator);
  
  // Move values
  // INCLUSIVE: i = 0, ..., right->GetSize()-1
  for (int i = 0; i <= right->GetSize() - 1; i++) {
    internal->SetValueAt(insert_offset + i, right->ValueAt(i));
  }
  
  // Move keys - NEVER read KeyAt(0)
  // INCLUSIVE: i = 1, ..., right->GetSize()-1
  for (int i = 1; i <= right->GetSize() - 1; i++) {
    internal->SetKeyAt(insert_offset + i, right->KeyAt(i));
  }
  
  internal->SetSize(internal->GetSize() + right->GetSize());
  
  right_guard.Drop();
  bpm_->DeletePage(right_id);
  
  for (int i = page_index + 1; i <= parent->GetSize() - 2; i++) {
    parent->SetKeyAt(i, parent->KeyAt(i + 1));
    parent->SetValueAt(i, parent->ValueAt(i + 1));
  }
  parent->ChangeSizeBy(-1);
  
  internal_guard.Drop();
  ctx->write_set_.pop_back();
  
  if (parent->GetSize() < parent->GetMinSize()) {
    if (ctx->write_set_.size() == 1) {
      AdjustRoot(ctx);
    } else {
      CoalesceOrRedistribute(ctx);
    }
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
      int child_index = FindChildIndexRead(internal_page, key);
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
      auto leaf_page = page_guard.template As<LeafPage>();

      // Binary search for key or the position where it should be
      int index = LowerBoundRead(leaf_page, key);

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