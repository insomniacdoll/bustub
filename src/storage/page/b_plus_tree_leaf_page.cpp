//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  BUSTUB_ASSERT(max_size >= 0 && static_cast<size_t>(max_size) <= LEAF_PAGE_SLOT_CNT,
                "leaf max_size exceeds physical capacity");
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
  num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> tombstone_keys;
  for (size_t i = 0; i < num_tombstones_; i++) {
    tombstone_keys.push_back(key_array_[tombstones_[i]]);
  }
  return tombstone_keys;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "KeyAt: index out of bounds");
  return key_array_[index];
}

/*
 * Helper method to set the key at a specific index
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "SetKeyAt: index out of bounds");
  key_array_[index] = key;
}

/*
 * Helper method to find and return the value (RID) associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "ValueAt: index out of bounds");
  return rid_array_[index];
}

/*
 * Helper method to set the value (RID) at a specific index
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < GetMaxSize(), "SetValueAt: index out of bounds");
  rid_array_[index] = value;
}

/**
 * @brief Physically delete the entry at the given index.
 *
 * Shifts all subsequent entries left, decrements size, and adjusts tombstones.
 * This is called when the tombstone buffer overflows and the oldest
 * tombstoned entry must be physically removed.
 *
 * PRECONDITION: 0 <= index < GetSize()
 *
 * @param index The physical index of the entry to delete
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PhysicalDeleteAt(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "PhysicalDeleteAt: index out of bounds");

  // Shift entries left to fill the gap
  for (int i = index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }

  // Decrement physical size
  ChangeSizeBy(-1);

  // Adjust tombstone indexes to account for the deletion
  AdjustTombstonesAfterDelete(index);
}

/**
 * @brief Adjust tombstone indexes after a physical deletion.
 *
 * When an entry at deleted_index is physically removed:
 * 1. Any tombstone pointing to deleted_index is removed from the buffer
 * 2. Any tombstone pointing to an index > deleted_index is decremented by 1
 * 3. Tombstones pointing to indices < deleted_index are unchanged
 *
 * @param deleted_index The physical index that was deleted
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::AdjustTombstonesAfterDelete(int deleted_index) {
  size_t deleted_idx = static_cast<size_t>(deleted_index);

  for (size_t i = 0; i < num_tombstones_;) {
    if (tombstones_[i] == deleted_idx) {
      // Remove this tombstone (shift remaining left)
      for (size_t j = i; j < num_tombstones_ - 1; j++) {
        tombstones_[j] = tombstones_[j + 1];
      }
      num_tombstones_--;
      // Don't increment i; check the new value at position i
    } else if (tombstones_[i] > deleted_idx) {
      // Decrement this tombstone index (entries shifted left)
      tombstones_[i]--;
      i++;
    } else {
      // Tombstone index < deleted_idx, no change needed
      i++;
    }
  }
}

/*
 * Helper method to check if an index is a tombstone
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstone(int index) const -> bool {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "IsTombstone: index out of bounds");
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (tombstones_[i] == static_cast<size_t>(index)) {
      return true;
    }
  }
  return false;
}

/**
 * @brief Add a tombstone for the given index.
 *
 * Tombstones mark entries as logically deleted without physically removing them.
 * When the tombstone buffer is full, the oldest tombstoned entry is physically
 * deleted to make room for the new tombstone.
 *
 * If NumTombs == 0, entries are physically deleted immediately without buffering.
 *
 * PRECONDITION: 0 <= index < GetSize()
 *
 * @param index The physical index of the entry to tombstone
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::AddTombstone(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "AddTombstone: index out of bounds");

  // Handle NumTombs == 0 case: immediate physical deletion, no buffering
  // Use if constexpr with else to ensure tombstone buffer code is not compiled
  // when LEAF_PAGE_TOMB_CNT == 0 (zero-sized array access would be problematic)
  if constexpr (LEAF_PAGE_TOMB_CNT == 0) {
    PhysicalDeleteAt(index);
    return;
  } else {
    // Non-zero tombstone capacity: use buffered deletion

    // Check if this index already has a tombstone (idempotent)
    for (size_t i = 0; i < num_tombstones_; i++) {
      if (tombstones_[i] == static_cast<size_t>(index)) {
        return;  // Already tombstoned, nothing to do
      }
    }

    // If buffer has room, simply add the new tombstone
    if (num_tombstones_ < static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
      tombstones_[num_tombstones_] = static_cast<size_t>(index);
      num_tombstones_++;
      return;
    }

    // Buffer is full - must flush the oldest tombstone
    // Step 1: Get the oldest tombstone index (at position 0)
    size_t oldest_index = tombstones_[0];

    // Step 2: Remove oldest from buffer (shift all left)
    for (size_t i = 0; i < num_tombstones_ - 1; i++) {
      tombstones_[i] = tombstones_[i + 1];
    }
    num_tombstones_--;

    // Step 3: Physically delete the oldest tombstoned entry
    // This also adjusts remaining tombstone indexes via AdjustTombstonesAfterDelete
    PhysicalDeleteAt(static_cast<int>(oldest_index));

    // Step 4: Adjust the new index if it was after the deleted position
    if (static_cast<size_t>(index) > oldest_index) {
      index--;
    }

    // Step 5: Defensive check - don't add duplicate after adjustment
    for (size_t i = 0; i < num_tombstones_; i++) {
      if (tombstones_[i] == static_cast<size_t>(index)) {
        return;  // Already tombstoned after adjustment
      }
    }

    // Step 6: Add the new tombstone
    tombstones_[num_tombstones_] = static_cast<size_t>(index);
    num_tombstones_++;
  }
}

/**
 * @brief Remove a specific tombstone by index
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveTombstone(int index) {
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (tombstones_[i] == static_cast<size_t>(index)) {
      // Shift remaining tombstones
      for (size_t j = i; j < num_tombstones_ - 1; j++) {
        tombstones_[j] = tombstones_[j + 1];
      }
      num_tombstones_--;
      return;
    }
  }
}

/**
 * @brief Clear a tombstone at the given index
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ClearTombstone(int index) {
  RemoveTombstone(index);
}

/**
 * @brief Get the number of tombstones
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNumTombstones() const -> size_t {
  return num_tombstones_;
}

/**
 * @brief Get tombstone indexes in recency order (oldest first).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstoneIndexes() const -> std::vector<size_t> {
  std::vector<size_t> indexes;
  indexes.reserve(num_tombstones_);
  for (size_t i = 0; i < num_tombstones_; i++) {
    indexes.push_back(tombstones_[i]);
  }
  return indexes;
}

/**
 * @brief Get tombstone buffer capacity (max number of tombstones).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstoneCapacity() const -> size_t {
  return static_cast<size_t>(LEAF_PAGE_TOMB_CNT);
}

/**
 * @brief Rebuild tombstones with overflow handling.
 * 
 * Returns true if the first physical key changed (entry at index 0 was deleted).
 * Caller must check GetSize() > 0 before reading KeyAt(0).
 * 
 * Loop boundaries: All loops use inclusive bounds.
 * Example: "i from 0 to n-1" means i = 0, 1, ..., n-1
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RebuildTombstones(std::vector<size_t> indexes) -> bool {
  // Validate all indexes within current size
  for (size_t idx : indexes) {
    BUSTUB_ASSERT(idx < static_cast<size_t>(GetSize()), 
                  "RebuildTombstones: index out of bounds");
  }
  
  bool first_key_changed = false;
  
  // Special case: zero capacity means all tombstones physically applied
  if constexpr (LEAF_PAGE_TOMB_CNT == 0) {
    while (!indexes.empty()) {
      size_t delete_idx = indexes.front();
      indexes.erase(indexes.begin());
      
      // Check against CURRENT rebased index (after previous deletions)
      if (delete_idx == 0) {
        first_key_changed = true;
      }
      
      // Physically delete: shift entries from delete_idx to size-2
      // INCLUSIVE: i = delete_idx, delete_idx+1, ..., GetSize()-2
      for (int i = static_cast<int>(delete_idx); i <= GetSize() - 2; i++) {
        key_array_[i] = key_array_[i + 1];
        rid_array_[i] = rid_array_[i + 1];
      }
      ChangeSizeBy(-1);
      
      // Rebase remaining indexes
      std::vector<size_t> rebased;
      for (size_t idx : indexes) {
        if (idx != delete_idx) {
          rebased.push_back(idx > delete_idx ? idx - 1 : idx);
        }
      }
      indexes = rebased;
    }
    
    num_tombstones_ = 0;
    return first_key_changed;
  }
  
  // Normal case: apply overflow tombstones
  while (indexes.size() > static_cast<size_t>(LEAF_PAGE_TOMB_CNT)) {
    size_t delete_idx = indexes.front();
    indexes.erase(indexes.begin());
    
    // Check against CURRENT rebased index
    if (delete_idx == 0) {
      first_key_changed = true;
    }
    
    // Physically delete: INCLUSIVE loop
    for (int i = static_cast<int>(delete_idx); i <= GetSize() - 2; i++) {
      key_array_[i] = key_array_[i + 1];
      rid_array_[i] = rid_array_[i + 1];
    }
    ChangeSizeBy(-1);
    
    // Rebase: remove duplicates, decrement greater indexes
    std::vector<size_t> rebased;
    for (size_t idx : indexes) {
      if (idx != delete_idx) {
        rebased.push_back(idx > delete_idx ? idx - 1 : idx);
      }
    }
    indexes = rebased;
  }
  
  // Install remaining indexes
  num_tombstones_ = indexes.size();
  for (size_t i = 0; i < num_tombstones_; i++) {
    tombstones_[i] = indexes[i];
  }
  
  return first_key_changed;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
