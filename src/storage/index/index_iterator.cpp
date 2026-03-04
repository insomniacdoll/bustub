//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, ReadPageGuard guard, int index,
                                  page_id_t root_page_id)
    : bpm_(std::move(bpm)), guard_(std::move(guard)), index_(index), root_page_id_(root_page_id), is_end_(false) {
  // Skip tombstones
  SkipTombstones();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : bpm_(nullptr), index_(0), root_page_id_(INVALID_PAGE_ID), is_end_(true) {}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return is_end_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto leaf = guard_.value().template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  return {leaf->KeyAt(index_), leaf->ValueAt(index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto leaf = guard_.value().template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  index_++;

  // Check if we need to move to the next page
  if (index_ >= leaf->GetSize()) {
    page_id_t next_page_id = leaf->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) {
      guard_.value().Drop();
      guard_ = bpm_->ReadPage(next_page_id);
      index_ = 0;
      SkipTombstones();
    } else {
      // Reached the end
      is_end_ = true;
      guard_.value().Drop();
      guard_ = std::nullopt;
    }
  } else {
    // Still on the same page, check for tombstones
    SkipTombstones();
  }

  return *this;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::SkipTombstones() {
  if (is_end_ || !guard_.has_value()) {
    return;
  }

  while (true) {
    auto leaf = guard_.value().template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();

    // Check if current index is within bounds
    if (index_ >= leaf->GetSize()) {
      page_id_t next_page_id = leaf->GetNextPageId();
      if (next_page_id != INVALID_PAGE_ID) {
        guard_.value().Drop();
        guard_ = bpm_->ReadPage(next_page_id);
        index_ = 0;
        continue;
      } else {
        is_end_ = true;
        guard_.value().Drop();
        guard_ = std::nullopt;
        return;
      }
    }

    // Check if current index has a tombstone
    if (leaf->IsTombstone(index_)) {
      index_++;
    } else {
      break;
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (is_end_ && itr.is_end_) {
    return true;
  }
  if (is_end_ || itr.is_end_) {
    return false;
  }
  // Both are not end iterators
  return guard_.value().GetPageId() == itr.guard_.value().GetPageId() && index_ == itr.index_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return !(*this == itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
