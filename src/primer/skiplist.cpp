//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.cpp
//
// Identification: src/primer/skiplist.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/skiplist.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>
#include "common/macros.h"
#include "fmt/core.h"

namespace bustub {



/** @brief Checks whether the container is empty. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Empty() -> bool {
  std::shared_lock<std::shared_mutex> lock(rwlock_);
  return size_ == 0;
}

/** @brief Returns the number of elements in the skip list. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Size() -> size_t {
  std::shared_lock<std::shared_mutex> lock(rwlock_);
  return size_;
}

/**
 * @brief Iteratively deallocate all the nodes.
 *
 * We do this to avoid stack overflow when the skip list is large.
 *
 * If we let the compiler handle the deallocation, it will recursively call the destructor of each node,
 * which could block up the stack.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Drop() {
  for (size_t i = 0; i < MaxHeight; i++) {
    auto curr = std::move(header_->links_[i]);
    while (curr != nullptr) {
      // std::move sets `curr` to the old value of `curr->links_[i]`,
      // and then resets `curr->links_[i]` to `nullptr`.
      curr = std::move(curr->links_[i]);
    }
  }
}

/**
 * @brief Removes all elements from the skip list.
 *
 * Note: You might want to use the provided `Drop` helper function.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Clear() {
  std::unique_lock<std::shared_mutex> lock(rwlock_);
  Drop();
  header_ = std::make_shared<SkipNode>(MaxHeight);
  height_ = 1;
  size_ = 0;
}

/**
 * @brief Inserts a key into the skip list.
 *
 * Note: `Insert` will not insert the key if it already exists in the skip list.
 *
 * @param key key to insert.
 * @return true if the insertion is successful, false if the key already exists.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Insert(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(rwlock_);
  std::vector<std::shared_ptr<SkipNode>> update(MaxHeight);
  auto node = header_;
  for (int i = height_ - 1; i >= 0; --i) {
    while (node->Next(i) != nullptr && compare_(node->Next(i)->Key(), key)) {
      node = node->Next(i);
    }
    update[i] = node;
  }
  node = node->Next(0);
  if (node != nullptr && !compare_(key, node->Key()) && !compare_(node->Key(), key)) {
    return false;
  }
  size_t height = RandomHeight();
  if (height > height_) {
    for (size_t i = height_; i < height; ++i) {
      update[i] = header_;
    }
    height_ = height;
  }
  node = std::make_shared<SkipNode>(height, key);
  for (size_t i = 0; i < height; ++i) {
    node->SetNext(i, update[i]->Next(i));
    update[i]->SetNext(i, node);
  }
  size_++;  // Increment size when inserting a new key
  return true;
}

/**
 * @brief Erases the key from the skip list.
 *
 * @param key key to erase.
 * @return bool true if the element got erased, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Erase(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(rwlock_);
  std::vector<std::shared_ptr<SkipNode>> update(MaxHeight);
  auto node = header_;
  for (int i = height_ - 1; i >= 0; --i) {
    while (node->Next(i) != nullptr && compare_(node->Next(i)->Key(), key)) {
      node = node->Next(i);
    }
    update[i] = node;
  }
  node = node->Next(0);
  if (node == nullptr || compare_(key, node->Key()) || compare_(node->Key(), key)) {
    return false;
  }
  for (size_t i = 0; i < height_; ++i) {
    if (update[i]->Next(i) != node) {
      break;
    }
    update[i]->SetNext(i, node->Next(i));
  }
  while (height_ > 1 && header_->Next(height_ - 1) == nullptr) {
    --height_;
  }
  --size_;  // Decrement size when erasing a key
  return true;
}

/**
 * @brief Checks whether a key exists in the skip list.
 *
 * @param key key to look up.
 * @return bool true if the element exists, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Contains(const K &key) -> bool {
  std::shared_lock<std::shared_mutex> lock(rwlock_);
  auto node = header_;
  for (int i = height_ - 1; i >= 0; --i) {
    while (node->Next(i) != nullptr && compare_(node->Next(i)->Key(), key)) {
      node = node->Next(i);
    }
  }
  node = node->Next(0);
  return node != nullptr && !compare_(key, node->Key()) && !compare_(node->Key(), key);
}

/**
 * @brief Prints the skip list for debugging purposes.
 *
 * Note: You may modify the functions in any way and the output is not tested.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Print() {
  auto node = header_->Next(LOWEST_LEVEL);
  while (node != nullptr) {
    fmt::println("Node {{ key: {}, height: {} }}", node->Key(), node->Height());
    node = node->Next(LOWEST_LEVEL);
  }
}

/**
 * @brief Generate a random height. The height should be capped at `MaxHeight`.
 * Note: we implement/simulate the geometric process to ensure platform independence.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::RandomHeight() -> size_t {
  // Branching factor (1 in 4 chance), see Pugh's paper.
  static constexpr unsigned int branching_factor = 4;
  // Start with the minimum height
  size_t height = 1;
  while (height < MaxHeight && (rng_() % branching_factor == 0)) {
    height++;
  }
  return height;
}

/**
 * @brief Gets the current node height.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Height() const -> size_t {
  return links_.size();
}

/**
 * @brief Gets the next node by following the link at `level`.
 *
 * @param level index to the link.
 * @return std::shared_ptr<SkipNode> the next node, or `nullptr` if such node does not exist.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Next(size_t level) const
    -> std::shared_ptr<SkipNode> {
  if (level >= links_.size()) {
    return nullptr;
  }
  return links_[level];
}

/**
 * @brief Set the `node` to be linked at `level`.
 *
 * @param level index to the link.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::SkipNode::SetNext(
    size_t level, const std::shared_ptr<SkipNode> &node) {
  if (level >= links_.size()) {
    links_.resize(level + 1);
  }
  links_[level] = node;
}

/** @brief Returns a reference to the key stored in the node. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Key() const -> const K & {
  return key_;
}

// Below are explicit instantiation of template classes.
template class SkipList<int>;
template class SkipList<std::string>;
template class SkipList<int, std::greater<>>;
template class SkipList<int, std::less<>, 8>;

}  // namespace bustub
