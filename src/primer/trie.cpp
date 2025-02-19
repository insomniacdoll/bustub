//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// trie.cpp
//
// Identification: src/primer/trie.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

/**
 * @brief Get the value associated with the given key.
 * 1. If the key is not in the trie, return nullptr.
 * 2. If the key is in the trie but the type is mismatched, return nullptr.
 * 3. Otherwise, return the value.
 */
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto current = root_;
  for (auto ch : key) {
    auto it = current->children_.find(ch);
    if (it == current->children_.end()) {
      return nullptr;
    }
    current = it->second;
  }
  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(current.get());
  if (value_node == nullptr) {
    return nullptr;
  }
  return value_node->value_.get();
}

/**
 * @brief Put a new key-value pair into the trie. If the key already exists, overwrite the value.
 * @return the new trie.
 */
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  auto new_root = std::make_shared<TrieNode>(*root_);
  std::shared_ptr<TrieNode> current = new_root;
  for (auto ch : key) {
    auto it = current->children_.find(ch);
    if (it == current->children_.end()) {
      current->children_[ch] = std::make_shared<TrieNode>();
    } else {
      current = std::const_pointer_cast<TrieNode>(it->second);
    }
  }
  current = std::make_shared<TrieNodeWithValue<T>>(current->children_, std::make_shared<T>(std::move(value)));
  return Trie(new_root);
}

/**
 * @brief Remove the key from the trie.
 * @return If the key does not exist, return the original trie. Otherwise, returns the new trie.
 */
auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto new_root = std::make_shared<TrieNode>(*root_);
  auto current = new_root;
  std::vector<std::shared_ptr<TrieNode>> path;
  path.push_back(current);
  for (auto ch : key) {
    auto it = current->children_.find(ch);
    if (it == current->children_.end()) {
      return Trie(root_);
    }
    current = std::const_pointer_cast<TrieNode>(it->second);
    path.push_back(current);
  }
  if (!current->is_value_node_) {
    return Trie(root_);
  }
  current = std::make_shared<TrieNode>(current->children_);
  path.pop_back();
  for (auto it = path.rbegin(); it != path.rend(); ++it) {
    auto node = *it;
    auto ch = key[it - path.rbegin()];
    if (current->children_.empty() && !current->is_value_node_) {
      node->children_.erase(ch);
    } else {
      node->children_[ch] = current;
    }
    if (!node->children_.empty() || node->is_value_node_) {
      break;
    }
    current = node;
  }
  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
