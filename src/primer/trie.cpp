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
#include <functional>
#include <iostream>
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
  auto node = root_;
  for (char c : key) {
    if (node == nullptr) {
      return nullptr;
    }
    auto it = node->children_.find(c);
    if (it == node->children_.end()) {
      return nullptr;
    }
    node = it->second;
  }

  if (node == nullptr || !node->is_value_node_) {
    return nullptr;
  }

  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
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
  auto value_ptr = std::make_shared<T>(std::move(value));

  // Recursive helper function that creates the new trie
  std::function<std::shared_ptr<const TrieNode>(std::shared_ptr<const TrieNode>, size_t)> insert;
  insert = [&](std::shared_ptr<const TrieNode> node, size_t depth) -> std::shared_ptr<const TrieNode> {
    // Base case: we've processed all characters in the key
    if (depth == key.size()) {
      if (node == nullptr) {
        // No existing node, create a new value node
        return std::make_shared<const TrieNodeWithValue<T>>(value_ptr);
      } else {
        // Replace existing node with value node, preserving children
        return std::make_shared<const TrieNodeWithValue<T>>(node->children_, value_ptr);
      }
    }

    // Get the current character
    char c = key[depth];
    
    // Get the child node for this character (if exists)
    std::shared_ptr<const TrieNode> child_node = nullptr;
    std::map<char, std::shared_ptr<const TrieNode>> new_children;
    
    // Copy all children to the new map
    if (node != nullptr) {
      for (const auto &[child_char, child] : node->children_) {
        new_children[child_char] = child;
      }
      
      // Get the specific child we're updating
      auto it = node->children_.find(c);
      if (it != node->children_.end()) {
        child_node = it->second;
      }
    }
    
    // Update the child for character c by recursively inserting
    new_children[c] = insert(child_node, depth + 1);

    // Create the new node with the updated children
    if (node != nullptr && node->is_value_node_) {
      // Node has a value, need to preserve it with the correct type
      if (auto *str_node = dynamic_cast<const TrieNodeWithValue<std::string>*>(node.get())) {
        return std::make_shared<const TrieNodeWithValue<std::string>>(str_node->value_, new_children);
      } else if (auto *int_node = dynamic_cast<const TrieNodeWithValue<uint32_t>*>(node.get())) {
        return std::make_shared<const TrieNodeWithValue<uint32_t>>(int_node->value_, new_children);
      } else if (auto *uint64_node = dynamic_cast<const TrieNodeWithValue<uint64_t>*>(node.get())) {
        return std::make_shared<const TrieNodeWithValue<uint64_t>>(uint64_node->value_, new_children);
      } else if (auto *integer_node = dynamic_cast<const TrieNodeWithValue<std::unique_ptr<uint32_t>>*>(node.get())) {
        return std::make_shared<const TrieNodeWithValue<std::unique_ptr<uint32_t>>>(integer_node->value_, new_children);
      } else if (auto *blocked_node = dynamic_cast<const TrieNodeWithValue<MoveBlocked>*>(node.get())) {
        return std::make_shared<const TrieNodeWithValue<MoveBlocked>>(blocked_node->value_, new_children);
      } else {
        // For unknown type, create a basic node with the updated children
        return std::make_shared<const TrieNode>(new_children);
      }
    } else {
      // Node doesn't have a value
      return std::make_shared<const TrieNode>(new_children);
    }
  };

  if (key.empty()) {
    if (root_ == nullptr) {
      return Trie(std::make_shared<const TrieNodeWithValue<T>>(value_ptr));
    } else {
      return Trie(std::make_shared<const TrieNodeWithValue<T>>(root_->children_, value_ptr));
    }
  }

  auto new_root = insert(root_, 0);
  return Trie(new_root);
}

/**
 * @brief Remove the key from the trie.
 * @return If the key does not exist, return the original trie. Otherwise, returns the new trie.
 */
auto Trie::Remove(std::string_view key) const -> Trie {
  // Check if the key exists
  auto node = root_;
  for (char c : key) {
    if (node == nullptr) {
      return *this;
    }
    auto it = node->children_.find(c);
    if (it == node->children_.end()) {
      return *this;
    }
    node = it->second;
  }

  if (node == nullptr || !node->is_value_node_) {
    return *this;
  }

  // Handle empty key case
  if (key.empty()) {
    if (root_ == nullptr || !root_->is_value_node_) {
      return *this;
    }

    // Remove the value from root, keep children
    if (root_->children_.empty()) {
      return Trie();
    }

    auto new_root = std::make_shared<const TrieNode>(root_->children_);
    return Trie(new_root);
  }

  // Helper function to recursively remove the key
  std::function<std::shared_ptr<const TrieNode>(std::shared_ptr<const TrieNode>, size_t)> remove_node;
  remove_node = [&](std::shared_ptr<const TrieNode> current_node, size_t depth) -> std::shared_ptr<const TrieNode> {
    if (depth == key.size()) {
      // At the target node, remove the value
      if (current_node->children_.empty()) {
        // If no children, return nullptr to remove this node
        return nullptr;
      } else {
        // If has children, return a new node without the value
        return std::make_shared<const TrieNode>(current_node->children_);
      }
    }

    char c = key[depth];
    
    // Get the child to process
    auto child_it = current_node->children_.find(c);
    if (child_it == current_node->children_.end()) {
      // This shouldn't happen if we checked existence earlier, but return original if so
      return current_node;
    }
    
    // Process the child recursively
    auto new_child = remove_node(child_it->second, depth + 1);
    
    // Update children map
    std::map<char, std::shared_ptr<const TrieNode>> new_children = current_node->children_;
    if (new_child == nullptr) {
      new_children.erase(c);
      // If current node has no value and no children after removal, remove the node
      if (new_children.empty() && !current_node->is_value_node_) {
        return nullptr;
      }
    } else {
      new_children[c] = new_child;
    }
    
    // Preserve the value if the current node has one
    if (current_node->is_value_node_) {
      // Handle different value types
      if (auto *str_node = dynamic_cast<const TrieNodeWithValue<std::string>*>(current_node.get())) {
        return std::make_shared<const TrieNodeWithValue<std::string>>(str_node->value_, new_children);
      } else if (auto *int_node = dynamic_cast<const TrieNodeWithValue<uint32_t>*>(current_node.get())) {
        return std::make_shared<const TrieNodeWithValue<uint32_t>>(int_node->value_, new_children);
      } else if (auto *uint64_node = dynamic_cast<const TrieNodeWithValue<uint64_t>*>(current_node.get())) {
        return std::make_shared<const TrieNodeWithValue<uint64_t>>(uint64_node->value_, new_children);
      } else if (auto *integer_node = dynamic_cast<const TrieNodeWithValue<std::unique_ptr<uint32_t>>*>(current_node.get())) {
        return std::make_shared<const TrieNodeWithValue<std::unique_ptr<uint32_t>>>(integer_node->value_, new_children);
      } else if (auto *blocked_node = dynamic_cast<const TrieNodeWithValue<MoveBlocked>*>(current_node.get())) {
        return std::make_shared<const TrieNodeWithValue<MoveBlocked>>(blocked_node->value_, new_children);
      }
      // For other types, create a regular node with updated children
      auto new_node = std::make_shared<TrieNode>(new_children);
      new_node->is_value_node_ = current_node->is_value_node_;
      return new_node;
    } else {
      return std::make_shared<const TrieNode>(new_children);
    }
  };

  // Remove the key from the trie
  auto new_root = remove_node(root_, 0);
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
