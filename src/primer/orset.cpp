//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// orset.cpp
//
// Identification: src/primer/orset.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"
#include "fmt/ranges.h"

namespace bustub {

/**
 * @brief Checks if an element is in the set.
 *
 * @param elem the element to check
 * @return true if the element is in the set, and false otherwise.
 */
template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  auto add_it = add_set_.find(elem);
  auto remove_it = remove_set_.find(elem);

  // If element was never added, it's not in the set
  if (add_it == add_set_.end()) {
    return false;
  }

  // If element was added but never removed, it's in the set
  if (remove_it == remove_set_.end()) {
    return true;
  }

  // Check if any of the add UIDs are not in the remove set
  const auto &add_uids = add_it->second;
  const auto &remove_uids = remove_it->second;

  for (const auto &add_uid : add_uids) {
    if (std::find(remove_uids.begin(), remove_uids.end(), add_uid) == remove_uids.end()) {
      return true; // At least one add operation is not removed
    }
  }

  return false; // All add operations have been removed
}

/**
 * @brief Adds an element to the set.
 *
 * @param elem the element to add
 * @param uid unique token associated with the add operation.
 */
template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  add_set_[elem].push_back(uid);
}

/**
 * @brief Removes an element from the set if it exists.
 *
 * @param elem the element to remove.
 */
template <typename T>
void ORSet<T>::Remove(const T &elem) {
  auto add_it = add_set_.find(elem);
  if (add_it != add_set_.end()) {
    // Add all existing add UIDs to the remove set
    for (const auto &uid : add_it->second) {
      auto &remove_uids = remove_set_[elem];
      if (std::find(remove_uids.begin(), remove_uids.end(), uid) == remove_uids.end()) {
        remove_uids.push_back(uid);
      }
    }
  }
}

/**
 * @brief Merge changes from another ORSet.
 *
 * @param other another ORSet
 */
template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // Merge add operations
  for (const auto &[elem, other_add_uids] : other.add_set_) {
    auto &local_add_uids = add_set_[elem];
    for (const auto &uid : other_add_uids) {
      if (std::find(local_add_uids.begin(), local_add_uids.end(), uid) == local_add_uids.end()) {
        local_add_uids.push_back(uid);
      }
    }
  }

  // Merge remove operations
  for (const auto &[elem, other_remove_uids] : other.remove_set_) {
    auto &local_remove_uids = remove_set_[elem];
    for (const auto &uid : other_remove_uids) {
      if (std::find(local_remove_uids.begin(), local_remove_uids.end(), uid) == local_remove_uids.end()) {
        local_remove_uids.push_back(uid);
      }
    }
  }
}

/**
 * @brief Gets all the elements in the set.
 *
 * @return all the elements in the set.
 */
template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  std::vector<T> result;
  for (const auto &[elem, _] : add_set_) {
    if (Contains(elem)) {
      result.push_back(elem);
    }
  }
  return result;
}

/**
 * @brief Gets a string representation of the set.
 *
 * @return a string representation of the set.
 */
template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub