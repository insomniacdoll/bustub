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
  return elements_.find(elem) != elements_.end();
}

/**
 * @brief Adds an element to the set.
 *
 * @param elem the element to add
 * @param uid unique token associated with the add operation.
 */
template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  elements_[elem].push_back(uid);
}

/**
 * @brief Removes an element from the set if it exists.
 *
 * @param elem the element to remove.
 */
template <typename T>
void ORSet<T>::Remove(const T &elem) {
  if (elements_.find(elem) != elements_.end()) {
    elements_.erase(elem);
  }
}

/**
 * @brief Merge changes from another ORSet.
 *
 * @param other another ORSet
 */
template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  for (const auto &[elem, uids] : other.elements_) {
    for (const auto &uid : uids) {
      Add(elem, uid);
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
  for (const auto &[elem, _] : elements_) {
    result.push_back(elem);
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