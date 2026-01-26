//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  if (width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("Width and depth must be non-zero");
  }

  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  // Initialize count matrix with atomic counters
  count_matrix_ = std::make_unique<std::unique_ptr<std::atomic<uint32_t>[]>[]>(depth_);
  for (uint32_t i = 0; i < depth_; i++) {
    count_matrix_[i] = std::make_unique<std::atomic<uint32_t>[]>(width_);
    for (uint32_t j = 0; j < width_; j++) {
      count_matrix_[i][j].store(0, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  count_matrix_ = std::move(other.count_matrix_);

  // Rebuild hash functions for this object
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }

  // Clear the moved-from object
  other.width_ = 0;
  other.depth_ = 0;
  other.hash_functions_.clear();
  other.count_matrix_.reset();
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    count_matrix_ = std::move(other.count_matrix_);

    // Rebuild hash functions for this object
    hash_functions_.reserve(depth_);
    for (size_t i = 0; i < depth_; i++) {
      hash_functions_.push_back(this->HashFunction(i));
    }

    // Clear the moved-from object
    other.width_ = 0;
    other.depth_ = 0;
    other.hash_functions_.clear();
    other.count_matrix_.reset();
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  for (uint32_t i = 0; i < depth_; i++) {
    size_t col = hash_functions_[i](item);
    count_matrix_[i][col].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  for (uint32_t i = 0; i < depth_; i++) {
    for (uint32_t j = 0; j < width_; j++) {
      uint32_t other_count = other.count_matrix_[i][j].load(std::memory_order_relaxed);
      if (other_count > 0) {
        count_matrix_[i][j].fetch_add(other_count, std::memory_order_relaxed);
      }
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_count = std::numeric_limits<uint32_t>::max();
  for (uint32_t i = 0; i < depth_; i++) {
    size_t col = hash_functions_[i](item);
    uint32_t count = count_matrix_[i][col].load(std::memory_order_relaxed);
    if (count < min_count) {
      min_count = count;
    }
  }
  return min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  for (uint32_t i = 0; i < depth_; i++) {
    for (uint32_t j = 0; j < width_; j++) {
      count_matrix_[i][j].store(0, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  std::vector<std::pair<KeyType, uint32_t>> results;
  for (const auto &candidate : candidates) {
    results.emplace_back(candidate, Count(candidate));
  }

  // Sort by count in descending order
  std::sort(results.begin(), results.end(),
            [](const std::pair<KeyType, uint32_t> &a, const std::pair<KeyType, uint32_t> &b) {
              return a.second > b.second;
            });

  // Return top k
  if (results.size() > static_cast<size_t>(k)) {
    results.resize(k);
  }
  return results;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
