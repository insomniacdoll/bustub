//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"
#include <iostream>

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : cardinality_(0), n_leading_bits_(n_leading_bits) {
  size_t num_buckets = (n_leading_bits_ > 0) ? (1ULL << n_leading_bits_) : 1;
  dense_bucket_.resize(num_buckets);
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  auto hash = CalculateHash(val);

  // Calculate index using the low n_leading_bits bits
  uint16_t index;
  if (n_leading_bits_ > 0) {
    index = hash & ((1ULL << n_leading_bits_) - 1);
  } else {
    index = 0;
  }

  // Calculate rank (position of leftmost 1 in the remaining bits from MSB)
  hash_t remaining_bits = hash >> n_leading_bits_;
  uint64_t rank = PositionOfLeftmostOne(remaining_bits);

  // Get current max rank from dense bucket
  uint64_t current_max_rank = 0;
  // Count consecutive 1s from bit 0 in dense bucket (cumulative representation)
  for (int i = 0; i < DENSE_BUCKET_SIZE; i++) {
    if (dense_bucket_[index].test(i)) {
      current_max_rank = i + 1;
    } else {
      break; // Stop at first 0 (cumulative representation)
    }
  }

  // Check overflow if dense bucket is full
  if (current_max_rank == DENSE_BUCKET_SIZE) {
    auto overflow_it = overflow_bucket_.find(index);
    if (overflow_it != overflow_bucket_.end()) {
      for (int i = 0; i < OVERFLOW_BUCKET_SIZE; i++) {
        if (overflow_it->second.test(i)) {
          current_max_rank = DENSE_BUCKET_SIZE + i + 1;
        }
      }
    }
  }

  // Update if new rank is greater
  if (rank > current_max_rank) {
    if (rank <= DENSE_BUCKET_SIZE) {
      // Set bits 0 to rank-1 in dense bucket (cumulative)
      for (int i = 0; i < rank; i++) {
        dense_bucket_[index].set(i);
      }
      // Clear overflow
      overflow_bucket_.erase(index);
    } else {
      // Fill dense bucket completely
      for (int i = 0; i < DENSE_BUCKET_SIZE; i++) {
        dense_bucket_[index].set(i);
      }
      
      // Handle overflow
      uint64_t overflow_rank = rank - DENSE_BUCKET_SIZE;
      if (overflow_rank <= OVERFLOW_BUCKET_SIZE) {
        overflow_bucket_[index] = std::bitset<OVERFLOW_BUCKET_SIZE>();
        overflow_bucket_[index].set(overflow_rank - 1);
      } else {
        // For very large overflow, set all overflow bits
        for (int i = 0; i < OVERFLOW_BUCKET_SIZE; i++) {
          overflow_bucket_[index].set(i);
        }
      }
    }
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  size_t num_buckets = (n_leading_bits_ > 0) ? (1ULL << n_leading_bits_) : 1;

  double sum = 0.0;
  for (size_t i = 0; i < num_buckets; i++) {
    uint64_t rank = 0;

    // Get rank from dense_bucket (find the highest set bit + 1)
    for (int j = DENSE_BUCKET_SIZE - 1; j >= 0; j--) {
      if (dense_bucket_[i].test(j)) {
        rank = j + 1;
        break;
      }
    }

    // Check if there's overflow (if dense bucket is all 1s, check overflow)
    bool is_dense_full = true;
    for (int j = 0; j < DENSE_BUCKET_SIZE; j++) {
      if (!dense_bucket_[i].test(j)) {
        is_dense_full = false;
        break;
      }
    }
    
    if (is_dense_full && rank == DENSE_BUCKET_SIZE) {
      auto overflow_it = overflow_bucket_.find(static_cast<uint16_t>(i));
      if (overflow_it != overflow_bucket_.end()) {
        // Get overflow rank
        for (int j = OVERFLOW_BUCKET_SIZE - 1; j >= 0; j--) {
          if (overflow_it->second.test(j)) {
            rank = DENSE_BUCKET_SIZE + j + 1;  // Add overflow to max dense value
            break;
          }
        }
      }
    }

    // For empty buckets, rank should be 1
    if (rank == 0) {
      rank = 1;
    }
    sum += 1.0 / std::pow(2.0, rank);
  }

  double estimate = CONSTANT * num_buckets * num_buckets / sum;

  // Apply small range correction
  if (estimate <= 2.5 * num_buckets) {
    size_t zeros = 0;
    for (size_t i = 0; i < num_buckets; i++) {
      // Count as zero if dense bucket has no bits set and no overflow exists
      bool is_zero = dense_bucket_[i].none();
      auto overflow_it = overflow_bucket_.find(static_cast<uint16_t>(i));
      if (is_zero && (overflow_it == overflow_bucket_.end() || overflow_it->second.none())) {
        zeros++;
      }
    }
    if (zeros != 0) {
      estimate = num_buckets * std::log(static_cast<double>(num_buckets) / zeros);
    }
  }

  cardinality_ = static_cast<uint64_t>(estimate);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
