//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.h
//
// Identification: src/include/primer/hyperloglog_presto.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "type/value.h"
#include "type/value_factory.h"

/** @brief Dense bucket size. */
static constexpr int DENSE_BUCKET_SIZE = 4;
/** @brief Overflow bucket size. */
static constexpr int OVERFLOW_BUCKET_SIZE = 3;

/** @brief Total bucket size. */
static constexpr int TOTAL_BUCKET_SIZE = (DENSE_BUCKET_SIZE + OVERFLOW_BUCKET_SIZE);

namespace bustub {

template <typename KeyType>
class HyperLogLogPresto {
  /**
   * INSTRUCTIONS: Testing framework will use the GetDenseBucket and GetOverflow function,
   * hence SHOULD NOT be deleted. It's essential to use the dense_bucket_
   * data structure.
   */

  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disabling default constructor. */
  HyperLogLogPresto() = delete;

  explicit HyperLogLogPresto(int16_t n_leading_bits);

  /** @brief Returns the dense_bucket_ data structure. */
  auto GetDenseBucket() const -> std::vector<std::bitset<DENSE_BUCKET_SIZE>> { return dense_bucket_; }

  /** @brief Returns overflow bucket of a specific given index. */
  auto GetOverflowBucketofIndex(uint16_t idx) { return overflow_bucket_[idx]; }

  /** @brief Returns the cardinality of the set. */
  auto GetCardinality() const -> uint64_t { return cardinality_; }

  auto AddElem(KeyType val) -> void;

  auto ComputeCardinality() -> void;

 private:
  /** @brief Calculate Hash.
   *
   * @param[in] val
   *
   * @returns hash value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    if constexpr (std::is_same_v<KeyType, std::string>) {
      Value val_obj = ValueFactory::GetVarcharValue(val);
      return bustub::HashUtil::HashValue(&val_obj);
    } else {
      Value val_obj = ValueFactory::GetBigIntValue(val);
      return bustub::HashUtil::HashValue(&val_obj);
    }
  }

  /** @brief Structure holding dense buckets (or also known as registers). */
  std::vector<std::bitset<DENSE_BUCKET_SIZE>> dense_bucket_;

  /** @brief Structure holding overflow buckets. */
  std::unordered_map<uint16_t, std::bitset<OVERFLOW_BUCKET_SIZE>> overflow_bucket_;

  /** @brief Storing cardinality value */
  uint64_t cardinality_;

  /** @brief Number of leading bits for index. */
  int16_t n_leading_bits_;

  /**
   * @brief Compute position of leftmost 1 in a hash value.
   *
   * @param[in] hash
   * @returns position of leftmost 1 (from MSB, 1-indexed)
   */
  inline auto PositionOfLeftmostOne(hash_t hash) const -> uint64_t {
    if (hash == 0) {
      return 65;  // Special case: all bits are 0, return 65
    }
    // __builtin_clzll returns number of leading zeros, position of leftmost 1 is clz + 1
    return __builtin_clzll(hash) + 1;
  }

  // TODO(student) - can add more data structures as required
};

}  // namespace bustub
