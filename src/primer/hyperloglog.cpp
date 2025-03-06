//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), n_bits_(n_bits), m_(1 << n_bits), registers_(m_, 0) {}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(hash);
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  for (size_t i = 0; i < BITSET_CAPACITY; ++i) {
    if (bset.test(i)) {
      return i;
    }
  }
  return BITSET_CAPACITY;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  auto hash = CalculateHash(val);
  auto binary = ComputeBinary(hash);
  auto index = binary.to_ulong() & ((1ULL << n_bits_) - 1);
  auto rank = PositionOfLeftmostOne(binary >> n_bits_) + 1;
  registers_[index] = std::max(registers_[index], static_cast<uint8_t>(rank));
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;
  for (auto reg : registers_) {
    sum += 1.0 / std::pow(2.0, reg);
  }
  double estimate = CONSTANT * m_ * m_ / sum;
  if (estimate <= 2.5 * m_) {
    size_t zeros = std::count(registers_.begin(), registers_.end(), 0);
    if (zeros != 0) {
      estimate = m_ * std::log(static_cast<double>(m_) / zeros);
    }
  }
  cardinality_ = static_cast<size_t>(estimate);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub