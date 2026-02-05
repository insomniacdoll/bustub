#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

/**
 * @brief This function returns a ValueGuard object that holds a reference to the value in the trie. If
 * the key does not exist in the trie, it will return std::nullopt.
 */
template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // (1) Take the root lock, get the root, and release the root lock.
  std::shared_lock<std::shared_mutex> root_lock(root_lock_);
  const T* value = root_.Get<T>(key);

  // (2) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  if (value) {
    return std::make_optional<ValueGuard<T>>(root_, *value);
  }
  return std::nullopt;
}

/**
 * @brief This function will insert the key-value pair into the trie. If the key already exists in the
 * trie, it will overwrite the value.
 */
template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // Ensure there is only one writer at a time.
  std::lock_guard<std::mutex> write_lock(write_lock_);

  // Move the value before acquiring the root lock to avoid deadlocks when T's move constructor blocks
  auto moved_value = std::move(value);

  std::unique_lock<std::shared_mutex> root_lock(root_lock_);

  // Insert or update the value in the trie and update the root.
  root_ = root_.Put(key, std::move(moved_value));
}

/** @brief This function will remove the key-value pair from the trie. */
void TrieStore::Remove(std::string_view key) {
  // Ensure there is only one writer at a time.
  std::lock_guard<std::mutex> write_lock(write_lock_);
  std::unique_lock<std::shared_mutex> root_lock(root_lock_);

  // Remove the value from the trie and update the root.
  root_ = root_.Remove(key);
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub