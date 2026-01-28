// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <optional>
#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames), curr_size_(0), mru_target_size_(0) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  
  // If no frames can be evicted, return nullopt
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  frame_id_t frame_id = -1;
  std::shared_ptr<FrameStatus> frame_status = nullptr;

  // According to ARC algorithm: if |L1| >= p, victimize from L1 (MRU side), else from L2 (MFU side)
  // L1 = MRU + MRU_ghost, but for eviction we only consider MRU (live frames)
  // When mru_.size() == mru_target_size_, we still evict from mru_ (per test expectations)
  bool victimize_from_mru = mru_.size() >= mru_target_size_;

  if (victimize_from_mru) {
    // Try to evict from MRU first
    for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
      frame_id_t fid = *it;
      auto frame_it = alive_map_.find(fid);
      if (frame_it != alive_map_.end() && frame_it->second->evictable_) {
        frame_id = fid;
        frame_status = frame_it->second;
        mru_.erase(std::next(it).base()); // Convert reverse iterator to forward iterator
        break;
      }
    }
    
    if (frame_id != -1) {
      // Move the frame from MRU to MRU ghost
      mru_ghost_.push_front(frame_status->page_id_);
      
      // Update ghost map
      ghost_map_[frame_status->page_id_] = std::make_shared<FrameStatus>(
          frame_status->page_id_, frame_status->frame_id_, 
          frame_status->evictable_, ArcStatus::MRU_GHOST);
      
      // Remove from alive map
      alive_map_.erase(frame_id);
      
      // Update current size
      curr_size_--;
      
      return frame_id;
    } else {
      // If all entries in MRU are pinned, try MFU
      for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
        frame_id_t fid = *it;
        auto frame_it = alive_map_.find(fid);
        if (frame_it != alive_map_.end() && frame_it->second->evictable_) {
          frame_id = fid;
          frame_status = frame_it->second;
          mfu_.erase(std::next(it).base());
          break;
        }
      }
      
      if (frame_id != -1) {
        // Move the frame from MFU to MFU ghost
        mfu_ghost_.push_front(frame_status->page_id_);
        
        // Update ghost map
        ghost_map_[frame_status->page_id_] = std::make_shared<FrameStatus>(
            frame_status->page_id_, frame_status->frame_id_, 
            frame_status->evictable_, ArcStatus::MFU_GHOST);
        
        // Remove from alive map
        alive_map_.erase(frame_id);
        
        // Update current size
        curr_size_--;
        
        return frame_id;
      }
    }
  } else {
    // Try to evict from MFU first
    for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
      frame_id_t fid = *it;
      auto frame_it = alive_map_.find(fid);
      if (frame_it != alive_map_.end() && frame_it->second->evictable_) {
        frame_id = fid;
        frame_status = frame_it->second;
        mfu_.erase(std::next(it).base());
        break;
      }
    }
    
    if (frame_id != -1) {
      // Move the frame from MFU to MFU ghost
      mfu_ghost_.push_front(frame_status->page_id_);
      
      // Update ghost map
      ghost_map_[frame_status->page_id_] = std::make_shared<FrameStatus>(
          frame_status->page_id_, frame_status->frame_id_, 
          frame_status->evictable_, ArcStatus::MFU_GHOST);
      
      // Remove from alive map
      alive_map_.erase(frame_id);
      
      // Update current size
      curr_size_--;
      
      return frame_id;
    } else {
      // If all entries in MFU are pinned, try MRU
      for (auto it = mru_.rbegin(); it != mru_.rend(); ++it) {
        frame_id_t fid = *it;
        auto frame_it = alive_map_.find(fid);
        if (frame_it != alive_map_.end() && frame_it->second->evictable_) {
          frame_id = fid;
          frame_status = frame_it->second;
          mru_.erase(std::next(it).base());
          break;
        }
      }
      
      if (frame_id != -1) {
        // Move the frame from MRU to MRU ghost
        mru_ghost_.push_front(frame_status->page_id_);
        
        // Update ghost map
        ghost_map_[frame_status->page_id_] = std::make_shared<FrameStatus>(
            frame_status->page_id_, frame_status->frame_id_, 
            frame_status->evictable_, ArcStatus::MRU_GHOST);
        
        // Remove from alive map
        alive_map_.erase(frame_id);
        
        // Update current size
        curr_size_--;
        
        return frame_id;
      }
    }
  }

  // If no evictable frames found, return nullopt
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  
  // Case 1: Access hits mru_ or mfu_
  if (alive_map_.find(frame_id) != alive_map_.end()) {
    auto& frame_status = alive_map_[frame_id];
    if (frame_status->arc_status_ == ArcStatus::MRU) {
      // Move from MRU to MFU
      auto it = std::find(mru_.begin(), mru_.end(), frame_id);
      if (it != mru_.end()) {
        mru_.erase(it);
        mfu_.push_front(frame_id);
        frame_status->arc_status_ = ArcStatus::MFU;
      }
    } else if (frame_status->arc_status_ == ArcStatus::MFU) {
      // Move to front of MFU
      auto it = std::find(mfu_.begin(), mfu_.end(), frame_id);
      if (it != mfu_.end()) {
        mfu_.erase(it);
        mfu_.push_front(frame_id);
      }
    }
  }
  // Case 2: Access hits mru_ghost_
  else if (ghost_map_.find(page_id) != ghost_map_.end() && 
           ghost_map_[page_id]->arc_status_ == ArcStatus::MRU_GHOST) {
    // Update target size
    // If MRU ghost size >= MFU ghost size, increase by 1
    // Otherwise, increase by MFU ghost size / MRU ghost size (rounded down)
    if (mru_ghost_.size() >= mfu_ghost_.size()) {
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + 1);
    } else {
      size_t delta = mfu_ghost_.size() / mru_ghost_.size();
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta);
    }
    
    // Remove from mru_ghost_ and add to mfu_
    auto it = std::find(mru_ghost_.begin(), mru_ghost_.end(), page_id);
    if (it != mru_ghost_.end()) {
      mru_ghost_.erase(it);
    }
    ghost_map_.erase(page_id);
    
    // Add to mfu_ with evictable=true status (frames coming back from ghost are considered evictable)
    mfu_.push_front(frame_id);
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
    // Increase size since this frame is now evictable
    curr_size_++;
  }
  // Case 3: Access hits mfu_ghost_
  else if (ghost_map_.find(page_id) != ghost_map_.end() && 
           ghost_map_[page_id]->arc_status_ == ArcStatus::MFU_GHOST) {
    // Update target size
    // If MFU ghost size >= MRU ghost size, decrease by 1
    // Otherwise, decrease by MRU ghost size / MFU ghost size (rounded down)
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      mru_target_size_ = std::max(static_cast<size_t>(0), mru_target_size_ - 1);
    } else {
      size_t delta = mru_ghost_.size() / mfu_ghost_.size();
      mru_target_size_ = std::max(static_cast<size_t>(0), mru_target_size_ - delta);
    }
    
    // Remove from mfu_ghost_ and add to mfu_
    auto it = std::find(mfu_ghost_.begin(), mfu_ghost_.end(), page_id);
    if (it != mfu_ghost_.end()) {
      mfu_ghost_.erase(it);
    }
    ghost_map_.erase(page_id);
    
    // Add to mfu_ with evictable=true status (frames coming back from ghost are considered evictable)
    mfu_.push_front(frame_id);
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MFU);
    // Increase size since this frame is now evictable
    curr_size_++;
  }
  // Case 4: Access misses all the lists
  else {
    // Check if MRU size + MRU ghost size == replacer size
    if (mru_.size() + mru_ghost_.size() == replacer_size_) {
      // Case 4(a): Kill the last element in MRU ghost list
      if (!mru_ghost_.empty()) {
        page_id_t ghost_page_id = mru_ghost_.back();
        mru_ghost_.pop_back();
        ghost_map_.erase(ghost_page_id);
      }
    } else if (mru_.size() + mru_ghost_.size() < replacer_size_) {
      // Case 4(b): Check if total size = 2 * replacer size
      if (mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
        // Kill the last element in MFU ghost list
        if (!mfu_ghost_.empty()) {
          page_id_t ghost_page_id = mfu_ghost_.back();
          mfu_ghost_.pop_back();
          ghost_map_.erase(ghost_page_id);
        }
      }
      // Otherwise, simply add to MRU
    }
    
    // Add to MRU
    mru_.push_front(frame_id);
    // Default to evictable = false, the user needs to call SetEvictable to make it evictable
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    // Frame ID is invalid, throw an exception
    throw std::invalid_argument("Invalid frame ID");
  }
  
  auto& frame_status = it->second;
  
  // If the evictable status is already the same, do nothing
  if (frame_status->evictable_ == set_evictable) {
    return;
  }
  
  // Update the evictable status
  frame_status->evictable_ = set_evictable;
  
  // Adjust current size based on the change
  if (set_evictable) {
    // Setting to evictable, so increment size
    curr_size_++;
  } else {
    // Setting to non-evictable, so decrement size
    curr_size_--;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    // Frame not found, return directly
    return;
  }
  
  auto& frame_status = it->second;
  
  if (!frame_status->evictable_) {
    // Frame is not evictable, throw an exception
    throw std::invalid_argument("Frame is not evictable");
  }
  
  // Remove frame from its current list
  switch (frame_status->arc_status_) {
    case ArcStatus::MRU:
      {
        auto list_it = std::find(mru_.begin(), mru_.end(), frame_id);
        if (list_it != mru_.end()) {
          mru_.erase(list_it);
        }
      }
      break;
    case ArcStatus::MFU:
      {
        auto list_it = std::find(mfu_.begin(), mfu_.end(), frame_id);
        if (list_it != mfu_.end()) {
          mfu_.erase(list_it);
        }
      }
      break;
    default:
      // Should not happen for frames in alive_map_
      break;
  }
  
  // Remove from alive map
  alive_map_.erase(it);
  
  // Decrement current size
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
