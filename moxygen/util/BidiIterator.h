/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace moxygen {

/**
 * BidiIterator provides a unified interface for forward or reverse iteration
 * over a container based on a boolean flag.
 *
 * @tparam Container The container type to iterate over
 */
template <typename Container>
class BidiIterator {
 public:
  using ForwardIterator = typename Container::iterator;
  using ReverseIterator = typename Container::reverse_iterator;

  using value_type = typename Container::value_type;
  using reference = typename Container::reference;
  using pointer = typename Container::pointer;

  /**
   * Default constructor (creates invalid iterator)
   */
  BidiIterator() : forward_(true), container_(nullptr) {}

  /**
   * Constructor
   * @param container Reference to the container to iterate over
   * @param forward If true, uses forward iteration; if false, uses reverse
   * iteration
   */
  BidiIterator(Container& container, bool forward)
      : forward_(forward), container_(&container) {
    if (forward_) {
      forward_it_ = container_->begin();
    } else {
      reverse_it_ = container_->rbegin();
    }
  }

  /**
   * Dereference operator
   */
  reference operator*() {
    if (forward_) {
      return *forward_it_;
    } else {
      return *reverse_it_;
    }
  }

  /**
   * Pre-increment operator
   */
  BidiIterator& operator++() {
    if (forward_) {
      ++forward_it_;
    } else {
      ++reverse_it_;
    }
    return *this;
  }

  /**
   * Equality comparison operator
   */
  bool operator==(const BidiIterator& other) const {
    if (forward_ != other.forward_) {
      return false;
    }
    if (forward_) {
      return forward_it_ == other.forward_it_;
    } else {
      return reverse_it_ == other.reverse_it_;
    }
  }

  /**
   * Inequality comparison operator
   */
  bool operator!=(const BidiIterator& other) const {
    return !(*this == other);
  }

  /**
   * Get an iterator pointing to the end of iteration
   */
  BidiIterator end() const {
    BidiIterator endIter;
    endIter.forward_ = forward_;
    endIter.container_ = container_;
    if (forward_) {
      endIter.forward_it_ = container_->end();
    } else {
      endIter.reverse_it_ = container_->rend();
    }
    return endIter;
  }

 private:
  bool forward_;
  Container* container_;
  ForwardIterator forward_it_;
  ReverseIterator reverse_it_;
};

} // namespace moxygen
