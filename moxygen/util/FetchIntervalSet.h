#pragma once

namespace moxygen {

template <typename T1, typename T2>
class FetchIntervalSet {
 public:
  // Insert a new interval into the set
  void insert(T1 start, T1 end, T2 value) {
    intervals_.emplace(start, Interval{end, value});
  }

  // Erase an interval from the set
  void erase(T1 start) {
    intervals_.erase(start);
  }

  // Get the data associated with the given index
  T2* getValue(T1 index) {
    for (auto& entry : intervals_) {
      if (index >= entry.first && index < entry.second.end) {
        return &entry.second.value;
      }
    }
    return nullptr;
  }

 private:
  struct Interval {
    T1 end;
    T2 value;
  };

  std::map<T1, Interval> intervals_;
};

} // namespace moxygen
