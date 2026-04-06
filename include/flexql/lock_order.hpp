#pragma once

#include <cassert>

namespace flexql {

enum class LockLevel : int {
  TABLE_MAP = 1,
  TABLE_RW = 2,
  INSERT_BUFFER = 3,
  BUFFER_POOL = 4,
  BUFFER_FRAME = 5,
  WAL = 6,
};

class DebugLockLevelGuard {
 public:
  explicit DebugLockLevelGuard(LockLevel level) : prev_(0), active_(false) {
#ifndef NDEBUG
    const int lv = static_cast<int>(level);
    assert(lv >= currentLevel());
    prev_ = currentLevel();
    currentLevel() = lv;
    active_ = true;
#endif
  }

  ~DebugLockLevelGuard() {
#ifndef NDEBUG
    if (active_) {
      currentLevel() = prev_;
    }
#endif
  }

 private:
  static int& currentLevel() {
    static thread_local int level = 0;
    return level;
  }

  int prev_;
  bool active_;
};

}  // namespace flexql

