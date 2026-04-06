#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace flexql {

class ThreadPool {
 public:
  ThreadPool();
  explicit ThreadPool(size_t thread_count);
  ~ThreadPool();

  void submit(std::function<void()> task);
  void waitIdle();
  void shutdown();
  size_t size() const;

 private:
  void workerLoop();

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  mutable std::mutex mtx_;
  std::condition_variable cv_;
  std::condition_variable cv_idle_;
  bool stopping_;
  size_t active_workers_;
};

}  // namespace flexql

