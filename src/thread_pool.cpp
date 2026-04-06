#include "flexql/thread_pool.hpp"

#include <algorithm>

namespace flexql {

namespace {

size_t defaultThreadCount() {
  const unsigned hw = std::thread::hardware_concurrency();
  if (hw == 0) {
    return 4;
  }
  return std::min<size_t>(static_cast<size_t>(hw), 16);
}

}  // namespace

ThreadPool::ThreadPool() : ThreadPool(defaultThreadCount()) {}

ThreadPool::ThreadPool(size_t thread_count) : workers_(), tasks_(), mtx_(), cv_(), cv_idle_(), stopping_(false), active_workers_(0) {
  workers_.reserve(thread_count);
  for (size_t i = 0; i < thread_count; ++i) {
    workers_.emplace_back(&ThreadPool::workerLoop, this);
  }
}

ThreadPool::~ThreadPool() {
  shutdown();
}

void ThreadPool::submit(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (stopping_) {
      return;
    }
    tasks_.push(std::move(task));
  }
  cv_.notify_one();
}

void ThreadPool::waitIdle() {
  std::unique_lock<std::mutex> lock(mtx_);
  cv_idle_.wait(lock, [&]() { return tasks_.empty() && active_workers_ == 0; });
}

void ThreadPool::shutdown() {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (stopping_) {
      return;
    }
    stopping_ = true;
  }
  cv_.notify_all();
  for (auto& w : workers_) {
    if (w.joinable()) {
      w.join();
    }
  }
  workers_.clear();
}

size_t ThreadPool::size() const {
  return workers_.size();
}

void ThreadPool::workerLoop() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mtx_);
      cv_.wait(lock, [&]() { return stopping_ || !tasks_.empty(); });

      if (stopping_ && tasks_.empty()) {
        return;
      }

      task = std::move(tasks_.front());
      tasks_.pop();
      active_workers_ += 1;
    }

    task();

    {
      std::lock_guard<std::mutex> lock(mtx_);
      active_workers_ -= 1;
      if (tasks_.empty() && active_workers_ == 0) {
        cv_idle_.notify_all();
      }
    }
  }
}

}  // namespace flexql

