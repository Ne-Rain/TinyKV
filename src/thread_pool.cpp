#include "thread_pool.hpp"
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

const std::size_t ThreadPool::npos = static_cast<std::size_t>(-1);

thread_local ThreadPool* ThreadPool::current_pool_ = nullptr;
thread_local std::size_t ThreadPool::current_index_ = ThreadPool::npos;

ThreadPool::ThreadPool(std::size_t thread_count) {
    if (thread_count == 0) {
        throw std::invalid_argument("thread_count can't be set 0");
    }
    threads_.reserve(thread_count);

    // 防止线程未全部创建完成抛出异常
    try {
        for (std::size_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back(&ThreadPool::worker, this, i);
        }
    } catch (...) {
        shutdown();
        throw;
    }
}

ThreadPool::~ThreadPool() noexcept { shutdown(); }

void ThreadPool::shutdown() {
    std::unique_lock<std::mutex> shutdown_lock(shutdown_mtx);
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (state_ == State::Stopped)
            return;
        state_ = State::Draining;
    }
    cv.notify_all();
    for (auto& t : threads_) {
        if (t.joinable())
            t.join();
    }
    {
        std::unique_lock<std::mutex> lock(mtx);
        state_ = State::Stopped;
    }
}

void ThreadPool::enqueue(
    std::function<void()> task,
    int priority,
    std::shared_ptr<TaskState> state) {

    if (!task) {
        throw std::invalid_argument(
            "task must not be empty");
    }

    Task t{};
    bool should_notify = false;

    {
        std::unique_lock<std::mutex> lock(mtx);

        if (state_ == State::Draining) {
            throw std::runtime_error(
                "cannot enqueue task into a stopping thread pool");
        } else if (state_ == State::Stopped) {
            throw std::runtime_error(
                "cannot enqueue task into a stopped thread pool");
        }

        t.tasks_ = std::move(task);
        t.state_ = std::move(state);
        t.priority = priority;
        t.sequence = sequence_++;

        tasks_.push(std::move(t));

        should_notify = !paused_;
    }

    if (should_notify) {  // pause状态不需要通知worker
        cv.notify_one();
    }
}

void ThreadPool::wait_for_tasks() {
    std::unique_lock<std::mutex> lock(mtx);
    finished_cv_.wait(lock, [this]() { return tasks_.empty() && running_tasks_ == 0; });
}

void ThreadPool::purge() {
    bool is_notify = false;
    {
        std::unique_lock<std::mutex> lock(mtx);
        while (!tasks_.empty()) {
            auto state = tasks_.top().state_;
            if (state) {
                TaskStatus expected = TaskStatus::Pending;
                state->status.compare_exchange_strong(expected, TaskStatus::Cancelled);
            }
            tasks_.pop();
        }
        is_notify = (running_tasks_ == 0);
    }
    if (is_notify)
        finished_cv_.notify_all();
}

void ThreadPool::pause() {
    std::unique_lock<std::mutex> lock(mtx);
    if (state_ != State::Running)
        return;
    paused_ = true;
}

void ThreadPool::resume() {
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (state_ != State::Running)
            return;
        paused_ = false;
    }
    cv.notify_all();
}

void ThreadPool::reset(std::size_t thread_count) {
    if (thread_count == 0) {
        throw std::invalid_argument("thread_count can't be set 0");
    }

    std::unique_lock<std::mutex> shutdown_lock(shutdown_mtx);
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (state_ == State::Stopped)
            return;
        state_ = State::Draining;
    }
    cv.notify_all();
    for (auto& t : threads_) {
        if (t.joinable())
            t.join();
    }

    {
        std::unique_lock<std::mutex> lock(mtx);
        threads_.clear();
        state_ = State::Running;
        paused_ = false;
        try {
            threads_.reserve(thread_count);
            for (std::size_t i = 0; i < thread_count; ++i) {
                threads_.emplace_back(&ThreadPool::worker, this, i);
            }
        } catch (...) {
            state_ = State::Stopped;
            paused_ = false;
            lock.unlock();
            cv.notify_all();
            for (auto& t : threads_) {
                if (t.joinable())
                    t.join();
            }
            threads_.clear();
            throw;
        }
    }
}

std::size_t ThreadPool::get_tasks_queued() {
    std::unique_lock<std::mutex> lock(mtx);
    return tasks_.size();
}

std::size_t ThreadPool::get_tasks_running() {
    std::unique_lock<std::mutex> lock(mtx);
    return running_tasks_;
}

std::size_t ThreadPool::get_tasks_total() {
    std::unique_lock<std::mutex> lock(mtx);
    return tasks_.size() + running_tasks_;
}

std::size_t ThreadPool::get_thread_index() {
    if (current_pool_ == nullptr)
        return npos;
    return current_index_;
}

void ThreadPool::worker(std::size_t i) {
    current_pool_ = this;
    current_index_ = i;
    while (true) {
        std::function<void()> task;
        std::shared_ptr<TaskState> task_state;
        bool should_run = true;
        {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock,
                    [this]() { return state_ != State::Running || (!tasks_.empty() && !paused_); });
            if (state_ != State::Running && tasks_.empty())
                break;

            task = std::move(tasks_.top().tasks_);
            task_state = tasks_.top().state_;
            tasks_.pop();

            if (task_state) {
                TaskStatus expected = TaskStatus::Pending;
                should_run =
                    task_state->status.compare_exchange_strong(expected, TaskStatus::Running);
            }
            if (should_run)
                ++running_tasks_;
        }
        if (should_run) {
            try {
                task();
            } catch (...) {
                // 处理非packaged包装异常
            }
        } else {
            std::unique_lock<std::mutex> lock(mtx);
            if (tasks_.empty() && running_tasks_ == 0)
                finished_cv_.notify_all();
            continue;
        }

        bool pool_is_idle = false;
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (task_state) {
                task_state->status.store(TaskStatus::Finished);
            }
            --running_tasks_;
            pool_is_idle = (running_tasks_ == 0 && tasks_.empty());
        }
        if (pool_is_idle)
            finished_cv_.notify_all();
    }
    current_index_ = npos;
    current_pool_ = nullptr;
}