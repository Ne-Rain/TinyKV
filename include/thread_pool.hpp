#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

enum class TaskStatus { Pending, Running, Finished, Cancelled };

struct TaskState {
    std::atomic<TaskStatus> status{TaskStatus::Pending};
};

class TaskCancelledException : public std::exception {
public:
    const char* what() const noexcept override { return "task cancelled"; }
};

template <typename T>
class TaskHandle {
public:
    explicit TaskHandle(std::future<T>&& future, std::shared_ptr<TaskState> state)
        : future_(std::move(future)), state_(std::move(state)) {}

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    TaskHandle(TaskHandle&&) noexcept = default;
    TaskHandle& operator=(TaskHandle&&) noexcept = default;

    T get() {
        if (is_cancelled())
            throw TaskCancelledException();
        future_.wait();
        if (is_cancelled())
            throw TaskCancelledException();
        return future_.get();
    }

    bool cancel() {
        if (!state_)
            return false;
        TaskStatus expected = TaskStatus::Pending;
        return state_->status.compare_exchange_strong(expected, TaskStatus::Cancelled);
    }

    bool is_cancelled() const { return state_ && state_->status.load() == TaskStatus::Cancelled; }

    bool valid() const { return future_.valid(); }

private:
    std::future<T> future_;
    std::shared_ptr<TaskState> state_;
};

class ThreadPool {
public:
    explicit ThreadPool(std::size_t thread_count);
    ~ThreadPool() noexcept;

    void shutdown();

    // 等待所有任务完成
    void wait_for_tasks();

    // 清空任务队列还未执行任务
    void purge();
    // 线程池暂停领取任务
    void pause();
    // 恢复暂停
    void resume();
    // 使用新的的线程数量重新创建worker
    void reset(std::size_t thread_count);

    // 线程池中任务数量
    std::size_t get_tasks_queued();
    // 正在执行的任务数量
    std::size_t get_tasks_running();
    // 还未完成任务数量
    std::size_t get_tasks_total();

    // 任务提交
    template <typename F, typename... Args>
    auto submit(F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type> {
        // using ResultType = typename std::result_of<F(Args...)>::type;

        // auto bound_task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

        // std::packaged_task<ResultType()> packaged(std::move(bound_task));

        // std::future<ResultType> future = packaged.get_future();

        // auto pkg_ptr = std::make_shared<std::packaged_task<ResultType()>>(std::move(packaged));

        // enqueue([pkg_ptr]() { (*pkg_ptr)(); });

        return submit_priority(0, std::forward<F>(f), std::forward<Args>(args)...);
    }

    template <typename F, typename... Args>
    auto submit_with_handle(F&& f, Args&&... args)
        -> TaskHandle<typename std::result_of<F(Args...)>::type> {
        auto state = std::make_shared<TaskState>();
        using ResultType = typename std::result_of<F(Args...)>::type;
        auto bound_task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        std::packaged_task<ResultType()> packaged(std::move(bound_task));
        std::future<ResultType> future = packaged.get_future();
        auto pkg_ptr = std::make_shared<std::packaged_task<ResultType()>>(std::move(packaged));
        enqueue([pkg_ptr]() { (*pkg_ptr)(); }, 0, state);
        return TaskHandle<ResultType>(std::move(future), state);
    }

    template <typename F, typename... Args>
    auto submit_priority(int priority, F&& f,
                         Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type> {
        using ResultType = typename std::result_of<F(Args...)>::type;

        auto bound_task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

        std::packaged_task<ResultType()> packaged(std::move(bound_task));

        std::future<ResultType> future = packaged.get_future();

        auto pkg_ptr = std::make_shared<std::packaged_task<ResultType()>>(std::move(packaged));

        enqueue([pkg_ptr]() { (*pkg_ptr)(); }, priority);

        return future;
    }

    // 多任务并行
    template <typename Index, typename F>
    void parallel_for(Index first, Index last, F&& f) {
        static_assert(std::is_integral<Index>::value == true, "Index should be integral");
        if (last < first) {
            throw std::invalid_argument("last must not be less than first");
        }
        if (last == first)
            return;

        using FunctionType = typename std::decay<F>::type;
        std::shared_ptr<FunctionType> f_ptr = std::make_shared<FunctionType>(std::forward<F>(f));

        std::size_t elem_cont = last - first;
        std::size_t block_cont;
        {
            std::unique_lock<std::mutex> lock(mtx); // reset会修改threads_size，防止数据竞争
            if (state_ == State::Running)
                block_cont = threads_.size();
            else
                throw std::runtime_error("thread pool is stopped");
        }
        if (elem_cont < block_cont) {
            block_cont = elem_cont;
        }

        std::size_t base = elem_cont / block_cont;
        std::size_t reminder = elem_cont % block_cont;

        std::vector<std::future<void>> futures;
        futures.reserve(block_cont);

        Index block_begin = first;
        std::exception_ptr first_exception;
        try {
            for (std::size_t block = 0; block < block_cont; ++block) {
                std::size_t block_size = base + (block < reminder ? 1 : 0);
                Index block_end = block_begin + static_cast<Index>(block_size);

                futures.push_back(submit([f_ptr, block_begin, block_end]() {
                    for (Index i = block_begin; i < block_end; ++i) {
                        (*f_ptr)(i);
                    }
                }));
                block_begin = block_end;
            }
        } catch (...) {
            first_exception = std::current_exception();
        }

        for (std::size_t i = 0; i < futures.size(); ++i) {
            try {
                futures[i].get();
            } catch (...) {
                if (!first_exception) {
                    first_exception = std::current_exception();
                }
            }
        }
        if (first_exception) {
            std::rethrow_exception(first_exception);
        }
    }

    template <typename Rep, typename Period>
    bool wait_for(const std::chrono::duration<Rep, Period>& duration) {
        std::unique_lock<std::mutex> lock(mtx);
        return finished_cv_.wait_for(lock, duration,
                                     [this]() { return tasks_.empty() && running_tasks_ == 0; });
    }

    template <typename Clock, typename Duration>
    bool wait_until(const std::chrono::time_point<Clock, Duration>& time_point) {
        std::unique_lock<std::mutex> lock(mtx);
        return finished_cv_.wait_until(lock, time_point,
                                       [this]() { return tasks_.empty() && running_tasks_ == 0; });
    }

    static std::size_t get_thread_index();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    static const std::size_t npos;

private:
    void worker(std::size_t i);
    // void enqueue(std::function<void()> task);
    void enqueue(std::function<void()> task, int priority,
                 std::shared_ptr<TaskState> state = nullptr);

    enum class State { Running, Draining, Stopped }; // 运行中、排空中、停止

    std::vector<std::thread> threads_;
    // std::queue<std::function<void()>> tasks_;
    struct Task {
        std::function<void()> tasks_;
        std::shared_ptr<TaskState> state_;
        int priority;
        std::size_t sequence; // 进入队列的顺序
    };
    struct TaskCompare {
        bool operator()(const Task& a, const Task& b) {
            if (a.priority == b.priority)
                return a.sequence > b.sequence;
            return a.priority < b.priority;
        }
    };
    std::priority_queue<Task, std::vector<Task>, TaskCompare> tasks_;

    std::mutex mtx;
    std::mutex shutdown_mtx;
    std::condition_variable cv;
    std::condition_variable finished_cv_;

    State state_ = State::Running;
    bool paused_ = false;
    std::size_t running_tasks_ = 0;
    std::size_t sequence_ = 0;

    static thread_local ThreadPool* current_pool_;
    static thread_local std::size_t current_index_;
};
