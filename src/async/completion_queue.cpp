#include "async/completion_queue.hpp"
#include "async/async_types.hpp"
#include <deque>
#include <mutex>
#include <utility>

void CompletionQueue::push(Completion completion) {
    std::lock_guard<std::mutex> lock(mtx_);
    queue_.push_back(std::move(completion));
}

// 取出一批完成结果
std::deque<Completion> CompletionQueue::take_all() {
    std::deque<Completion> batch;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        batch.swap(queue_); // 把共享队列清空方便后续线程工作，结果返回处理
    }
    return batch;
}