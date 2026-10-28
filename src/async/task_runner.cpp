#include "async/task_runner.hpp"
#include "core/command_executor.hpp"
#include "async/completion_queue.hpp"
#include "async/event_notifier.hpp"

#include <cstdio>
#include <exception>
#include <new>
#include <utility>

// 执行请求任务
void run_request_task(RequestTask task, KVStore& store, CompletionQueue& completed,
                      const EventNotifier& notifier) {
    try {
        Completion completion;
        completion.connection_id = task.connection_id;
        completion.sequence = task.sequence;
        // 执行阶段：可恢复异常转换为错误响应。
        try {
            completion.response = handle_payload(store, task.payload);
        } catch (const std::bad_alloc&) {
            // 内存耗尽不再尝试分配错误响应，交给外层处理。
            throw;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "request execution failed, id=%llu, sequence=%llu: %s\n",
                         static_cast<unsigned long long>(task.connection_id),
                         static_cast<unsigned long long>(task.sequence), e.what());
            completion.response = "ERR INTERNAL";
        } catch (...) {
            std::fprintf(stderr,
                         "request execution failed, id=%llu, sequence=%llu: "
                         "unknown exception\n",
                         static_cast<unsigned long long>(task.connection_id),
                         static_cast<unsigned long long>(task.sequence));
            completion.response = "ERR INTERNAL";
        }
        std::string{}.swap(task.payload); // 释放请求正文
        // 发布阶段：必须先入队，再通知。
        completed.push(std::move(completion));
        if (!notifier.notify()) {
            std::fprintf(stderr, "completion notification failed, id=%llu, sequence=%llu\n",
                         static_cast<unsigned long long>(task.connection_id),
                         static_cast<unsigned long long>(task.sequence));
            std::terminate();
        }
    } catch (const std::exception& e) {
        // 包括内存耗尽、构造错误响应失败、完成队列入队失败等。
        std::fprintf(stderr, "fatal request task failure, id=%llu, sequence=%llu: %s\n",
                     static_cast<unsigned long long>(task.connection_id),
                     static_cast<unsigned long long>(task.sequence), e.what());
        std::terminate();
    } catch (...) {
        std::fprintf(stderr,
                     "fatal request task failure, id=%llu, sequence=%llu: "
                     "unknown exception\n",
                     static_cast<unsigned long long>(task.connection_id),
                     static_cast<unsigned long long>(task.sequence));
        std::terminate();
    }
}