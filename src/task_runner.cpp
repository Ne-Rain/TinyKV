#include "task_runner.hpp"
#include "async_types.hpp"
#include "command_executor.hpp"

void run_request_task(RequestTask task, KVStore& store, CompletionQueue& completed,
                      const EventNotifier& notifier) {
    Completion completion;
    completion.connection_id = task.connection_id;
    completion.sequence = task.sequence;
    completion.response = handle_payload(store, task.payload);
}