#pragma once

#include "async/async_types.hpp"
#include "async/completion_queue.hpp"
#include "async/event_notifier.hpp"
#include "core/kv_store.hpp"

void run_request_task(RequestTask task, KVStore& store, CompletionQueue& completed,
                      const EventNotifier& notifier);