#pragma once

#include "async_types.hpp"
#include "completion_queue.hpp"
#include "event_notifier.hpp"
#include "kv_store.hpp"

void run_request_task(RequestTask task, KVStore& store, CompletionQueue& completed,
                      const EventNotifier& notifier);