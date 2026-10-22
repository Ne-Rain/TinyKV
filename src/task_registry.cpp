#include "task_registry.hpp"
#include "server.hpp"

bool register_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence,
                   TaskQuota& quota, Budget_Bytes& budget) {
    if (tasks.find(connection_id) != tasks.end()) {
        return false;
    }
    tasks.emplace(connection_id, InFlightTask{});
    tasks[connection_id].sequence = sequence;
    if (!try_reserve_task(quota, budget, tasks[connection_id].reservation)) {
        tasks.erase(connection_id);
        return false;
    }
    return true;
}

bool release_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence, TaskQuota& quota,
                  Budget_Bytes& budget) {
    auto task = tasks.find(connection_id);
    if (task == tasks.end()) {
        return false;
    }
    if (task->second.sequence != sequence) {
        return false;
    }
    rollback_task(quota, budget, task->second.reservation);
    tasks.erase(connection_id);
    return true;
}