#pragma once
#include "server.hpp"
#include "task_quota.hpp"
#include <cstdint>
#include <unordered_map>

// 在途任务记录
struct InFlightTask {
    std::uint64_t sequence{0};
    TaskReservation reservation;
};

using InFlightTasks = std::unordered_map<std::uint64_t, InFlightTask>;

bool register_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence,
                   TaskQuota& quota, Budget_Bytes& budget);

bool release_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence, TaskQuota& quota,
                  Budget_Bytes& budget);
// 任务完成释放未使用资源
bool finish_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence,
                 std::size_t frame_bytes, TaskQuota& quota, Budget_Bytes& budget);