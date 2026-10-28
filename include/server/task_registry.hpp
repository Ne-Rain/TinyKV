#pragma once
#include "server/server.hpp"
#include "server/task_quota.hpp"
#include <cstdint>
#include <unordered_map>

// 在途任务记录
struct InFlightTask {
    std::uint64_t sequence{0};
    TaskReservation reservation;
    std::size_t request_bytes{0};  // 从连接队列转交工作任务、尚未结算的请求正文预算
};

using InFlightTasks = std::unordered_map<std::uint64_t, InFlightTask>;

bool register_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence,
                   TaskQuota& quota, Budget_Bytes& budget);

bool release_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence, TaskQuota& quota,
                  Budget_Bytes& budget);
// 任务完成释放未使用资源
bool finish_task(InFlightTasks& tasks, std::uint64_t connection_id, std::uint64_t sequence,
                 std::size_t frame_bytes, TaskQuota& quota, Budget_Bytes& budget);