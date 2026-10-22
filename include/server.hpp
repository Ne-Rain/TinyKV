#pragma once
#include "task_quota.hpp"
#include <cstddef>

// 最大响应字节（帧头+"VALUE "+4096最大空间）
constexpr std::size_t response_bytes = 4 + 6 + 4096;

// 响应预留记录
struct TaskReservation {
    std::size_t response_bytes{0};
    bool active{false};
};
struct Budget_Bytes {
    explicit Budget_Bytes(std::size_t limit = 64 * 1024 * 1024) : limit_bytes(limit) {}

    bool try_reserve(std::size_t bytes) noexcept;
    void release(std::size_t bytes) noexcept;

    const std::size_t limit_bytes; // 全局总上限
    std::size_t used_bytes = 0; // 当前已经占用的字节(包括已经承诺分配给任务，还未实际生成的数据额度)
    std::size_t peak_bytes = 0; // 已占用字节历史峰值
};

// 尝试分配任务资源(分配任务名额以及空间)
bool try_reserve_task(TaskQuota& quota, Budget_Bytes& budget, TaskReservation& out) noexcept;
// 回滚任务资源
void rollback_task(TaskQuota& quota, Budget_Bytes& budget, TaskReservation& reservation) noexcept;
