#pragma once

#include <cstddef>

class TaskQuota {
public:
    explicit TaskQuota(std::size_t limit);

    bool try_acquire() noexcept;
    void release() noexcept;

    std::size_t used() const noexcept;
    std::size_t limit() const noexcept;

    TaskQuota(const TaskQuota&) = delete;
    TaskQuota& operator=(const TaskQuota&) = delete;

private:
    std::size_t limit_;
    std::size_t used_{0};
};