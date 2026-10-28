#include "server/task_quota.hpp"
#include <cassert>
#include <stdexcept>

TaskQuota::TaskQuota(std::size_t limit) {
    if (limit == 0) {
        throw std::invalid_argument("limit is not allowed set 0");
    }
    limit_ = limit;
}

bool TaskQuota::try_acquire() noexcept {
    if (used_ == limit_) {
        return false;
    }
    ++used_;
    return true;
}
void TaskQuota::release() noexcept {
    assert(used_ > 0);
    --used_;
}

std::size_t TaskQuota::used() const noexcept { return used_; }
std::size_t TaskQuota::limit() const noexcept { return limit_; }