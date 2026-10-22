#include "server.hpp"
#include <algorithm>
#include <cassert>
#include <cstddef>

bool Budget_Bytes::try_reserve(std::size_t bytes) noexcept {
    if (used_bytes >= limit_bytes) {
        return false;
    }
    if (bytes > limit_bytes - used_bytes) {
        return false;
    }
    used_bytes += bytes;
    peak_bytes = std::max(peak_bytes, used_bytes);
    return true;
}

void Budget_Bytes::release(std::size_t bytes) noexcept {
    assert(bytes <= used_bytes);
    used_bytes -= bytes;
}

bool try_reserve_task(TaskQuota& quota, Budget_Bytes& budget, TaskReservation& out) noexcept {
    if (out.active || !quota.try_acquire()) {
        return false;
    }
    if (!budget.try_reserve(response_bytes)) {
        quota.release();
        return false;
    }
    out.active = true;
    out.response_bytes = response_bytes;
    return true;
}

void rollback_task(TaskQuota& quota, Budget_Bytes& budget, TaskReservation& reservation) noexcept {
    if (!reservation.active) {
        return;
    }
    quota.release();
    budget.release(reservation.response_bytes);
    reservation.response_bytes = 0;
    reservation.active = false;
}