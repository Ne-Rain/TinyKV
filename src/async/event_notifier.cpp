#include "async/event_notifier.hpp"
#include <cerrno>
#include <cstdint>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <system_error>
#include <unistd.h>

EventNotifier::EventNotifier() {
    fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (fd_ == -1) {
        throw std::system_error(errno, std::generic_category(), "eventfd");
    }
}
EventNotifier::~EventNotifier() {
    if (fd_ != -1) {
        close(fd_);
    }
}

int EventNotifier::fd() const noexcept { return fd_; }

bool EventNotifier::notify() const {
    while (true) {
        std::uint64_t one = 1;
        ssize_t n = write(fd_, &one, sizeof(one));
        if (n == 8) { // sizeof(uint64_t)
            return true;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        } else if (n == -1 && errno == EAGAIN) {
            return true;
        } else {
            return false;
        }
    }
}
bool EventNotifier::drain() const {
    while (true) {
        std::uint64_t val = 0;
        ssize_t n = read(fd_, &val, sizeof(val));
        if (n == 8) {
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        } else if (n == -1 && errno == EAGAIN) {
            return true;
        } else {
            return false;
        }
    }
}