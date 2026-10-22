#pragma once

#include "async_types.hpp"
#include <deque>
#include <mutex>

class CompletionQueue{
public:
    void push(Completion completion);

    std::deque<Completion> task_all();
private:
    std::mutex mtx_;
    std::deque<Completion> queue_;
};