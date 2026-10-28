#pragma once

class EventNotifier {
public:
    EventNotifier();
    ~EventNotifier();

    int fd() const noexcept;

    bool notify() const;
    bool drain() const;

    EventNotifier(const EventNotifier&) = delete;
    EventNotifier& operator=(const EventNotifier&) = delete;

private:
    int fd_{-1};
};