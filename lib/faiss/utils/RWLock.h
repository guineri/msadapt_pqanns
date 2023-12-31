//
// Created by gnandrade on 23/03/2021.
//

#ifndef PQNNS_MULTI_STREAM_RWLOCK_H
#define PQNNS_MULTI_STREAM_RWLOCK_H

#include <condition_variable>
#include <iostream>

class RWLock {
public:
    RWLock() : _status(0), _waiting_readers(0), _waiting_writers(0) {}
    RWLock(const RWLock&) = delete;
    RWLock(RWLock&&) = delete;
    RWLock& operator = (const RWLock&) = delete;
    RWLock& operator = (RWLock&&) = delete;

    void rdlock() {
		//std::cout << "[read lock][status=" << _status << "]" << std::endl;
        std::unique_lock<std::mutex> lck(_mtx);
        _waiting_readers += 1;
        _read_cv.wait(lck, [&]() { return _waiting_writers == 0 && _status >= 0; });
        _waiting_readers -= 1;
        _status += 1;
    }

    void wrlock() {
		//std::cout << "[write lock][status=" << _status << "]" << std::endl;
        std::unique_lock<std::mutex> lck(_mtx);
        _waiting_writers += 1;
        _write_cv.wait(lck, [&]() { return _status >= 0; });
        _waiting_writers -= 1;
        _status = -1;
    }

    void unlock() {
        std::unique_lock<std::mutex> lck(_mtx);
        if (_status == -1) {
            _status = 0;
        } else {
            _status -= 1;
        }
        if (_waiting_writers > 0) {
            if (_status == 0) {
                _write_cv.notify_one();
            }
        } else {
            _read_cv.notify_all();
        }
    }

private:
    // -1    : one writer
    // 0     : no reader and no writer
    // n > 0 : n reader
    int32_t _status;
    int32_t _waiting_readers;
    int32_t _waiting_writers;
    std::mutex _mtx;
    std::condition_variable _read_cv;
    std::condition_variable _write_cv;
};

#endif //PQNNS_MULTI_STREAM_RWLOCK_H
