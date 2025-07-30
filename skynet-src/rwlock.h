#ifndef SKYNET_RWLOCK_H
#define SKYNET_RWLOCK_H

#ifndef USE_PTHREAD_LOCK

#include "atomic.h"

// read-write lock struct
struct rwlock {
	ATOM_INT write;
	ATOM_INT read;
};

static inline void
rwlock_init(struct rwlock *lock) {
	ATOM_INIT(&lock->write, 0);
	ATOM_INIT(&lock->read, 0);
}

// implement read lock.
static inline void
rwlock_rlock(struct rwlock *lock) {
	for (;;) {
        // busy loop until write == 0
		while(ATOM_LOAD(&lock->write)) {}
        // atomic add read lock
		ATOM_FINC(&lock->read);
        // double check write lock
		if (ATOM_LOAD(&lock->write)) {
            // if there are write lock, remove read lock
			ATOM_FDEC(&lock->read);
		} else {
            // success add read lock, return
			break;
		}
	}
}

// implement write lock
static inline void
rwlock_wlock(struct rwlock *lock) {
    // busy loop until set write lock success
	while (!ATOM_CAS(&lock->write,0,1)) {}
    // busy loop until read lock is reset
	while(ATOM_LOAD(&lock->read)) {}
}

// implement reset write lock
static inline void
rwlock_wunlock(struct rwlock *lock) {
    // just clear write lock
	ATOM_STORE(&lock->write, 0);
}

// implement reset read lock
static inline void
rwlock_runlock(struct rwlock *lock) {
    // just decrease read lock
	ATOM_FDEC(&lock->read);
}

#else

#include <pthread.h>

// only for some platform doesn't have __sync_*
// todo: check the result of pthread api

struct rwlock {
	pthread_rwlock_t lock;
};

static inline void
rwlock_init(struct rwlock *lock) {
	pthread_rwlock_init(&lock->lock, NULL);
}

static inline void
rwlock_rlock(struct rwlock *lock) {
	 pthread_rwlock_rdlock(&lock->lock);
}

static inline void
rwlock_wlock(struct rwlock *lock) {
	 pthread_rwlock_wrlock(&lock->lock);
}

static inline void
rwlock_wunlock(struct rwlock *lock) {
	pthread_rwlock_unlock(&lock->lock);
}

static inline void
rwlock_runlock(struct rwlock *lock) {
	pthread_rwlock_unlock(&lock->lock);
}

#endif

#endif
