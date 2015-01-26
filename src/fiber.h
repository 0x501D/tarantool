#ifndef TARANTOOL_FIBER_H_INCLUDED
#define TARANTOOL_FIBER_H_INCLUDED
/*
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "trivia/config.h"

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
#include "tt_pthread.h"
#include "third_party/tarantool_ev.h"
#include "coro.h"
#include "trivia/util.h"
#include "third_party/queue.h"
#include "small/mempool.h"
#include "small/region.h"

#if defined(__cplusplus)
#include "exception.h"
#endif /* defined(__cplusplus) */
#include "salad/rlist.h"

enum { FIBER_NAME_MAX = REGION_NAME_MAX };

enum {
	/**
	 * It's safe to resume (wakeup) this fiber
	 * with a spurious wakeup if it is suspended,
	 * e.g. to force it to check that it's been
	 * cancelled.
	 */
	FIBER_IS_CANCELLABLE = 1 << 0,
	/**
	 * Indicates that a fiber has been requested to end
	 * prematurely.
	 */
	FIBER_IS_CANCELLED      = 1 << 1,
	FIBER_DEFAULT_FLAGS = FIBER_IS_CANCELLABLE
};

/**
 * This is thrown by fiber_* API calls when the fiber is
 * cancelled.
 */
#if defined(__cplusplus)
class FiberCancelException: public Exception {
public:
	FiberCancelException(const char *file, unsigned line)
		: Exception(file, line) {
		/* Nothing */
	}

	virtual void log() const {
		say_debug("FiberCancelException");
	}
};
#endif /* defined(__cplusplus) */

/**
 * \brief Pre-defined key for fiber local storage
 */
enum fiber_key {
	/** box.session */
	FIBER_KEY_SESSION = 0,
	/** Lua fiber.storage */
	FIBER_KEY_LUA_STORAGE = 1,
	/** transaction */
	FIBER_KEY_TXN = 2,
	/** User global privilege and authentication token */
	FIBER_KEY_USER = 3,
	FIBER_KEY_MAX = 4
};

struct fiber {
#ifdef ENABLE_BACKTRACE
	void *last_stack_frame;
#endif
	int csw;
	struct tarantool_coro coro;
	/* A garbage-collected memory pool. */
	struct region gc;
	/** Fiber id. */
	uint32_t fid;

	struct rlist link;
	struct rlist state;

	/** Triggers invoked before this fiber yields. Must not throw. */
	struct rlist on_yield;
	/** Triggers invoked before this fiber stops.  Must not throw. */
	struct rlist on_stop;
	/**
	 * The list of fibers awaiting for this fiber's timely
	 * (or untimely) death.
	 */
	struct rlist wake;

	/* This struct is considered as non-POD when compiling by g++.
	 * You can safetly ignore all offset_of-related warnings.
	 * See http://gcc.gnu.org/bugzilla/show_bug.cgi?id=31488
	 */
	void (*f) (va_list);
	va_list f_data;
	uint32_t flags;
	void *fls[FIBER_KEY_MAX]; /* fiber local storage */
};

enum { FIBER_CALL_STACK = 16 };
class Exception;

/**
 * @brief An independent execution unit that can be managed by a separate OS
 * thread. Each cord consists of fibers to implement cooperative multitasking
 * model.
 */
struct cord {
	/** The fiber that is currently being executed. */
	struct fiber *fiber;
	/** The "main" fiber of this cord, the scheduler. */
	struct fiber sched;
	struct ev_loop *loop;
	/** Call stack - in case one fiber decides to call
	 * another with fiber_call(). This is not used
	 * currently, all fibers are called by the sched
         */
	struct fiber *stack[FIBER_CALL_STACK];
	/** Stack pointer in fiber call stack. */
	struct fiber **sp;
	/**
	 * Every new fiber gets a new monotonic id. Ids 1-100 are
	 * reserved.
         */
	uint32_t max_fid;
	pthread_t id;
	/** A helper hash to map id -> fiber. */
	struct mh_i32ptr_t *fiber_registry;
	/** All fibers */
	struct rlist fibers;
	/** A cache of dead fibers for reuse */
	struct rlist zombie_fibers;
	/** Fibers, ready for execution */
	struct rlist ready_fibers;
	/** A watcher to have a single async event for all ready fibers.
	 * This technique is necessary to be able to suspend
	 * a single fiber on a few watchers (for example,
	 * a timeout and an event from network, whichever comes
	 * first).
	 * */
	ev_async wakeup_event;
	/** A memory cache for (struct fiber) */
	struct mempool fiber_pool;
	/** A runtime slab cache for general use in this cord. */
	struct slab_cache slabc;
	char name[FIBER_NAME_MAX];
	/** Last thrown exception */
	class Exception *exception;
	size_t exception_size;
};

extern __thread struct cord *cord_ptr;

#define cord() cord_ptr
#define fiber() cord()->fiber
#define loop() (cord()->loop)

/**
 * Start a cord with the given thread function.
 * The return value of the function can be collected
 * with cord_join(). If the function terminates with
 * an exception, the return value is NULL, and cord_join()
 * moves the exception from the terminated cord to
 * the caller of cord_join().
 */
int
cord_start(struct cord *cord, const char *name,
	   void *(*f)(void *), void *arg);

/**
 * Wait for \a cord to terminate. If \a cord has already
 * terminated, then returns immediately.
 *
 * @post If the subject cord terminated with an exception,
 * preserves the exception in the caller's cord.
 *
 * @param cord cord
 * @retval  0  pthread_join succeeded.
 *             If the thread function terminated with an
 *             exception, the exception is raised in the
 *             caller cord.
 * @retval -1   pthread_join failed.
 */
int
cord_join(struct cord *cord);

static inline void
cord_set_name(const char *name)
{
	snprintf(cord()->name, FIBER_NAME_MAX, "%s", name);
}

void
cord_destroy(struct cord *cord);

/** True if this cord represents the process main thread. */
bool
cord_is_main();

void fiber_init(void);
void fiber_free(void);
typedef void(*fiber_func)(va_list);

struct fiber *
fiber_new(const char *name, fiber_func f);

void
fiber_set_name(struct fiber *fiber, const char *name);

static inline const char *
fiber_name(struct fiber *f)
{
	return region_name(&f->gc);
}

bool
fiber_checkstack();

void
fiber_yield(void);

/**
 * @brief yield & check for timeout
 * @return true if timeout exceeded
 */
bool
fiber_yield_timeout(ev_tstamp delay);

void
fiber_destroy_all();

void
fiber_gc(void);

void
fiber_call(struct fiber *callee, ...);

void
fiber_wakeup(struct fiber *f);

struct fiber *
fiber_find(uint32_t fid);

/**
 * Cancel a fiber. A cancelled fiber will have
 * FiberCancelException raised in it.
 *
 * Waits for the fiber to end.
 * A fiber without flag FIBER_IS_CANCELLABLE can not be
 * spuriously woken up.
 */
void
fiber_cancel(struct fiber *f);

/**
 * Check if the current fiber has been cancelled.  Raises
 * tnt_FiberCancelException
 */
void
fiber_testcancel(void);

/**
 * Make it possible or not possible to wakeup the current
 * fiber immediately when it's cancelled.
 *
 * @return previous state.
 */
bool
fiber_setcancellable(bool yesno);

void
fiber_sleep(ev_tstamp s);

void
fiber_schedule(ev_loop * /* loop */, ev_watcher *watcher, int revents);

/**
 * \brief Associate \a value with \a key in fiber local storage
 * \param fiber fiber
 * \param key pre-defined key
 * \param value value to set
 */
inline void
fiber_set_key(struct fiber *fiber, enum fiber_key key, void *value)
{
	assert(key < FIBER_KEY_MAX);
	fiber->fls[key] = value;
}

bool
fiber_is_cancelled();

/**
 * \brief Retrieve value by \a key from fiber local storage
 * \param fiber fiber
 * \param key pre-defined key
 * \return value from from fiber local storage
 */
inline void *
fiber_get_key(struct fiber *fiber, enum fiber_key key)
{
	assert(key < FIBER_KEY_MAX);
	return fiber->fls[key];
}

/**
 * Finalizer callback
 * \sa fiber_key_on_gc()
 */
typedef void (*fiber_key_gc_cb)(enum fiber_key key, void *arg);
typedef int (*fiber_stat_cb)(struct fiber *f, void *ctx);

int
fiber_stat(fiber_stat_cb cb, void *cb_ctx);

#endif /* TARANTOOL_FIBER_H_INCLUDED */
