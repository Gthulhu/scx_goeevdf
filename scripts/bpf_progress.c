#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef uint32_t u32;
typedef int32_t s32;
typedef uint64_t u64;
typedef int64_t s64;
#include "bpf/intf.h"

#define READ_ONCE(x) (*(volatile typeof(x) *)&(x))
#define MOCK_READ1(p, a) ((p)->a)
#define MOCK_READ4(p, a, b, c, d) ((p)->a->b->c->d)
#define MOCK_READ_SELECT(_1, _2, _3, _4, NAME, ...) NAME
#define BPF_CORE_READ(p, ...) MOCK_READ_SELECT(__VA_ARGS__, MOCK_READ4, unused, unused, MOCK_READ1)(p, __VA_ARGS__)
#define bpf_core_field_exists(field) 1
#define BPF_STRUCT_OPS(name, ...) name(__VA_ARGS__)
#define SCHED_DSQ_BASE 0x1000ULL
#define SCX_TASK_QUEUED 1
#define SCX_ENQ_HEAD 16ULL
#define SCX_ENQ_WAKEUP 1ULL
#define SCX_ENQ_PREEMPT (1ULL << 32)
#define SCX_KICK_IDLE 1
#define SCX_KICK_PREEMPT 2
#define SCX_DSQ_LOCAL_ON (1ULL << 63)
#define SCX_DSQ_LOCAL (SCX_DSQ_LOCAL_ON | 1)
#define SCX_SLICE_DFL 20000000ULL
#define BPF_RB_NO_WAKEUP 1

struct kernfs_node { u64 id; };
struct cgroup { struct kernfs_node *kn; };
struct css_set { struct cgroup *dfl_cgrp; };
struct task_struct {
	s32 pid, tgid, cpu;
	u32 policy, nr_cpus_allowed;
	struct { u64 flags, slice, core_sched_at, weight; } scx;
	struct { u64 sum_exec_runtime, slice; bool custom_slice; } se;
	struct css_set *cgroups;
	void *cpus_ptr;
};
struct task_ctx {
	u64 enq_seq;
	u32 slot, wake_flags;
	s32 waker_pid, waker_cpu;
	bool selected, waiting, hrtick;
};
struct bpf_timer { int unused; };
struct hrtick { struct bpf_timer timer; u64 run_seq; s32 cpu; };
struct heartbeat { struct bpf_timer timer; };
struct scx_cpu_release_args {};
struct scx_cpu_acquire_args {};

static int cpu_tbl, ctl_tbl, task_tbl, task_ctxs, ev_rings, usersched_tids;
static struct cpu_slot cpu_state;
static struct cpu_ctl cpu_control;
static struct task_pub publication;
static struct task_ctx context;
static struct task_struct task;
static u32 usersched_tgid = 99, nr_shards = 2, usersched_cpus[MAX_SHARDS] = {0, 1};
static u64 needed_s[MAX_SHARDS], heartbeat_ticks;
static u64 nr_queued, nr_queued_s[MAX_SHARDS], nr_ev_drops, nr_congested;
static u64 usersched_slice = 1000000;
static u64 nr_bounces, nr_stale_cmds, mock_now = 100;
static int kicks, inserts, drains, keep_events, releases, timer_starts;
static u64 insert_flags, insert_dsq, insert_slice;
static bool consume_usersched, allow_cpu1;
static s32 local_queued;
static bool ring_full, assert_enqueue_order;
static struct ev ring_event, last_event;
static u64 expected_queued;

static void *bpf_map_lookup_elem(void *map, const void *raw_key)
{
	const u32 *key = raw_key;
	if (map == &cpu_tbl)
		return *key < MAX_CPUS ? &cpu_state : NULL;
	if (map == &ctl_tbl)
		return *key < MAX_CPUS ? &cpu_control : NULL;
	if (map == &task_tbl)
		return *key < MAX_TASKS ? &publication : NULL;
	if (map == &ev_rings)
		return *key < nr_shards ? &ev_rings : NULL;
	return NULL;
}
static void scx_bpf_kick_cpu(s32 cpu, u64 flags) { assert(cpu >= 0); assert(flags); kicks++; }
static int bpf_timer_start(struct bpf_timer *timer, u64 when, u64 flags)
{ (void)timer; (void)flags; assert(when); timer_starts++; return 0; }
static struct task_struct *bpf_task_from_pid(s32 pid) { return pid == task.pid ? &task : NULL; }
static struct task_ctx *bpf_task_storage_get(void *map, struct task_struct *p, void *v, u64 flags)
{ (void)map; (void)p; (void)v; (void)flags; return &context; }
static bool bpf_cpumask_test_cpu(u32 cpu, void *mask) { (void)mask; return cpu == 0 || allow_cpu1; }
static s32 scx_bpf_task_cpu(struct task_struct *p) { return p->cpu; }
static u32 shard_for_cpu(s32 cpu) { (void)cpu; return 0; }
static void scx_bpf_dsq_insert(struct task_struct *p, u64 dsq, u64 slice, u64 flags)
{ assert(p == &task); inserts++; insert_dsq = dsq; insert_slice = slice; insert_flags = flags; }
static void bpf_task_release(struct task_struct *p) { assert(p == &task); releases++; }
static void drain_shard(u32 shard) { assert(shard == 0); drains++; }
static bool scx_bpf_dsq_move_to_local(u64 dsq) { assert(dsq == SCHED_DSQ_BASE); return consume_usersched; }
static s32 scx_bpf_dsq_nr_queued(u64 dsq) { assert(dsq == SCX_DSQ_LOCAL); return local_queued; }
static u64 bpf_ktime_get_ns(void) { return mock_now; }
static u64 bpf_get_current_pid_tgid(void) { return 73; }
static u32 bpf_get_smp_processor_id(void) { return 1; }
static void *bpf_ringbuf_reserve(void *ring, u64 size, u64 flags)
{ assert(ring == &ev_rings); assert(size == sizeof(ring_event)); (void)flags; return ring_full ? NULL : &ring_event; }
static void bpf_ringbuf_submit(struct ev *event, u64 flags)
{
	(void)flags;
	if (assert_enqueue_order && event->kind == EV_ENQUEUE) {
		assert(nr_queued == expected_queued && nr_queued_s[0] == expected_queued);
		assert(context.waiting);
	}
	last_event = *event;
	if (event->kind == EV_KEEP)
		keep_events++;
}

#include "callbacks.h"

static void test_enqueue(void)
{
	struct kernfs_node kn = {.id = 91};
	struct cgroup cgroup = {.kn = &kn};
	struct css_set css = {.dfl_cgrp = &cgroup};
	task = (struct task_struct){.pid = 42, .tgid = 40, .cpu = 0, .policy = 3,
		.nr_cpus_allowed = 2, .scx = {.weight = 123},
		.se = {.sum_exec_runtime = 456, .slice = 789, .custom_slice = true},
		.cgroups = &css};
	context = (struct task_ctx){.slot = 9};
	ge_select_cpu(&task, 0, 16);
	assert(context.selected && context.waker_pid == 73 && context.waker_cpu == 1);
	assert_enqueue_order = true;
	expected_queued = 1;
	ge_enqueue(&task, SCX_ENQ_WAKEUP);
	assert(kicks == 0); /* Wake preemption belongs to Go, not event publication. */
	assert(last_event.kind == EV_ENQUEUE && last_event.now == mock_now);
	assert(last_event.pid == 42 && last_event.tgid == 40 && last_event.cpu == 0);
	assert(last_event.weight == 123 && last_event.sum_exec == 456 && last_event.policy == 3);
	assert(last_event.nr_cpus_allowed == 2 && last_event.cgid == 91 && last_event.custom_slice == 789);
	assert(last_event.enq_seq == 1 && last_event.slot == 9 && last_event.flags == SCX_ENQ_WAKEUP);
	assert(last_event.wake_flags == 16 && last_event.waker_pid == 73 && last_event.waker_cpu == 1);
	assert(!context.selected && context.waiting);
	expected_queued = 2;
	ge_enqueue(&task, 0);
	assert(kicks == 0);
	assert(last_event.enq_seq == 2 && !last_event.wake_flags && !last_event.waker_pid);
	ring_full = true;
	ge_enqueue(&task, 0);
	assert(nr_ev_drops == 1 && nr_congested == 1 && nr_queued == 2 && nr_queued_s[0] == 2);
	assert(!context.waiting && inserts == 1 && insert_dsq == SCX_DSQ_LOCAL_ON);
	assert(insert_slice == SCX_SLICE_DFL);
	ring_full = false;
	task.tgid = usersched_tgid;
	ge_enqueue(&task, 0);
	assert(inserts == 2 && insert_dsq == SCHED_DSQ_BASE && nr_queued == 2);
	assert_enqueue_order = false;
	kicks = inserts = 0;
	task.cgroups = NULL;
}

int main(void)
{
	test_enqueue();
	struct hrtick tick = {.cpu = 0, .run_seq = 7};
	int key = 0;
	cpu_state.curr_pid = 42;
	cpu_state.run_seq = 7;
	cpu_control.nr_queued = 1;
	hrtick_fire(NULL, &key, &tick);
	assert(kicks == 1);
	tick.run_seq++;
	hrtick_fire(NULL, &key, &tick);
	assert(kicks == 1);
	tick.run_seq = 7;
	cpu_control.nr_queued = 0;
	hrtick_fire(NULL, &key, &tick);
	assert(kicks == 1);

	struct heartbeat heartbeat = {};
	heartbeat_fire(NULL, &key, &heartbeat);
	assert(heartbeat_ticks == 1 && timer_starts == 1 && kicks == 3);
	assert(needed_s[0] == 1 && needed_s[1] == 1);

	task = (struct task_struct){.pid = 42, .tgid = 42, .cpu = 0,
		.scx = {.flags = SCX_TASK_QUEUED}};
	struct kernfs_node kn = {};
	struct cgroup cgroup = {.kn = &kn};
	struct css_set css = {.dfl_cgrp = &cgroup};
	task.cgroups = &css;
	context = (struct task_ctx){.waiting = true, .enq_seq = 5, .slot = 1};
	struct cmd cmd = {.pid = 42, .cpu = 0, .enq_seq = 4, .slice = 1000};
	do_dispatch(&cmd, 0);
	assert(inserts == 0 && nr_stale_cmds == 1 && releases == 1);
	cmd.enq_seq = 5;
	cmd.enq_flags = SCX_ENQ_PREEMPT;
	cmd.expect_curr = 77;
	do_dispatch(&cmd, 0);
	assert(inserts == 1 && !(insert_flags & SCX_ENQ_PREEMPT));
	assert(!context.waiting && insert_slice == 1000);
	context.waiting = true;
	cmd.cpu = 1;
	cmd.expect_curr = 42;
	do_dispatch(&cmd, 0);
	assert(inserts == 2 && insert_dsq == SCX_DSQ_LOCAL_ON && nr_bounces == 1);
	assert(!(insert_flags & SCX_ENQ_PREEMPT));

	cpu_control.keep_slice = 1234;
	ge_dispatch(0, &task);
	assert(drains == 1 && keep_events == 1 && task.scx.slice == 1234);
	cpu_control.nr_queued = 1;
	ge_dispatch(0, &task);
	assert(keep_events == 1);
	cpu_control.nr_queued = 0;
	local_queued = 1;
	ge_dispatch(0, &task);
	assert(keep_events == 1 && task.scx.slice == 1234);
	local_queued = 0;
	consume_usersched = true;
	ge_dispatch(0, &task);
	assert(keep_events == 1);

	u64 v = 0;
	context.slot = 1;
	publication = (struct task_pub){.seq = 1, .pid = 42, .core_vtime = 10};
	assert(!core_vtime(&task, &v));
	publication.seq = 2;
	publication.pid = 43;
	assert(!core_vtime(&task, &v));
	publication.pid = 42;
	assert(core_vtime(&task, &v) && v == 10);
	struct task_struct other = {.pid = 43, .scx = {.core_sched_at = 9}};
	task.scx.core_sched_at = 8;
	assert(ge_core_sched_before(&task, &other));

	mock_now = 100;
	ge_cpu_release(0, NULL);
	assert(cpu_state.released_at == 100);
	mock_now = 150;
	ge_cpu_acquire(0, NULL);
	assert(cpu_state.displaced_ns == 50 && cpu_state.released_at == 0);
	ge_cpu_acquire(0, NULL);
	assert(cpu_state.displaced_ns == 50);
	puts("BPF callback progress tests passed");
	return 0;
}
