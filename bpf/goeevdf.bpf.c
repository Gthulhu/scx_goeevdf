#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "intf.h"
#define BPF_STRUCT_OPS(name, ...) SEC("struct_ops/" #name) BPF_PROG(name, ##__VA_ARGS__)
#define BPF_STRUCT_OPS_SLEEPABLE(name, ...) SEC("struct_ops.s/" #name) BPF_PROG(name, ##__VA_ARGS__)

void scx_bpf_dsq_insert(struct task_struct *, u64, u64, u64) __ksym;
bool scx_bpf_dsq_move_to_local(u64) __ksym;
s32 scx_bpf_create_dsq(u64, s32) __ksym;
void scx_bpf_kick_cpu(s32, u64) __ksym;
s32 scx_bpf_task_cpu(const struct task_struct *) __ksym;
struct task_struct *bpf_task_from_pid(s32) __ksym;
void bpf_task_release(struct task_struct *) __ksym;
bool bpf_cpumask_test_cpu(u32, const struct cpumask *) __ksym;
void scx_bpf_cpuperf_set(s32, u32) __ksym __weak;
u32 scx_bpf_nr_cpu_ids(void) __ksym;

#ifndef CLOCK_MONOTONIC
#define CLOCK_MONOTONIC 1
#endif
#define READ_ONCE(x) (*(volatile typeof(x) *)&(x))

#define SCHED_DSQ_BASE 0x1000ULL
#define MMAP_ARRAY(name, value_t, n) \
	struct { __uint(type, BPF_MAP_TYPE_ARRAY); __uint(map_flags, BPF_F_MMAPABLE); \
		__type(key, u32); __type(value, value_t); __uint(max_entries, n); } name SEC(".maps")

struct ev_ring { __uint(type, BPF_MAP_TYPE_RINGBUF); __uint(max_entries, 4 << 20); };
struct ev_ring events SEC(".maps");
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY_OF_MAPS);
	__uint(max_entries, MAX_SHARDS);
	__type(key, u32);
	__array(values, struct ev_ring);
} ev_rings SEC(".maps") = { .values = { [0] = &events } };
#define CMD_RING(n) struct { __uint(type, BPF_MAP_TYPE_USER_RINGBUF); \
	__uint(max_entries, 4 << 20); } cmds_##n SEC(".maps")
#define EACH_SHARD(X) \
	X(0) X(1) X(2) X(3) X(4) X(5) X(6) X(7) \
	X(8) X(9) X(10) X(11) X(12) X(13) X(14) X(15) \
	X(16) X(17) X(18) X(19) X(20) X(21) X(22) X(23) \
	X(24) X(25) X(26) X(27) X(28) X(29) X(30) X(31)
#define DECL_CMD(n) CMD_RING(n);
EACH_SHARD(DECL_CMD)
#undef DECL_CMD
struct { __uint(type, BPF_MAP_TYPE_HASH); __type(key, u32);
	__type(value, u32); __uint(max_entries, MAX_SHARDS); } usersched_tids SEC(".maps");
MMAP_ARRAY(cpu_tbl, struct cpu_slot, MAX_CPUS);
MMAP_ARRAY(ctl_tbl, struct cpu_ctl, MAX_CPUS);
MMAP_ARRAY(task_tbl, struct task_pub, MAX_TASKS);

struct hrtick {
	struct bpf_timer timer;
	u64 run_seq;
	s32 cpu;
};
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__type(key, u32);
	__type(value, struct hrtick);
	__uint(max_entries, MAX_CPUS);
} hrticks SEC(".maps");

struct task_ctx {
	u64 enq_seq;
	u32 slot, wake_flags;
	s32 waker_pid, waker_cpu;
	bool selected, waiting, hrtick;
};
struct { __uint(type, BPF_MAP_TYPE_TASK_STORAGE); __uint(map_flags, BPF_F_NO_PREALLOC);
	__type(key, int); __type(value, struct task_ctx); } task_ctxs SEC(".maps");

const volatile u32 usersched_tgid;
const volatile u64 usersched_slice = 1000000;
const volatile u32 cpu_shard[MAX_CPUS];
const volatile u32 nr_shards = 1;
volatile u64 nr_queued, nr_scheduled, nr_ev_drops, nr_congested, nr_stale_cmds, nr_bounces;
volatile u64 nr_queued_s[MAX_SHARDS], nr_scheduled_s[MAX_SHARDS], needed_s[MAX_SHARDS];

static __always_inline u32 shard_for_cpu(s32 cpu)
{
	u32 key = (u32)cpu & (MAX_CPUS - 1);
	u32 shard = cpu_shard[key];
	if (cpu < 0 || cpu >= MAX_CPUS)
		return 0;
	return shard < nr_shards && shard < MAX_SHARDS ? shard : 0;
}

static int hrtick_fire(void *map, int *key, struct hrtick *h)
{
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, key);
	struct cpu_ctl *ctl = bpf_map_lookup_elem(&ctl_tbl, key);

	if (s && ctl && s->curr_pid && (u32)s->run_seq == (u32)h->run_seq &&
	    READ_ONCE(ctl->nr_queued))
		scx_bpf_kick_cpu(h->cpu, SCX_KICK_PREEMPT);
	return 0;
}

static __always_inline void hrtick_arm_at(s32 cpu, u64 when, u32 run_seq)
{
	u32 key;
	struct hrtick *h;
	struct cpu_slot *s;

	if (cpu < 0 || cpu >= MAX_CPUS || !when)
		return;
	key = cpu;
	s = bpf_map_lookup_elem(&cpu_tbl, &key);
	h = bpf_map_lookup_elem(&hrticks, &key);
	if (!s || !h || !s->curr_pid || (u32)s->run_seq != run_seq)
		return;
	h->run_seq = s->run_seq;
	h->cpu = cpu;
	bpf_timer_start(&h->timer, when, BPF_F_TIMER_ABS);
}

static __always_inline struct ev *ev_reserve(u32 kind, s32 cpu)
{
	u32 shard = shard_for_cpu(cpu);
	void *ring = bpf_map_lookup_elem(&ev_rings, &shard);
	struct ev *e;
	if (!ring)
		goto drop;
	e = bpf_ringbuf_reserve(ring, sizeof(*e), 0);
	if (!e) {
drop:
		__sync_fetch_and_add(&nr_ev_drops, 1);
		return 0;
	}
	__builtin_memset(e, 0, sizeof(*e));
	e->kind = kind;
	e->now = bpf_ktime_get_ns();
	return e;
}

static __always_inline void ev_task(struct ev *e, struct task_struct *p, struct task_ctx *t)
{
	e->pid = p->pid;
	e->tgid = p->tgid;
	e->cpu = scx_bpf_task_cpu(p);
	e->sum_exec = BPF_CORE_READ(p, se.sum_exec_runtime);
	e->weight = BPF_CORE_READ(p, scx.weight);
	e->policy = BPF_CORE_READ(p, policy);
	e->nr_cpus_allowed = BPF_CORE_READ(p, nr_cpus_allowed);
	if (bpf_core_field_exists(p->cgroups) &&
	    bpf_core_field_exists(((struct css_set *)0)->dfl_cgrp) &&
	    bpf_core_field_exists(((struct cgroup *)0)->kn) &&
	    bpf_core_field_exists(((struct kernfs_node *)0)->id))
		e->cgid = BPF_CORE_READ(p, cgroups, dfl_cgrp, kn, id);
	if (bpf_core_field_exists(p->se.custom_slice) && BPF_CORE_READ(p, se.custom_slice))
		e->custom_slice = BPF_CORE_READ(p, se.slice);
	if (t) {
		e->slot = t->slot;
		e->enq_seq = t->enq_seq;
	}
}

static __always_inline void send_task(struct task_struct *p, u32 kind, u64 flags, u64 rb_flags)
{
	struct task_ctx *t;
	struct ev *e;
	if (p->tgid == usersched_tgid)
		return;
	t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	e = ev_reserve(kind, scx_bpf_task_cpu(p));
	if (!e)
		return;
	ev_task(e, p, t);
	e->flags = flags;
	bpf_ringbuf_submit(e, rb_flags);
}

s32 BPF_STRUCT_OPS(ge_init_task, struct task_struct *p, struct scx_init_task_args *args)
{
	return bpf_task_storage_get(&task_ctxs, p, 0, BPF_LOCAL_STORAGE_GET_F_CREATE) ? 0 : -12;
}

s32 BPF_STRUCT_OPS(ge_select_cpu, struct task_struct *p, s32 prev_cpu, u64 wake_flags)
{
	struct task_ctx *t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (t) {
		t->wake_flags = wake_flags;
		t->waker_pid = (s32)bpf_get_current_pid_tgid();
		t->waker_cpu = bpf_get_smp_processor_id();
		t->selected = true;
	}
	return prev_cpu;
}

void BPF_STRUCT_OPS(ge_enqueue, struct task_struct *p, u64 flags)
{
	struct task_ctx *t;
	struct ev *e;
	if (p->tgid == usersched_tgid) {
		u32 tid = p->pid;
		u32 *owner = bpf_map_lookup_elem(&usersched_tids, &tid);
		u32 shard = owner && *owner < nr_shards ? *owner : 0;
		scx_bpf_dsq_insert(p, SCHED_DSQ_BASE + shard, usersched_slice, flags);
		return;
	}
	t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (t) {
		t->enq_seq++;
		t->waiting = false;
	}
	e = t ? ev_reserve(EV_ENQUEUE, scx_bpf_task_cpu(p)) : 0;
	if (!e) {
		__sync_fetch_and_add(&nr_congested, 1);
		scx_bpf_dsq_insert(p, SCX_DSQ_LOCAL_ON | scx_bpf_task_cpu(p), SCX_SLICE_DFL, flags);
		return;
	}
	ev_task(e, p, t);
	e->flags = flags;
	if ((flags & SCX_ENQ_WAKEUP) && t->selected) {
		e->wake_flags = t->wake_flags;
		e->waker_pid = t->waker_pid;
		e->waker_cpu = t->waker_cpu;
	}
	t->selected = false;
	t->waiting = true;
	__sync_fetch_and_add(&nr_queued, 1);
	__sync_fetch_and_add(&nr_queued_s[shard_for_cpu(scx_bpf_task_cpu(p))], 1);
	bpf_ringbuf_submit(e, 0);
	scx_bpf_kick_cpu(scx_bpf_task_cpu(p), SCX_KICK_PREEMPT);
}

static __always_inline void do_dispatch(const struct cmd *c, u32 owner)
{
	struct task_struct *p = bpf_task_from_pid(c->pid);
	struct task_ctx *t;
	u64 flags = c->enq_flags & (SCX_ENQ_PREEMPT | SCX_ENQ_HEAD);
	s32 cpu = c->cpu;
	u32 key;
	struct cpu_slot *s;
	if (!p)
		return;
	if (cpu < 0 || cpu >= MAX_CPUS || shard_for_cpu(cpu) != owner) {
		__sync_fetch_and_add(&nr_bounces, 1);
		goto out;
	}
	t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (!t || !t->waiting || t->enq_seq != c->enq_seq) {
		__sync_fetch_and_add(&nr_stale_cmds, 1);
		goto out;
	}
	if (cpu < 0 || cpu >= MAX_CPUS || !bpf_cpumask_test_cpu(cpu, p->cpus_ptr)) {
		__sync_fetch_and_add(&nr_bounces, 1);
		cpu = scx_bpf_task_cpu(p);
		flags &= ~SCX_ENQ_PREEMPT;
	}
	if (cpu < 0 || cpu >= MAX_CPUS || shard_for_cpu(cpu) != owner) {
		__sync_fetch_and_add(&nr_bounces, 1);
		goto out;
	}
	key = cpu;
	s = bpf_map_lookup_elem(&cpu_tbl, &key);
	if ((flags & SCX_ENQ_PREEMPT) && (!s || (s32)s->curr_pid != c->expect_curr))
		flags &= ~SCX_ENQ_PREEMPT;
	t->slot = c->slot;
	t->hrtick = !!(c->cflags & CMDF_HRTICK);
	t->waiting = false;
	scx_bpf_dsq_insert(p, SCX_DSQ_LOCAL_ON | cpu, c->slice ? c->slice : SCX_SLICE_DFL, flags);
	if (!(flags & SCX_ENQ_PREEMPT))
		scx_bpf_kick_cpu(cpu, SCX_KICK_IDLE);
out:
	bpf_task_release(p);
}

static long drain_one(struct bpf_dynptr *d, void *ctx)
{
	const struct cmd *c = bpf_dynptr_data(d, 0, sizeof(*c));
	u32 owner = *(u32 *)ctx;
	u32 key;
	struct cpu_slot *s;
	if (!c)
		return 0;
	if (c->cpu < 0 || c->cpu >= MAX_CPUS ||
	    shard_for_cpu(c->cpu) != owner) {
		__sync_fetch_and_add(&nr_bounces, 1);
		return 0;
	}
	switch (c->kind) {
	case CMD_DISPATCH:
		do_dispatch(c, owner);
		break;
	case CMD_KICK:
		if (c->cpu >= 0 && c->cpu < MAX_CPUS)
			scx_bpf_kick_cpu(c->cpu, c->kick_flags);
		break;
	case GE_CMD_STOP:
		if (c->cpu < 0 || c->cpu >= MAX_CPUS)
			break;
		key = c->cpu;
		s = bpf_map_lookup_elem(&cpu_tbl, &key);
		if (s && (s32)s->curr_pid == c->pid)
			scx_bpf_kick_cpu(c->cpu, SCX_KICK_PREEMPT);
		break;
	case CMD_HRTICK:
		hrtick_arm_at(c->cpu, c->slice, c->slot);
		break;
	}
	return 0;
}

static __always_inline void drain_shard(u32 shard)
{
#define DRAIN_CASE(n) case n: bpf_user_ringbuf_drain(&cmds_##n, drain_one, &shard, 0); break;
	switch (shard) {
	EACH_SHARD(DRAIN_CASE)
	}
#undef DRAIN_CASE
}

void BPF_STRUCT_OPS(ge_dispatch, s32 cpu, struct task_struct *prev)
{
	struct cpu_ctl *ctl;
	struct cpu_slot *s;
	u32 key;
	u32 shard = shard_for_cpu(cpu);
	drain_shard(shard);
	/* A runnable userspace scheduler must be able to handle signals and
	 * recover from missed events even when the counters read as zero. */
	if (scx_bpf_dsq_move_to_local(SCHED_DSQ_BASE + shard))
		return;
	if (!prev || prev->tgid == usersched_tgid ||
	    !(BPF_CORE_READ(prev, scx.flags) & SCX_TASK_QUEUED))
		return;
	key = cpu;
	ctl = bpf_map_lookup_elem(&ctl_tbl, &key);
	if (!ctl || ctl->nr_queued)
		return;
	prev->scx.slice = ctl->keep_slice ? ctl->keep_slice : SCX_SLICE_DFL;
	s = bpf_map_lookup_elem(&cpu_tbl, &key);
	if (s)
		s->keeps++;
	send_task(prev, EV_KEEP, 0, BPF_RB_NO_WAKEUP);
}

void BPF_STRUCT_OPS(ge_running, struct task_struct *p)
{
	u32 cpu = scx_bpf_task_cpu(p);
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &cpu);
	if (s && p->tgid != usersched_tgid) {
		struct task_ctx *t;
		s->run_start = bpf_ktime_get_ns();
		s->run_seq++;
		s->curr_pid = p->pid;
		t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
		if (t && t->hrtick && p->scx.slice)
			hrtick_arm_at(cpu, s->run_start + p->scx.slice, (u32)s->run_seq);
	}
}
void BPF_STRUCT_OPS(ge_stopping, struct task_struct *p, bool runnable)
{
	u32 cpu = scx_bpf_task_cpu(p);
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &cpu);
	if (s && s->curr_pid == (u32)p->pid)
		s->curr_pid = 0;
}
void BPF_STRUCT_OPS(ge_tick, struct task_struct *p)
{
	s32 cpu = scx_bpf_task_cpu(p);
	u32 key;
	struct cpu_ctl *ctl;
	struct cpu_slot *s;
	u64 perf;

	if (cpu < 0 || cpu >= MAX_CPUS)
		return;
	key = cpu;
	ctl = bpf_map_lookup_elem(&ctl_tbl, &key);
	s = bpf_map_lookup_elem(&cpu_tbl, &key);
	if (!ctl || !s)
		return;
	perf = READ_ONCE(ctl->perf_target);
	if (scx_bpf_cpuperf_set && perf && perf <= 1024 && perf != s->perf_applied) {
		scx_bpf_cpuperf_set(cpu, perf);
		s->perf_applied = perf;
	}
}

static __always_inline bool core_vtime(struct task_struct *p, u64 *out)
{
	struct task_ctx *t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	struct task_pub *pub;
	u32 slot;
	u64 seq, v, weight, published_at;
	s32 pid;
	s32 cpu;
	struct cpu_slot *s;

	if (!t || !(slot = t->slot) || slot >= MAX_TASKS)
		return false;
	pub = bpf_map_lookup_elem(&task_tbl, &slot);
	if (!pub)
		return false;
	seq = READ_ONCE(pub->seq);
	if (seq & 1)
		return false;
	__asm__ __volatile__("" ::: "memory");
	v = READ_ONCE(pub->core_vtime);
	weight = READ_ONCE(pub->weight);
	published_at = READ_ONCE(pub->published_at);
	pid = READ_ONCE(pub->pid);
	__asm__ __volatile__("" ::: "memory");
	if (READ_ONCE(pub->seq) != seq || pid != p->pid)
		return false;
	cpu = scx_bpf_task_cpu(p);
	if (weight && cpu >= 0 && cpu < MAX_CPUS) {
		u64 start, now, delta, run_seq;
		u32 key = cpu;

		s = bpf_map_lookup_elem(&cpu_tbl, &key);
		if (s && READ_ONCE(s->curr_pid) == pid) {
			run_seq = READ_ONCE(s->run_seq);
			start = READ_ONCE(s->run_start);
			if (published_at > start)
				start = published_at;
			now = bpf_ktime_get_ns();
			if (READ_ONCE(s->curr_pid) == pid &&
			    READ_ONCE(s->run_seq) == run_seq && start && now > start) {
				delta = now - start;
				if (delta > (~0ULL >> 10))
					delta = ~0ULL >> 10;
				v += delta * 1024 / weight;
			}
		}
	}
	*out = v;
	return true;
}

bool BPF_STRUCT_OPS(ge_core_sched_before, struct task_struct *a, struct task_struct *b)
{
	u64 va, vb;

	if (core_vtime(a, &va) && core_vtime(b, &vb))
		return (s64)(va - vb) < 0;
	return (s64)(a->scx.core_sched_at - b->scx.core_sched_at) < 0;
}

void BPF_STRUCT_OPS(ge_dump_task, struct scx_dump_ctx *dctx, struct task_struct *p)
{
	struct task_ctx *t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	struct task_pub *pub;
	u32 slot;
	u64 seq, data[5];
	char fmt[] = "pid=%d vruntime=%llu deadline=%llu lag=%lld weight=%llu";

	if (!t || !(slot = t->slot) || slot >= MAX_TASKS)
		return;
	pub = bpf_map_lookup_elem(&task_tbl, &slot);
	if (!pub)
		return;
	seq = READ_ONCE(pub->seq);
	if (seq & 1)
		return;
	__asm__ __volatile__("" ::: "memory");
	data[0] = p->pid;
	data[1] = READ_ONCE(pub->vruntime);
	data[2] = READ_ONCE(pub->deadline);
	data[3] = READ_ONCE(pub->lag);
	data[4] = READ_ONCE(pub->weight);
	__asm__ __volatile__("" ::: "memory");
	if (scx_bpf_dump_bstr && READ_ONCE(pub->seq) == seq &&
	    READ_ONCE(pub->pid) == p->pid)
		scx_bpf_dump_bstr(fmt, data, sizeof(data));
}
void BPF_STRUCT_OPS(ge_cpu_release, s32 cpu, struct scx_cpu_release_args *args)
{
	u32 key = cpu;
	struct cpu_slot *s;
	if (cpu < 0 || cpu >= MAX_CPUS)
		return;
	s = bpf_map_lookup_elem(&cpu_tbl, &key);
	if (s && !s->released_at)
		s->released_at = bpf_ktime_get_ns();
}
void BPF_STRUCT_OPS(ge_cpu_acquire, s32 cpu, struct scx_cpu_acquire_args *args)
{
	u32 key = cpu;
	struct cpu_slot *s;
	u64 now;
	if (cpu < 0 || cpu >= MAX_CPUS)
		return;
	s = bpf_map_lookup_elem(&cpu_tbl, &key);
	if (!s || !s->released_at)
		return;
	now = bpf_ktime_get_ns();
	s->displaced_ns += now - s->released_at;
	s->released_at = 0;
}
void BPF_STRUCT_OPS(ge_quiescent, struct task_struct *p, u64 flags)
{
	struct task_ctx *t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (t)
		t->waiting = false;
	send_task(p, flags & SCX_DEQ_SLEEP ? EV_SLEEP : EV_DEQUEUE, flags, 0);
}
void BPF_STRUCT_OPS(ge_exit_task, struct task_struct *p, struct scx_exit_task_args *args)
{
	struct task_ctx *t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (t)
		t->waiting = false;
	send_task(p, EV_EXIT, 0, 0);
}
void BPF_STRUCT_OPS(ge_update_idle, s32 cpu, bool idle)
{
	u32 key = cpu;
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &key);
	struct ev *e;
	if (s)
		s->idle = idle;
	u32 shard = shard_for_cpu(cpu);
	if (idle && (nr_queued_s[shard] || nr_scheduled_s[shard] || needed_s[shard]) &&
	    (e = ev_reserve(EV_IDLE, cpu))) {
		needed_s[shard] = 0;
		e->cpu = cpu;
		bpf_ringbuf_submit(e, 0);
		scx_bpf_kick_cpu(cpu, SCX_KICK_IDLE);
	}
}
bool BPF_STRUCT_OPS(ge_yield, struct task_struct *from, struct task_struct *to)
{
	if (to)
		return false;
	send_task(from, EV_YIELD, 0, 0);
	from->scx.slice = 0;
	return false;
}
void BPF_STRUCT_OPS(ge_set_cpumask, struct task_struct *p, const struct cpumask *mask)
{
	send_task(p, EV_CPUMASK, 0, 0);
}
static __always_inline u64 cgroup_id(struct cgroup *cg)
{
	return cg ? BPF_CORE_READ(cg, kn, id) : 0;
}

struct scx_cgroup_init_args___goeevdf {
	u32 weight;
	u64 bw_period_us;
	u64 bw_quota_us;
	u64 bw_burst_us;
} __attribute__((preserve_access_index));

s32 BPF_STRUCT_OPS_SLEEPABLE(ge_cgroup_init, struct cgroup *cg, struct scx_cgroup_init_args *args)
{
	const struct scx_cgroup_init_args___goeevdf *compat = (const void *)args;
	struct ev *e = ev_reserve(EV_CGRP_INIT, -1);
	if (e) {
		e->cgid = cgroup_id(cg);
		e->arg[0] = args->weight;
		if (bpf_core_field_exists(compat->bw_period_us)) {
			e->arg[1] = BPF_CORE_READ(compat, bw_period_us);
			e->arg[2] = BPF_CORE_READ(compat, bw_quota_us);
			e->arg[3] = BPF_CORE_READ(compat, bw_burst_us);
		}
		bpf_ringbuf_submit(e, 0);
	}
	return 0;
}
void BPF_STRUCT_OPS(ge_cgroup_exit, struct cgroup *cg)
{
	struct ev *e = ev_reserve(EV_CGRP_EXIT, -1);
	if (e) {
		e->cgid = cgroup_id(cg);
		bpf_ringbuf_submit(e, 0);
	}
}
void BPF_STRUCT_OPS(ge_cgroup_move, struct task_struct *p, struct cgroup *from, struct cgroup *to)
{
	struct task_ctx *t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	struct ev *e = ev_reserve(EV_CGRP_MOVE, -1);
	if (e) {
		ev_task(e, p, t);
		e->cgid = cgroup_id(to);
		e->arg[0] = cgroup_id(from);
		bpf_ringbuf_submit(e, 0);
	}
}
void BPF_STRUCT_OPS(ge_cgroup_set_weight, struct cgroup *cg, u32 weight)
{
	struct ev *e = ev_reserve(EV_CGRP_WEIGHT, -1);
	if (e) {
		e->cgid = cgroup_id(cg);
		e->arg[0] = weight;
		bpf_ringbuf_submit(e, 0);
	}
}
#ifdef GOEEVDF_HAVE_CGROUP_SET_BANDWIDTH
void BPF_STRUCT_OPS(ge_cgroup_set_bandwidth, struct cgroup *cg, u64 period, u64 quota, u64 burst)
{
	struct ev *e = ev_reserve(EV_CGRP_BW, -1);
	if (e) {
		e->cgid = cgroup_id(cg);
		e->arg[0] = period;
		e->arg[1] = quota;
		e->arg[2] = burst;
		bpf_ringbuf_submit(e, 0);
	}
}
#endif
static long hrtick_init_one(u32 cpu, void *ctx)
{
	int *err = ctx;
	struct hrtick *h = bpf_map_lookup_elem(&hrticks, &cpu);

	if (!h) {
		*err = -12;
		return 1;
	}
	*err = bpf_timer_init(&h->timer, &hrticks, CLOCK_MONOTONIC);
	if (*err)
		return 1;
	*err = bpf_timer_set_callback(&h->timer, hrtick_fire);
	if (*err)
		return 1;
	h->cpu = cpu;
	return 0;
}

s32 BPF_STRUCT_OPS_SLEEPABLE(ge_init)
{
	int ret;
	u32 nr = scx_bpf_nr_cpu_ids();
	int err = 0;

	if (!nr_shards || nr_shards > MAX_SHARDS)
		return -22;
	for (u32 shard = 0; shard < MAX_SHARDS; shard++) {
		if (shard >= nr_shards)
			break;
		ret = scx_bpf_create_dsq(SCHED_DSQ_BASE + shard, -1);
		if (ret)
			return ret;
	}
	if (nr > MAX_CPUS)
		nr = MAX_CPUS;
	ret = bpf_loop(nr, hrtick_init_one, &err, 0);
	if (ret < 0)
		return ret;
	if (err)
		return err;
	return 0;
}
struct kick_arg { s32 cpu; u32 pad; u64 flags; };
struct hrtick_arg { s32 cpu; u32 run_seq; u64 when; };
SEC("syscall")
int ge_kick_prog(struct kick_arg *arg)
{
	if (arg->cpu >= 0 && arg->cpu < MAX_CPUS)
		scx_bpf_kick_cpu(arg->cpu, arg->flags);
	return 0;
}
SEC("syscall")
int ge_hrtick_prog(struct hrtick_arg *arg)
{
	hrtick_arm_at(arg->cpu, arg->when, arg->run_seq);
	return 0;
}
SEC("syscall")
int ge_test_emit_prog(struct kick_arg *arg)
{
	struct ev *e = ev_reserve(EV_ENQUEUE, arg->cpu);
	if (!e)
		return -12;
	e->cpu = arg->cpu;
	e->enq_seq = arg->flags;
	__sync_fetch_and_add(&nr_queued, 1);
	__sync_fetch_and_add(&nr_queued_s[shard_for_cpu(arg->cpu)], 1);
	bpf_ringbuf_submit(e, 0);
	return 0;
}
SEC(".struct_ops")
struct sched_ext_ops ge_ops = {
	.select_cpu = (void *)ge_select_cpu,
	.enqueue = (void *)ge_enqueue,
	.dispatch = (void *)ge_dispatch,
	.running = (void *)ge_running,
	.stopping = (void *)ge_stopping,
	.tick = (void *)ge_tick,
	.core_sched_before = (void *)ge_core_sched_before,
	.dump_task = (void *)ge_dump_task,
	.cpu_release = (void *)ge_cpu_release,
	.cpu_acquire = (void *)ge_cpu_acquire,
	.quiescent = (void *)ge_quiescent,
	.exit_task = (void *)ge_exit_task,
	.init_task = (void *)ge_init_task,
	.update_idle = (void *)ge_update_idle,
	.yield = (void *)ge_yield,
	.set_cpumask = (void *)ge_set_cpumask,
	.cgroup_init = (void *)ge_cgroup_init,
	.cgroup_exit = (void *)ge_cgroup_exit,
	.cgroup_move = (void *)ge_cgroup_move,
	.cgroup_set_weight = (void *)ge_cgroup_set_weight,
#ifdef GOEEVDF_HAVE_CGROUP_SET_BANDWIDTH
	.cgroup_set_bandwidth = (void *)ge_cgroup_set_bandwidth,
#endif
	.init = (void *)ge_init,
	.name = "goeevdf",
};
char LICENSE[] SEC("license") = "GPL";
