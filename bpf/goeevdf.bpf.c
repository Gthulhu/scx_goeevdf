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

#define USER_DSQ 0x47454556ULL
#define MMAP_ARRAY(name, value_t, n) \
	struct { __uint(type, BPF_MAP_TYPE_ARRAY); __uint(map_flags, BPF_F_MMAPABLE); \
		__type(key, u32); __type(value, value_t); __uint(max_entries, n); } name SEC(".maps")

struct { __uint(type, BPF_MAP_TYPE_RINGBUF); __uint(max_entries, 16 << 20); } events SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_USER_RINGBUF); __uint(max_entries, 4 << 20); } cmds SEC(".maps");
MMAP_ARRAY(cpu_tbl, struct cpu_slot, MAX_CPUS);
MMAP_ARRAY(ctl_tbl, struct cpu_ctl, MAX_CPUS);
MMAP_ARRAY(task_tbl, struct task_pub, MAX_TASKS);

struct task_ctx {
	u64 enq_seq;
	u32 slot, wake_flags;
	s32 waker_pid, waker_cpu;
	bool selected, waiting;
};
struct { __uint(type, BPF_MAP_TYPE_TASK_STORAGE); __uint(map_flags, BPF_F_NO_PREALLOC);
	__type(key, int); __type(value, struct task_ctx); } task_ctxs SEC(".maps");

const volatile u32 usersched_tgid;
const volatile u64 usersched_slice = 1000000;
volatile u64 nr_queued, nr_scheduled, nr_ev_drops, nr_congested, nr_stale_cmds, nr_bounces;

static __always_inline struct ev *ev_reserve(u32 kind)
{
	struct ev *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
	if (!e) {
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
	e = ev_reserve(kind);
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
		scx_bpf_dsq_insert(p, USER_DSQ, usersched_slice, flags);
		return;
	}
	t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (t) {
		t->enq_seq++;
		t->waiting = false;
	}
	e = t ? ev_reserve(EV_ENQUEUE) : 0;
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
	bpf_ringbuf_submit(e, 0);
	scx_bpf_kick_cpu(scx_bpf_task_cpu(p), SCX_KICK_PREEMPT);
}

static __always_inline void do_dispatch(const struct cmd *c)
{
	struct task_struct *p = bpf_task_from_pid(c->pid);
	struct task_ctx *t;
	u64 flags = c->enq_flags & (SCX_ENQ_PREEMPT | SCX_ENQ_HEAD);
	s32 cpu = c->cpu;
	u32 key;
	struct cpu_slot *s;
	if (!p)
		return;
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
	key = cpu;
	s = bpf_map_lookup_elem(&cpu_tbl, &key);
	if ((flags & SCX_ENQ_PREEMPT) && (!s || (s32)s->curr_pid != c->expect_curr))
		flags &= ~SCX_ENQ_PREEMPT;
	t->slot = c->slot;
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
	u32 key;
	struct cpu_slot *s;
	if (!c)
		return 0;
	switch (c->kind) {
	case CMD_DISPATCH:
		do_dispatch(c);
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
	}
	return 0;
}

void BPF_STRUCT_OPS(ge_dispatch, s32 cpu, struct task_struct *prev)
{
	struct cpu_ctl *ctl;
	struct cpu_slot *s;
	u32 key;
	bpf_user_ringbuf_drain(&cmds, drain_one, 0, 0);
	/* A runnable userspace scheduler must be able to handle signals and
	 * recover from missed events even when the counters read as zero. */
	if (scx_bpf_dsq_move_to_local(USER_DSQ))
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
		s->curr_pid = p->pid;
		s->run_start = bpf_ktime_get_ns();
		s->run_seq++;
	}
}
void BPF_STRUCT_OPS(ge_stopping, struct task_struct *p, bool runnable)
{
	u32 cpu = scx_bpf_task_cpu(p);
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &cpu);
	if (s && s->curr_pid == (u32)p->pid)
		s->curr_pid = 0;
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
	if (idle && (nr_queued || nr_scheduled) && (e = ev_reserve(EV_IDLE))) {
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
s32 BPF_STRUCT_OPS_SLEEPABLE(ge_cgroup_init, struct cgroup *cg, struct scx_cgroup_init_args *args)
{
	struct ev *e = ev_reserve(EV_CGRP_INIT);
	if (e) {
		e->cgid = cgroup_id(cg);
		e->arg[0] = args->weight;
		e->arg[1] = args->bw_period_us;
		e->arg[2] = args->bw_quota_us;
		e->arg[3] = args->bw_burst_us;
		bpf_ringbuf_submit(e, 0);
	}
	return 0;
}
void BPF_STRUCT_OPS(ge_cgroup_exit, struct cgroup *cg)
{
	struct ev *e = ev_reserve(EV_CGRP_EXIT);
	if (e) {
		e->cgid = cgroup_id(cg);
		bpf_ringbuf_submit(e, 0);
	}
}
void BPF_STRUCT_OPS(ge_cgroup_move, struct task_struct *p, struct cgroup *from, struct cgroup *to)
{
	struct task_ctx *t = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	struct ev *e = ev_reserve(EV_CGRP_MOVE);
	if (e) {
		ev_task(e, p, t);
		e->cgid = cgroup_id(to);
		e->arg[0] = cgroup_id(from);
		bpf_ringbuf_submit(e, 0);
	}
}
void BPF_STRUCT_OPS(ge_cgroup_set_weight, struct cgroup *cg, u32 weight)
{
	struct ev *e = ev_reserve(EV_CGRP_WEIGHT);
	if (e) {
		e->cgid = cgroup_id(cg);
		e->arg[0] = weight;
		bpf_ringbuf_submit(e, 0);
	}
}
void BPF_STRUCT_OPS(ge_cgroup_set_bandwidth, struct cgroup *cg, u64 period, u64 quota, u64 burst)
{
	struct ev *e = ev_reserve(EV_CGRP_BW);
	if (e) {
		e->cgid = cgroup_id(cg);
		e->arg[0] = period;
		e->arg[1] = quota;
		e->arg[2] = burst;
		bpf_ringbuf_submit(e, 0);
	}
}
s32 BPF_STRUCT_OPS_SLEEPABLE(ge_init)
{
	return scx_bpf_create_dsq(USER_DSQ, -1);
}
struct kick_arg { s32 cpu; u32 pad; u64 flags; };
SEC("syscall")
int ge_kick_prog(struct kick_arg *arg)
{
	if (arg->cpu >= 0 && arg->cpu < MAX_CPUS)
		scx_bpf_kick_cpu(arg->cpu, arg->flags);
	return 0;
}
SEC("syscall")
int ge_test_emit_prog(struct kick_arg *arg)
{
	struct ev *e = ev_reserve(EV_ENQUEUE);
	if (!e)
		return -12;
	e->cpu = arg->cpu;
	e->enq_seq = arg->flags;
	__sync_fetch_and_add(&nr_queued, 1);
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
	.cgroup_set_bandwidth = (void *)ge_cgroup_set_bandwidth,
	.init = (void *)ge_init,
	.name = "goeevdf",
};
char LICENSE[] SEC("license") = "GPL";
