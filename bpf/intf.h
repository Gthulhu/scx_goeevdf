#ifndef GOEEVDF_INTF_H
#define GOEEVDF_INTF_H

#define MAX_CPUS 1024
#define MAX_TASKS 65536

enum ev_kind {
	EV_ENQUEUE = 1, EV_SLEEP, EV_DEQUEUE, EV_EXIT, EV_KEEP, EV_IDLE,
	EV_YIELD, EV_CPUMASK, EV_CGRP_INIT, EV_CGRP_EXIT, EV_CGRP_MOVE,
	EV_CGRP_WEIGHT, EV_CGRP_IDLE, EV_CGRP_BW,
};
enum cmd_kind { CMD_DISPATCH = 1, CMD_KICK, GE_CMD_STOP, CMD_HRTICK };
/* vmlinux.h already has an unrelated CMD_STOP enumerator. */
#define CMD_STOP GE_CMD_STOP
#define CMDF_HRTICK (1U << 0)

struct ev {
	u32 kind;
	s32 pid, tgid, cpu;
	u64 flags, now, sum_exec, weight, custom_slice, cgid;
	s32 waker_pid, waker_cpu;
	u32 wake_flags, policy, nr_cpus_allowed, slot;
	u64 enq_seq, arg[4];
};
struct cmd {
	u32 kind;
	s32 pid, cpu;
	u32 cflags;
	u64 slice, enq_flags, enq_seq;
	u32 slot;
	s32 expect_curr;
	u64 kick_flags, pad;
};
struct cpu_slot {
	u32 curr_pid, idle;
	u64 run_start, run_seq, displaced_ns, released_at, keeps, perf_applied, pad;
};
struct cpu_ctl {
	u64 nr_queued, keep_slice, perf_target, pad[5];
};
struct task_pub {
	u64 seq;
	s32 pid;
	u32 pad;
	u64 core_vtime, vruntime, deadline;
	s64 lag;
	u64 weight;
};
_Static_assert(sizeof(struct ev) == 128, "ev size");
_Static_assert(sizeof(struct cmd) == 64, "cmd size");
_Static_assert(sizeof(struct cpu_slot) == 64, "cpu_slot size");
_Static_assert(sizeof(struct cpu_ctl) == 64, "cpu_ctl size");
_Static_assert(sizeof(struct task_pub) == 56, "task_pub size");
#endif
