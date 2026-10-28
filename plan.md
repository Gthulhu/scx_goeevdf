# EEVDF in Go（基於 qumun）實作計畫

## 目標與範圍

目標：把 scx\_reevdf（PR #3926）涵蓋的 fair.c 規則**全部**移植成 Go 排程器 `scx_goeevdf`。所有排程政策與狀態（per-CPU pack、EDQ、placement、負載追蹤、三種 balance、cgroup 權重、cpu.max、pressure、cpufreq、hrtick 決策、core scheduling 的比較值）都在 Go；BPF 是新寫的薄資料平面，只負責回報事件、執行 Go 的指令，以及少數必須在核心回呼裡同步完成的機制。

qumun 不當作要擴充的程式碼，而是技巧來源：bpftool skeleton + cgo 載入、`asmcgocall` 熱路徑、mmap 的 BSS 原子讀寫、`ring_buffer__consume_n` 逐筆取、user ring buffer 送指令、`SEC("syscall")` 程式、心跳 timer、排程執行緒專屬 DSQ。

移植範圍與 PR 模組一一對應：

| PR 模組 | Go 版落點 | 需要 BPF 協助的部分 |
| --- | --- | --- |
| accounting.rs | `eevdf/accounting.go` | 無 |
| pack.rs、taskctx.rs | `eevdf/pack.go`、`task.go`（per-CPU pack） | 無 |
| main.rs、preempt.rs | `eevdf/policy.go`、`preempt.go` | slice 到期回報、`SCX_ENQ_PREEMPT` |
| idle.rs | `eevdf/placement.go` | idle 狀態、waker 與 wake\_flags |
| load.rs | `eevdf/load.go` | `se.sum_exec_runtime`、時間戳 |
| newidle.rs、balance.rs、balance\_state.rs | `eevdf/newidle.go`、`balance.go` | idle 事件 |
| active.rs | `eevdf/active.go` | 對遠端 CPU kick PREEMPT |
| cgroup.rs | `eevdf/cgroup.go` | cgroup 回呼事件、task 的 cgroup id |
| bandwidth.rs | `eevdf/bandwidth.go` | 停下正在跑的任務 |
| pressure.rs | `eevdf/pressure.go` | `ops.cpu_release/cpu_acquire` 計時；IRQ/steal 由 Go 讀 `/proc/stat` |
| hrtick.rs | `eevdf/hrtick.go` | 每 CPU `bpf_timer` |
| core scheduling（main.rs `core_sched_before`） | `eevdf/coresched.go` | `ops.core_sched_before` 讀共享表 |
| features.rs、debug.rs | `eevdf/features.go`、`debug.go` | `ops.dump_task` 讀共享表 |

分期只代表開發順序，不代表範圍取捨：Phase 1 核心 EEVDF（pack、EDQ、placement、preemption、yield、DELAY\_DEQUEUE）；Phase 2 負載與三種 balance、pressure、cpufreq；Phase 3 cgroup 與 cpu.max；Phase 4 hrtick、core scheduling、proxy execution 與調校；Phase 5 per-LLC 分片（Part E，超過 64 顆 CPU 才需要）。每期結束都要能載入並通過該期的測試。

權重單位：Go 內部一律用 fair.c 的 NICE\_0 = 1024，進來時把 `p->scx.weight`（nice 0 = 100）乘 1024/100，讓 PR 的 `tests/accounting.rs` 可逐字移植。

## scx PR #3926 詳讀

[PR #3926](https://github.com/sched-ext/scx/pull/3926) 由 arighi 於 2026-10-25 合併，新增 `scheds/experimental/scx_reevdf`：BPF 端約 9000 行 Rust，逐條對照 fair.c 的 EEVDF 實作（scx\_eevdf 的 Rust 版），跑在 cid-form sched\_ext 與 BPF arena 上。對我們有用的是它的「規則拆解方式」與 `accounting.rs` 的純函式算術，而不是 arena/cid 的管線。

### 核心概念（Go 版要照做的部分）

- **Pack**：一個 CID 上「排隊中 + 正在跑」的任務集合。reference V = 成員的加權平均 vruntime，隨 join / leave / charge 增量維護（`join_delta`、`leave_delta`、`charge` 帶 remainder 防截斷）。沒有全域虛擬時間，vruntime 只在 pack 內有意義。
- **Eligibility**：`vruntime <= V`（`!time_before(V, v)`）才有資格被挑。
- **Lag**：`clamp(V - v, ±limit)`，limit = `scale_by_dl_weight(max(request, default) + tick, w)`。任務離開 pack 時存 lag，再加入時用 `place()` 還原，並以 (W+w)/W 放大（PLACE\_LAG）。
- **Request / deadline**：`deadline = v + request × NICE0 / max(w, NICE0/4)`。`Request.prepare()` 只在前一個 request 用完時重發；首次（fork）只要一半（PLACE\_DEADLINE\_INITIAL）；睡眠會丟棄剩餘 request，非睡眠搬移用 `rebase()` 保留相對 deadline（PLACE\_REL\_DEADLINE）。
- **EDQ**：每 CID 一棵依 deadline 排序、可一次下降找到「第一個 eligible」的樹。Pick = queue 與 curr 之中 eligible 且 deadline 最早者（`pick_eevdf()`）；沒有 eligible 時退回 head 以維持工作守恆。
- **Wakeup preemption**（`preempt.rs::should_preempt`）：wakee 必須 eligible；PREEMPT\_SHORT（wakee request 較短）可越過保護；curr 仍在 RUN\_TO\_PARITY 保護內則不搶；wakee deadline 必須早於 curr 與 queue head。不搶時把 curr 的保護縮到最短競爭 request。
- **Keep running**（`keep_running`）：slice 用完時先 charge、重發 request，若 curr 仍 eligible 且無更早 deadline 的 eligible 任務就續跑（平手也續跑）。
- **DELAY\_DEQUEUE**：負 lag 睡眠的任務不立即「洗白」，記下離開時的 (V, W, generation)，醒來時用 `delay_debt()` 依 pack 前進量償還；pack 曾清空則債務免除。

### 回呼與職責對照

| 回呼 | scx\_reevdf 做什麼 | 檔案 |
| --- | --- | --- |
| select\_cid | 喚醒/fork 找 idle CID，負債任務直接回原 CID | idle.rs |
| enqueue | place 進 pack，決定直接跑、搶佔或排隊 | main.rs, pack.rs, preempt.rs |
| dispatch | keep\_running 或從 EDQ 取第一個 eligible，否則 balance/steal | main.rs, preempt.rs |
| running | 換 pack（帶 lag）、prepare\_request、publish curr 資訊 | main.rs, pack.rs |
| stopping | `charge()`：vruntime += Δ × NICE0 / w，fold 進 V | main.rs, pack.rs |
| quiescent | 先 charge 再算 lag、離開 pack、記 delay | main.rs, pack.rs |
| yield | eligible 時 vruntime = deadline 並重發 request | main.rs |

### Feature 開關（Go 版保留同名 flag）

PR 的每條 fair.c 規則都能單獨關掉（`features.rs` 與 loader 的 CLI），共 20 餘個開關與數值參數；Go 版逐一對應，完整對照表見 C14。

### 測試資產可直接借用

`tests/accounting.rs` 有 20 餘個純算術測試（如 `weighted_deadline`：prepare(1000, 200, 2048) → deadline 1100；`joins_track_the_weighted_average` → 550），Go 版應逐一移植。`tests/fairness.py` 把 nice 0 與 nice 6 的 worker 綁同一顆 CPU 跑 6 秒，要求 CPU 時間比落在 2.5–5.0。

## qumun 架構：可借用的 cgo 與 BPF 技巧

[qumun](https://github.com/Gthulhu/qumun)（commit `5cb4a45`，2026-10-27）是 scx\_rustland\_core 的 Go 版：BPF 把可執行任務經 ring buffer 丟給單一、`LockOSThread` 的 Go 迴圈，Go 決定順序與 CPU，再經 user ring buffer 交回 BPF dispatch。以下是它與 BPF 互動的七種管道，scx\_goeevdf 借用這些技巧，但 BPF 程式與 Go 套件是新寫的。

| # | 管道 | 方向 | qumun 實作 | 呼叫方式 |
| --- | --- | --- | --- | --- |
| 1 | Skeleton 載入 | Go→核心 | `wrapper.c` 的 `open_skel()` 呼叫 bpftool 產生的 `main_bpf__open()`；libbpfgo fork 的 `BPFReplaceExistedObject` 接手 `bpf_object`，`Start()` = `BPFLoadObject` | 標準 cgo |
| 2 | Rodata 設定 | Go→BPF | `const volatile` 變數（`usersched_pid`、`builtin_idle`、`adaptive_wakeup`、`default_slice`）經 `&C.global_obj.rodata.x` 用 `schedbridge.StoreBool/Uint64` 寫入，必須在 `Start()` 前 | asmcgocall 直呼 |
| 3 | BSS 計數器 | 雙向 | `nr_queued`、`nr_scheduled` 等；讀用 `atomic.LoadUint64` 直接讀 mmap 的 BSS 位址，寫用 `schedbridge.StoreUint64/SubUint64` | 無 C 呼叫 / asmcgocall |
| 4 | `queued` RINGBUF | BPF→Go | enqueue 時 `bpf_ringbuf_reserve` 72 byte `queued_task_ctx`。adaptive 模式下 Go 用 `ring_buffer__consume_n(ring, 1)`（`hotpath.c`）逐筆取，callback 只 memcpy 到 C 記憶體 | asmcgocall 直呼 |
| 5 | `dispatched` USER\_RINGBUF | Go→BPF | Go `encodeDispatch` 寫 64 byte 記錄，`user_ring_buffer__reserve/submit` 非阻塞；BPF 在 `ops.dispatch` 用 `bpf_user_ringbuf_drain` 逐筆 `dispatch_task()` 插入 per-CPU 或 SHARED DSQ（`insert_vtime`，vtime 即排序鍵） | asmcgocall 直呼 |
| 6 | SEC("syscall") 程式 | Go→BPF | `rs_select_cpu`（找 idle CPU，無則回負值→`RL_CPU_ANY`）、`do_preempt`（kick PREEMPT）、`enable_sibling_cpu`；用 `BPF_PROG_TEST_RUN` 觸發，熱路徑走 `c_select_cpu_wrapper` | asmcgocall 直呼 / cilium `Program.Run` |
| 7 | 一般 BPF map | Go→BPF | `priority_tasks`、`priority_tasks_prio` 用 `bpf_map__update_elem` | 標準 cgo |

### 熱路徑 cgo 規則（agent 必須遵守）

- `internal/schedbridge.Call(fn, arg)` 透過 `//go:linkname runtime.asmcgocall` 直接切到 g0 堆疊，不經 `entersyscall`，不釋放 P。只能呼叫「有界、不阻塞、不配置記憶體、不回呼 Go、不保存 Go 指標」的 C wrapper。可能阻塞的呼叫用 `schedbridge.CallSyscall`（走 `runtime.cgocall`）或一般 cgo。
- 每個 wrapper 只收一個 `void *`，指向 Go 端同版面的 struct；Go 端用成對的陣列長度斷言（`_ [unsafe.Sizeof(x{}) - C.sizeof_struct_x]byte` 雙向）在編譯期檢查 size、alignment、每個 field offset。新增任何 wire struct 都要照抄這套斷言。
- 連結需要 `-ldflags=-checklinkname=0`；只支援 linux amd64/arm64。
- 不要設 `GOMAXPROCS=1`：SelectCPU 佔住 P 時，其他 goroutine 仍需前進。

### 借用與不沿用

qumun 的 `main.bpf.c` 是 scx\_rustland\_core 式的「全域 vtime + 單一 queued ring」後端：它沒有 per-CPU 狀態、不回報睡眠與退出、只有 72 byte 的固定記錄、依賴外部 `models.QueuedTask`。全量移植 fair.c 需要的資料它都沒有，所以 BPF 程式重寫，Go 也另起 module。

| 技巧 | 來源 | scx\_goeevdf 怎麼用 |
| --- | --- | --- |
| bpftool skeleton + `open_skel()` 由 cgo 開啟，load 前寫 rodata | `wrapper.c`、`rodata.go` | 照抄模式，換成 `goeevdf.skel.h`；不依賴 libbpfgo fork，直接用 libbpf C API |
| `schedbridge.Call`（`asmcgocall`）+ 單一 `void *` 參數 struct + 雙向編譯期斷言 | `internal/schedbridge`、`hotpath.go` | 整包複製；事件 consume、指令 submit、`do_kick` syscall 都走它 |
| `ring_buffer__consume_n(ring, 1)`，callback 只 memcpy 到 C 記憶體 | `hotpath.c` | 記錄改 128 byte，callback 檢查大小 |
| `user_ring_buffer__reserve/submit` 非阻塞，`ENOSPC` 回傳不等待 | `hotpath.c` | 指令 ring（dispatch、kick、stop） |
| BSS 原子讀寫（skeleton 位址 + `atomic.Load*`） | `bss.go` | 只放計數器；大量共享狀態改用 `BPF_F_MMAPABLE` array map，同樣手法 mmap |
| `SEC("syscall")` + `BPF_PROG_TEST_RUN` | `rs_select_cpu`、`do_preempt` | 保留 `do_kick(cpu, flags)` 作為不必等 dispatch drain 的緊急 kick |
| 心跳 `bpf_timer` 喚醒排程執行緒 | `usersched_timer` | 1 ms 心跳，驅動 Go 的週期工作（balance、bandwidth refill、pressure 視窗） |
| 排程執行緒專屬 DSQ，有待辦才派 | `SCHED_DSQ`、`usersched_has_pending_tasks()` | 照抄 |
| sysfs 拓撲解析 | `util/topology.go` | 擴充為 core / cluster / LLC / node / capacity / packing tier |

不沿用：qumun 的 `queued_task_ctx`、`dispatched_task_ctx`、`priority_tasks`、kernel mode、`builtin_idle` 直派、`SHARED_DSQ` 與 vtime DSQ（改為 Go 決定 CPU 後直接插 `SCX_DSQ_LOCAL_ON | cpu`）、`models.QueuedTask` 與 plugin 介面。

## 整體設計：Go 控制平面、BPF 資料平面

PR 把每條 fair.c 規則放進對應的 sched\_ext 回呼；scx\_goeevdf 把同一條規則放進 Go，觸發時機改由「BPF 事件 + Go 1 ms 心跳」提供。每顆 CPU 在 Go 裡有自己的 pack（V、W）與 EDQ，對應 PR 的 per-CID pack；Go 決定任務去哪顆 CPU，再以 `SCX_DSQ_LOCAL_ON | cpu` 插入，任務不會被核心自行搬走，所以 Go 的視圖與實際一致。

&#91;embedded content: scx\_goeevdf 架構 · Go 控制平面 / BPF 資料平面\]

往下的箭頭是 BPF 給 Go 的資訊，往上的是 Go 給 BPF 的指令；共享表雙向，但每個欄位只有一方寫。

### 回呼時機對照（PR → Go）

| fair.c / PR 時機 | scx\_goeevdf 觸發來源 |
| --- | --- |
| select\_cid + enqueue（喚醒、fork、搬移） | EV\_ENQUEUE（帶 waker、wake\_flags、prev cpu） |
| dispatch：keep\_running 或 pick | 有同伴時：prev 經 EV\_ENQUEUE（非 WAKEUP）回 Go；Go 端該 CPU 佇列空時 BPF 直接續跑（共享表 `nr_queued == 0`） |
| running / stopping 的 charge | 每個事件都帶 `se.sum_exec_runtime`，Go 自算差值 |
| quiescent（lag、DELAY\_DEQUEUE） | EV\_SLEEP / EV\_DEQUEUE |
| tick：load、group weight、cpu.max、兩種 balance | Go 1 ms 心跳 + 各模組自己的 interval |
| update\_idle → newidle pull | EV\_IDLE |
| hrtick | Go 在 dispatch 指令標 HRTICK，BPF `running` 時武裝 bpf\_timer |
| core\_sched\_before | BPF 讀 `task_tbl[slot].core_vtime`（Go 寫） |
| cpu.max 節流、active balance 搬正在跑的任務 | 指令 STOP（kick PREEMPT），任務經 EV\_ENQUEUE 回 Go |

### 一個任務的生命週期

1. **醒來**：BPF `select_cpu` 只記下 wake\_flags 並回 prev\_cpu；`enqueue` 送 EV\_ENQUEUE。Go 用 `placement.go` 選 CPU c → `Pack[c].Place`（lag、DELAY\_DEQUEUE 結清）→ `Prepare` → c 閒置就直接 DISPATCH，否則跑 `shouldPreempt`，再否則放進 `EDQ[c]`。
2. **執行**：BPF `running` 在 `cpu_tbl[c]` 寫 curr pid、開始時間、run\_seq，必要時武裝 hrtick。
3. **slice 到期**：`EDQ[c]` 非空時 BPF 不續跑，prev 以非 WAKEUP 的 EV\_ENQUEUE 回 Go；Go charge、重發 deadline、做 c 的 pick（keep\_running 平手留 curr）。
4. **CPU 空了**：EV\_IDLE → Go 先看 `EDQ[c]`，空的就 newidle pull（同 LLC → node → 全機）。
5. **睡眠**：EV\_SLEEP → charge → `Quiesce`（存 lag，負 lag 記 delay）。
6. **背景**：心跳驅動 load 取樣、periodic balance、active balance、cgroup shares、cpu.max refill、pressure 視窗、cpufreq、GC。

### 關鍵設計決策

- **單一排程 goroutine 持有全部狀態**：不需要 PR 的 atomics、trylock 與跨 CPU 交接協定（active.rs 的 reservation），規則可以照 fair.c 的順序直寫。代價是事件吞吐受單核限制；超過約 64 顆 CPU 時可改成每 LLC 一個 goroutine，各自持有該 LLC 的 pack，跨 LLC 搬移走 channel。
- **執行時間用 `se.sum_exec_runtime`**：核心在 `update_curr_scx` 以 rq task clock 累加它，已扣掉 IRQ 時間（CONFIG\_IRQ\_TIME\_ACCOUNTING），正是 PR 用 task clock charge 的意圖；Go 保留上次值算差，事件丟了也不會漏算時間。
- **Go 能做 BPF 做不到的事**：`sched_getaffinity` 取完整 cpumask、讀 cgroupfs 的 `cpu.max`/`cpu.idle`/`cpuset.cpus.effective`、讀 `/proc/stat` 的 irq/softirq/steal、讀 sysfs 的 `cpu_capacity`。這些取代 PR 裡需要推估或輪詢的部分。
- **指令帶 `enq_seq`**：BPF 每次 enqueue 遞增任務的序號，Go 的 DISPATCH 指令回填它看到的序號，不符就丟棄，避免對已被核心收回又重新入列的任務重複派送。

## Part A1：新 BPF 程式（`bpf/intf.h`、`bpf/goeevdf.bpf.c`）

原則：BPF 不算任何 EEVDF 數字，只做三件事：把事件連同 `se.sum_exec_runtime` 送給 Go；照 Go 的指令插入 `SCX_DSQ_LOCAL_ON | cpu`、kick、停下正在跑的任務；在 Go 對某 CPU 沒有排隊任務時讓 prev 續跑。程式以 scx 的 `common.bpf.h` 與 compat 巨集編寫（`scx_bpf_dsq_insert` / `scx_bpf_dispatch` 依核心版本切換）。

### A1.1 `intf.h`：wire 格式與共享表

```c
#define MAX_CPUS  1024
#define MAX_TASKS 65536 /* task_tbl 槽數，Go 配發 slot */

enum ev_kind {
	EV_ENQUEUE = 1, EV_SLEEP, EV_DEQUEUE, EV_EXIT, EV_KEEP, EV_IDLE, EV_YIELD,
	EV_CPUMASK, EV_CGRP_INIT, EV_CGRP_EXIT, EV_CGRP_MOVE, EV_CGRP_WEIGHT,
	EV_CGRP_IDLE, EV_CGRP_BW,
};

struct ev {                      /* 128 byte，little endian */
	u32 kind;  s32 pid;  s32 tgid;  s32 cpu;    /*   0 */
	u64 flags;          /* enq_flags / deq_flags      16 */
	u64 now;            /* bpf_ktime_get_ns()         24 */
	u64 sum_exec;       /* p->se.sum_exec_runtime     32 */
	u64 weight;         /* p->scx.weight              40 */
	u64 custom_slice;   /* sched_attr.sched_runtime   48 */
	u64 cgid;           /* scx_bpf_task_cgroup() id   56 */
	s32 waker_pid;  s32 waker_cpu;              /*  64 */
	u32 wake_flags; u32 policy;                 /*  72 */
	u32 nr_cpus_allowed; u32 slot;              /*  80 */
	u64 enq_seq;                                /*  88 */
	u64 arg[4];         /* 依 kind：cgroup 參數等      96 */
};

enum cmd_kind { CMD_DISPATCH = 1, CMD_KICK, CMD_STOP, CMD_HRTICK };
#define CMDF_HRTICK (1u << 0)

struct cmd {                     /* 64 byte */
	u32 kind;  s32 pid;  s32 cpu;  u32 cflags;  /*  0 */
	u64 slice;          /* ns                         16 */
	u64 enq_flags;      /* 可含 SCX_ENQ_PREEMPT        24 */
	u64 enq_seq;        /* 必須等於任務目前序號        32 */
	u32 slot;  s32 expect_curr; /* PREEMPT 時預期的 curr pid  40 */
	u64 kick_flags;     /* CMD_KICK 用                48 */
	u64 pad;                                    /*  56 */
};

struct cpu_slot {                /* BPF 寫、Go 讀；64 byte 一條 cache line */
	u32 curr_pid; u32 idle;
	u64 run_start;      /* bpf_ktime_get_ns() */
	u64 run_seq;        /* 每次 running +1，hrtick 驗證用 */
	u64 displaced_ns;   /* cpu_release→acquire 累計 */
	u64 released_at;
	u64 keeps;          /* BPF 自行續跑 prev 的次數 */
	u64 pad[2];
};

struct cpu_ctl {                 /* Go 寫、BPF 讀 */
	u64 nr_queued;      /* Go 端 EDQ[cpu] 的長度 */
	u64 keep_slice;     /* 續跑時給的 slice */
	u64 perf_target;    /* 0..1024，tick 套用 */
	u64 pad[5];
};

struct task_pub {                /* Go 寫、BPF 讀；seqcount 保護 */
	u64 seq;            /* 奇數 = Go 正在寫 */
	s32 pid; u32 pad;
	u64 core_vtime;     /* core_sched_before 比較值 */
	u64 vruntime, deadline;
	s64 lag;
	u64 weight;
};

_Static_assert(sizeof(struct ev) == 128, "ev");
_Static_assert(sizeof(struct cmd) == 64, "cmd");
_Static_assert(sizeof(struct cpu_slot) == 64, "cpu_slot");
```

### A1.2 Maps 與 per-task context

```c
struct { __uint(type, BPF_MAP_TYPE_RINGBUF); __uint(max_entries, 16 << 20); } events SEC(".maps");
struct { __uint(type, BPF_MAP_TYPE_USER_RINGBUF); __uint(max_entries, 4 << 20); } cmds SEC(".maps");

#define MMAP_ARRAY(name, T, n) struct { __uint(type, BPF_MAP_TYPE_ARRAY); \
	__uint(map_flags, BPF_F_MMAPABLE); __type(key, u32); __type(value, T); \
	__uint(max_entries, n); } name SEC(".maps")
MMAP_ARRAY(cpu_tbl, struct cpu_slot, MAX_CPUS);
MMAP_ARRAY(ctl_tbl, struct cpu_ctl, MAX_CPUS);
MMAP_ARRAY(task_tbl, struct task_pub, MAX_TASKS);

struct task_ctx {
	u64 enq_seq;
	u32 slot;        /* Go 在 DISPATCH 帶來；0 = 尚未配發 */
	u32 wake_flags;
	s32 waker_pid, waker_cpu;
	bool selected;   /* select_cpu 剛記過喚醒資訊 */
	bool hrtick;
};
struct { __uint(type, BPF_MAP_TYPE_TASK_STORAGE); __uint(map_flags, BPF_F_NO_PREALLOC);
	 __type(key, int); __type(value, struct task_ctx); } task_ctxs SEC(".maps");

const volatile u32 usersched_tgid;
const volatile u64 usersched_slice = 1000000;
volatile u64 nr_queued, nr_ev_drops, nr_congested, nr_stale_cmds, nr_bounces;
```

### A1.3 事件 helper

```c
static __always_inline struct ev *ev_reserve(u32 kind)
{
	struct ev *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);

	if (!e) {
		__sync_fetch_and_add(&nr_ev_drops, 1);
		return NULL;
	}
	__builtin_memset(e, 0, sizeof(*e));
	e->kind = kind;
	e->now = bpf_ktime_get_ns();
	return e;
}

static __always_inline void ev_task(struct ev *e, struct task_struct *p, struct task_ctx *tctx)
{
	e->pid = p->pid;
	e->tgid = p->tgid;
	e->cpu = scx_bpf_task_cpu(p);
	e->sum_exec = p->se.sum_exec_runtime; /* update_curr_scx 以 task clock 累加 */
	e->weight = p->scx.weight;
	if (bpf_core_field_exists(p->se.custom_slice) && p->se.custom_slice)
		e->custom_slice = p->se.slice;
	e->policy = p->policy;
	e->nr_cpus_allowed = p->nr_cpus_allowed;
	if (tctx) {
		e->slot = tctx->slot;
		e->enq_seq = tctx->enq_seq;
	}
}

static void ev_send_task(struct task_struct *p, u32 kind, u64 flags, u64 rb_flags)
{
	struct task_ctx *tctx = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	struct ev *e;

	if (p->tgid == usersched_tgid || !(e = ev_reserve(kind)))
		return;
	ev_task(e, p, tctx);
	e->flags = flags;
	bpf_ringbuf_submit(e, rb_flags);
}
```

### A1.4 select\_cpu、enqueue

```c
s32 BPF_STRUCT_OPS(ge_select_cpu, struct task_struct *p, s32 prev_cpu, u64 wake_flags)
{
	struct task_ctx *tctx = bpf_task_storage_get(&task_ctxs, p, 0, 0);

	if (tctx) {   /* 放置全由 Go 決定，這裡只記喚醒資訊 */
		tctx->wake_flags = wake_flags;
		tctx->waker_pid = (s32)bpf_get_current_pid_tgid();
		tctx->waker_cpu = bpf_get_smp_processor_id();
		tctx->selected = true;
	}
	return prev_cpu;
}

void BPF_STRUCT_OPS(ge_enqueue, struct task_struct *p, u64 enq_flags)
{
	struct task_ctx *tctx;
	struct cgroup *cg;
	struct ev *e;

	if (p->tgid == usersched_tgid) {
		scx_bpf_dsq_insert(p, SCHED_DSQ, usersched_slice, enq_flags);
		return;
	}
	tctx = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (tctx)
		tctx->enq_seq++;
	if (!tctx || !(e = ev_reserve(EV_ENQUEUE))) {
		/* ring 滿或無 context：先在原 CPU 跑，執行時間仍記在 sum_exec */
		__sync_fetch_and_add(&nr_congested, 1);
		scx_bpf_dsq_insert(p, SCX_DSQ_LOCAL_ON | scx_bpf_task_cpu(p), SCX_SLICE_DFL, enq_flags);
		return;
	}
	ev_task(e, p, tctx);
	e->flags = enq_flags;
	if ((enq_flags & SCX_ENQ_WAKEUP) && tctx->selected) {
		e->wake_flags = tctx->wake_flags;
		e->waker_pid = tctx->waker_pid;
		e->waker_cpu = tctx->waker_cpu;
	}
	tctx->selected = false;
	if ((cg = scx_bpf_task_cgroup(p))) {
		e->cgid = cg->kn->id;
		bpf_cgroup_release(cg);
	}
	__sync_fetch_and_add(&nr_queued, 1); /* 先記帳再發布 */
	bpf_ringbuf_submit(e, 0);
}
```

`SCX_ENQ_REENQ`（`cpu_release` 時 `scx_bpf_reenqueue_local()` 退回的任務）也走同一條路，Go 依 flags 判斷是 pressure 造成的退回。

### A1.5 dispatch：執行指令、續跑判斷

```c
static void do_dispatch(const struct cmd *c)
{
	struct task_struct *p = bpf_task_from_pid(c->pid);
	struct task_ctx *tctx;
	u64 flags = c->enq_flags;
	s32 cpu = c->cpu;

	if (!p)
		return;
	tctx = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (!tctx || tctx->enq_seq != c->enq_seq) {
		__sync_fetch_and_add(&nr_stale_cmds, 1); /* 核心已收回或重新入列 */
		goto out;
	}
	if (cpu < 0 || cpu >= MAX_CPUS || !bpf_cpumask_test_cpu(cpu, p->cpus_ptr)) {
		__sync_fetch_and_add(&nr_bounces, 1); /* affinity 剛變：先送回原 CPU */
		cpu = scx_bpf_task_cpu(p);
		flags &= ~SCX_ENQ_PREEMPT;
	}
	if (flags & SCX_ENQ_PREEMPT) {
		struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});
		if (!s || (s32)s->curr_pid != c->expect_curr)
			flags &= ~SCX_ENQ_PREEMPT; /* 受害者已換人：不搶，只排隊 */
	}
	tctx->slot = c->slot;
	tctx->hrtick = c->cflags & CMDF_HRTICK;
	scx_bpf_dsq_insert(p, SCX_DSQ_LOCAL_ON | cpu, c->slice, flags);
	if (!(flags & SCX_ENQ_PREEMPT))
		scx_bpf_kick_cpu(cpu, SCX_KICK_IDLE);
out:
	bpf_task_release(p);
}

static void do_stop(const struct cmd *c)
{
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){c->cpu});

	/* 只在目標仍是同一個任務時停：active balance、cpu.max 節流 */
	if (s && (s32)s->curr_pid == c->pid)
		scx_bpf_kick_cpu(c->cpu, SCX_KICK_PREEMPT);
}

static long drain_one(struct bpf_dynptr *d, void *ctx)
{
	const struct cmd *c = bpf_dynptr_data(d, 0, sizeof(*c));

	if (!c)
		return 0;
	switch (c->kind) {
	case CMD_DISPATCH: do_dispatch(c); break;
	case CMD_KICK:     scx_bpf_kick_cpu(c->cpu, c->kick_flags); break;
	case CMD_STOP:     do_stop(c); break;
	case CMD_HRTICK:   hrtick_arm_at(c->cpu, c->slice, c->slot); break;
	}
	return scx_bpf_dispatch_nr_slots() ? 0 : 1; /* 0 繼續、1 停止 */
}

void BPF_STRUCT_OPS(ge_dispatch, s32 cpu, struct task_struct *prev)
{
	struct cpu_ctl *ctl;

	bpf_user_ringbuf_drain(&cmds, drain_one, NULL, BPF_RB_NO_WAKEUP);
	if (usersched_has_pending() && scx_bpf_dsq_move_to_local(SCHED_DSQ))
		return;
	/* pick_eevdf：Go 在這顆 CPU 沒有排隊任務 → curr 續跑 */
	if (prev && (prev->scx.flags & SCX_TASK_QUEUED) && prev->tgid != usersched_tgid &&
	    (ctl = bpf_map_lookup_elem(&ctl_tbl, &(u32){cpu})) && !READ_ONCE(ctl->nr_queued)) {
		struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});

		prev->scx.slice = READ_ONCE(ctl->keep_slice) ?: SCX_SLICE_DFL;
		if (s)
			s->keeps++;
		/* 讓 Go 補做 update_curr/update_deadline，但不喚醒它 */
		ev_send_task(prev, EV_KEEP, 0, BPF_RB_NO_WAKEUP);
	}
}
```

`usersched_has_pending()` 照 qumun：`nr_queued || nr_scheduled || bpf_ringbuf_query(&events, BPF_RB_AVAIL_DATA) || usersched_needed`。

### A1.6 running、stopping、quiescent、exit、idle、yield、cpumask

```c
void BPF_STRUCT_OPS(ge_running, struct task_struct *p)
{
	s32 cpu = scx_bpf_task_cpu(p);
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});
	struct task_ctx *tctx;

	if (!s)
		return;
	if (p->tgid == usersched_tgid) {
		usersched_last_run_at = bpf_ktime_get_ns();
		return;
	}
	s->curr_pid = p->pid;
	s->run_start = bpf_ktime_get_ns();
	s->run_seq++;
	tctx = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	if (tctx && tctx->hrtick)
		hrtick_arm(cpu, p->scx.slice, s->run_seq); /* A2 */
}

void BPF_STRUCT_OPS(ge_stopping, struct task_struct *p, bool runnable)
{
	s32 cpu = scx_bpf_task_cpu(p);
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});

	if (s && s->curr_pid == (u32)p->pid)
		s->curr_pid = 0;
}

void BPF_STRUCT_OPS(ge_quiescent, struct task_struct *p, u64 deq_flags)
{
	ev_send_task(p, (deq_flags & SCX_DEQ_SLEEP) ? EV_SLEEP : EV_DEQUEUE, deq_flags, 0);
}

void BPF_STRUCT_OPS(ge_exit_task, struct task_struct *p, struct scx_exit_task_args *args)
{
	ev_send_task(p, EV_EXIT, 0, 0);
}

void BPF_STRUCT_OPS(ge_update_idle, s32 cpu, bool idle)
{
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});
	struct ev *e;

	if (s)
		WRITE_ONCE(s->idle, idle);
	if (!idle || !(nr_queued || nr_scheduled))
		return;  /* Go 沒有待派任務就不必 newidle */
	if ((e = ev_reserve(EV_IDLE))) {
		e->cpu = cpu;
		bpf_ringbuf_submit(e, 0);
	}
	set_usersched_needed();
	scx_bpf_kick_cpu(cpu, 0);
}

bool BPF_STRUCT_OPS(ge_yield, struct task_struct *from, struct task_struct *to)
{
	if (to)
		return false;   /* yield_to() 拒絕，同 PR */
	ev_send_task(from, EV_YIELD, 0, 0);
	from->scx.slice = 0; /* 結束 slice，回 Go 做 yield_task_fair */
	return false;
}

void BPF_STRUCT_OPS(ge_set_cpumask, struct task_struct *p, const struct cpumask *mask)
{
	ev_send_task(p, EV_CPUMASK, 0, 0); /* Go 以 sched_getaffinity 重讀 */
}
```

`stopping` 不送事件：slice 到期會接 enqueue，睡眠會接 quiescent，兩者都帶 `sum_exec`；省下每次切換一筆記錄。

## Part A2：必須在核心裡同步完成的機制

這些機制的時機在回呼內或計時器裡，Go 無法即時介入；但它們用的數字（何時到期、比較值、目標頻率）全由 Go 寫進共享表或指令。`cpu_slot` 的 `pad[0]` 改名為 `perf_applied`。

### A2.1 hrtick（hrtick.rs）

```c
struct hrtick { struct bpf_timer timer; u64 run_seq; s32 cpu; };
struct { __uint(type, BPF_MAP_TYPE_ARRAY); __type(key, u32); __type(value, struct hrtick);
	 __uint(max_entries, MAX_CPUS); } hrticks SEC(".maps");

static int hrtick_fire(void *map, int *key, struct hrtick *h)
{
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, key);
	struct cpu_ctl *ctl = bpf_map_lookup_elem(&ctl_tbl, key);

	/* 仍是同一段執行、且 Go 在這顆 CPU 有排隊任務：結束 request */
	if (s && ctl && (u32)s->run_seq == (u32)h->run_seq && READ_ONCE(ctl->nr_queued))
		scx_bpf_kick_cpu(h->cpu, SCX_KICK_PREEMPT);
	return 0;
}

static void hrtick_arm_at(s32 cpu, u64 when_ns, u64 run_seq) /* CLOCK_MONOTONIC 絕對時間 */
{
	struct hrtick *h = bpf_map_lookup_elem(&hrticks, &(u32){cpu});

	if (!h)
		return;
	h->run_seq = run_seq;
	h->cpu = cpu;
	bpf_timer_start(&h->timer, when_ns, BPF_F_TIMER_ABS);
}

static void hrtick_arm(s32 cpu, u64 slice, u64 run_seq)
{
	hrtick_arm_at(cpu, bpf_ktime_get_ns() + slice, run_seq);
}
```

兩個武裝點，對應 PR 的 `start_with_company()` 與 `hrtick_update()`：DISPATCH 指令帶 `CMDF_HRTICK`（派工當下 `EDQ[cpu]` 非空）時由 `running` 武裝；任務後來加入一顆只有 curr 的 CPU 時，Go 送新指令 `CMD_HRTICK`（`cpu`、`slice` 欄位放絕對到期時間、`slot` 欄位放 Go 讀到的 `run_seq`），`drain_one` 呼叫 `hrtick_arm_at`。timer 無法從 op 取消，過期的那次會因 `run_seq` 不符而空轉，與 PR 相同。`ge_init` 對每顆可能的 CPU 做 `bpf_timer_init(&h->timer, &hrticks, CLOCK_MONOTONIC)` 與 `bpf_timer_set_callback(&h->timer, hrtick_fire)`。

### A2.2 core scheduling（`ops.core_sched_before`）

```c
static __always_inline bool core_vtime(struct task_struct *p, u64 *out)
{
	struct task_ctx *tctx = bpf_task_storage_get(&task_ctxs, p, 0, 0);
	struct task_pub *tp;
	u64 seq, v;

	if (!tctx || !tctx->slot || !(tp = bpf_map_lookup_elem(&task_tbl, &tctx->slot)))
		return false;
	seq = READ_ONCE(tp->seq);   /* arm64 需改 load-acquire */
	if (seq & 1)
		return false;
	v = READ_ONCE(tp->core_vtime);
	if (READ_ONCE(tp->seq) != seq || tp->pid != p->pid)
		return false;
	*out = v;
	return true;
}

bool BPF_STRUCT_OPS(ge_core_sched_before, struct task_struct *a, struct task_struct *b)
{
	u64 va, vb;

	if (core_vtime(a, &va) && core_vtime(b, &vb))
		return (s64)(va - vb) < 0;
	return (s64)(a->scx.core_sched_at - b->scx.core_sched_at) < 0; /* 退回核心預設 FIFO */
}
```

Go 在每次 publish 一個 task 時寫 `core_vtime`（C14 說明如何算），寫入前後把 `seq` 加 1 形成 seqcount。

### A2.3 pressure 計時（`ops.cpu_release` / `cpu_acquire`）

```c
void BPF_STRUCT_OPS(ge_cpu_release, s32 cpu, struct scx_cpu_release_args *args)
{
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});

	if (s)
		s->released_at = bpf_ktime_get_ns();
	/* 有 cpu_release 時排程器必須自行退回 local DSQ 的任務 */
	scx_bpf_reenqueue_local();
}

void BPF_STRUCT_OPS(ge_cpu_acquire, s32 cpu, struct scx_cpu_acquire_args *args)
{
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});

	if (s && s->released_at) {
		s->displaced_ns += bpf_ktime_get_ns() - s->released_at;
		s->released_at = 0;
	}
}
```

PR 只能從「stopping 時 slice 還有剩」推估被 RT/DL 拿走的時間；sched\_ext 的 `cpu_release` 直接告訴我們，量得更準。IRQ 與 steal 由 Go 讀 `/proc/stat` 補上（C13）。

### A2.4 tick：cpufreq

```c
void BPF_STRUCT_OPS(ge_tick, struct task_struct *p)
{
	s32 cpu = scx_bpf_task_cpu(p);
	struct cpu_ctl *ctl = bpf_map_lookup_elem(&ctl_tbl, &(u32){cpu});
	struct cpu_slot *s = bpf_map_lookup_elem(&cpu_tbl, &(u32){cpu});
	u64 perf;

	if (!ctl || !s)
		return;
	perf = READ_ONCE(ctl->perf_target);
	if (perf && perf != s->perf_applied) {
		scx_bpf_cpuperf_set(cpu, perf);
		s->perf_applied = perf;
	}
}
```

### A2.5 cgroup 回呼 → 事件

```c
static __always_inline u64 cg_id(struct cgroup *cg) { return cg ? cg->kn->id : 0; }

static __always_inline u64 cg_parent_id(struct cgroup *cg)
{
	struct cgroup *p;
	u64 id;

	if (!cg->level || !(p = bpf_cgroup_ancestor(cg, cg->level - 1)))
		return 0;
	id = p->kn->id;
	bpf_cgroup_release(p);
	return id;
}

static void ev_cgrp(u32 kind, struct cgroup *cg, u64 a0, u64 a1, u64 a2, u64 a3)
{
	struct ev *e = ev_reserve(kind);

	if (!e)
		return;
	e->cgid = cg_id(cg);
	e->arg[0] = a0; e->arg[1] = a1; e->arg[2] = a2; e->arg[3] = a3;
	bpf_ringbuf_submit(e, 0);
}

s32 BPF_STRUCT_OPS_SLEEPABLE(ge_cgroup_init, struct cgroup *cg, struct scx_cgroup_init_args *args)
{
	u64 period = 0, quota = 0, burst = 0;

	if (bpf_core_field_exists(args->bw_quota_us)) {
		period = args->bw_period_us;
		quota = args->bw_quota_us;
		burst = args->bw_burst_us;
	}
	ev_cgrp(EV_CGRP_INIT, cg, cg_parent_id(cg), args->weight, period, quota);
	if (burst)
		ev_cgrp(EV_CGRP_BW, cg, period, quota, burst, 0);
	return 0;
}

void BPF_STRUCT_OPS(ge_cgroup_exit, struct cgroup *cg) { ev_cgrp(EV_CGRP_EXIT, cg, 0, 0, 0, 0); }
void BPF_STRUCT_OPS(ge_cgroup_set_weight, struct cgroup *cg, u32 w) { ev_cgrp(EV_CGRP_WEIGHT, cg, w, 0, 0, 0); }
void BPF_STRUCT_OPS(ge_cgroup_set_idle, struct cgroup *cg, bool idle) { ev_cgrp(EV_CGRP_IDLE, cg, idle, 0, 0, 0); }
void BPF_STRUCT_OPS(ge_cgroup_set_bandwidth, struct cgroup *cg, u64 period_us, u64 quota_us, u64 burst_us)
{
	ev_cgrp(EV_CGRP_BW, cg, period_us, quota_us, burst_us, 0);
}

void BPF_STRUCT_OPS(ge_cgroup_move, struct task_struct *p, struct cgroup *from, struct cgroup *to)
{
	struct ev *e = ev_reserve(EV_CGRP_MOVE);

	if (!e)
		return;
	ev_task(e, p, bpf_task_storage_get(&task_ctxs, p, 0, 0));
	e->arg[0] = cg_id(from);
	e->arg[1] = cg_id(to);
	bpf_ringbuf_submit(e, 0);
}
```

`cgroup_set_idle`、`cgroup_set_bandwidth` 與 `scx_cgroup_init_args.bw_*` 只在較新的核心存在：載入前由 Go 用 BTF 檢查 `sched_ext_ops` 成員，缺的就把 struct\_ops 對應欄位設成 NULL（libbpf `bpf_map__set_autoattach` 不適用，改在 skeleton open 後把 `skel->struct_ops.goeevdf->cgroup_set_idle = NULL`），並開啟 Go 端 cgroupfs 輪詢（C11、C12）。

### A2.6 心跳、init、ops 宣告

```c
static int heartbeat_fn(void *map, int *key, struct bpf_timer *t)
{
	s32 cpu;

	set_usersched_needed();          /* 讓 Go 跑週期工作 */
	if ((cpu = scx_bpf_pick_idle_cpu(scx_bpf_get_idle_cpumask(), 0)) >= 0)
		scx_bpf_kick_cpu(cpu, SCX_KICK_IDLE);
	bpf_timer_start(t, NSEC_PER_MSEC, 0);
	return 0;
}

s32 BPF_STRUCT_OPS_SLEEPABLE(ge_init)
{
	/* 1. scx_bpf_create_dsq(SCHED_DSQ, -1)
	 * 2. 每顆 CPU 初始化 hrticks[cpu] 的 timer 與 callback
	 * 3. 啟動 heartbeat timer（1 ms），照 qumun usersched_timer_init() */
	return 0;
}

void BPF_STRUCT_OPS(ge_dump_task, struct scx_dump_ctx *dctx, struct task_struct *p)
{
	/* 讀 task_tbl[slot]：印 vruntime、deadline、lag、weight，對應 debug.rs */
}

SCX_OPS_DEFINE(goeevdf,
	.select_cpu = (void *)ge_select_cpu,     .enqueue = (void *)ge_enqueue,
	.dispatch = (void *)ge_dispatch,         .tick = (void *)ge_tick,
	.running = (void *)ge_running,           .stopping = (void *)ge_stopping,
	.quiescent = (void *)ge_quiescent,       .yield = (void *)ge_yield,
	.set_cpumask = (void *)ge_set_cpumask,   .update_idle = (void *)ge_update_idle,
	.cpu_acquire = (void *)ge_cpu_acquire,   .cpu_release = (void *)ge_cpu_release,
	.core_sched_before = (void *)ge_core_sched_before,
	.init_task = (void *)ge_init_task,       .exit_task = (void *)ge_exit_task,
	.cgroup_init = (void *)ge_cgroup_init,   .cgroup_exit = (void *)ge_cgroup_exit,
	.cgroup_move = (void *)ge_cgroup_move,   .cgroup_set_weight = (void *)ge_cgroup_set_weight,
	.cgroup_set_idle = (void *)ge_cgroup_set_idle,
	.cgroup_set_bandwidth = (void *)ge_cgroup_set_bandwidth,
	.dump_task = (void *)ge_dump_task,
	.init = (void *)ge_init,                 .exit = (void *)ge_exit,
	.flags = SCX_OPS_ENQ_LAST | SCX_OPS_KEEP_BUILTIN_IDLE | SCX_OPS_HAS_CGROUP_WEIGHT,
	.timeout_ms = 5000,
	.name = "goeevdf");
```

`ge_init_task` 只建立 task storage（`BPF_LOCAL_STORAGE_GET_F_CREATE`）；新任務第一次 enqueue 時 Go 才認識它，fork 由 `wake_flags & SCX_WAKE_FORK` 辨識。核心需求：sched\_ext（6.12+）、`BPF_F_TIMER_ABS`（6.7+）、`ops.core_sched_before` 需 CONFIG\_SCHED\_CORE。

## Part B：Go 綁定層（`internal/bridge`、`internal/bpf`）

新 module `github.com/<org>/scx_goeevdf`。綁定層三個 package：`internal/bridge` 整包複製 qumun 的 `internal/schedbridge`（`Call`、`CallSyscall`、ABI 斷言慣例）；`internal/bpf` 是 cgo，包 skeleton、ring、指令與 syscall；`eevdf/` 完全不碰 cgo。

### B1. 檔案

| 檔案 | 內容 | 借用自 |
| --- | --- | --- |
| `internal/bpf/skel.c`、`skel.h` | `ge_open()`、rodata setter、`ge_disable_op(name)`、`ge_load()`、`ge_attach()`、map fd getter | qumun `wrapper.c` |
| `internal/bpf/hot.c`、`hot.h` | `ge_ev_open/close`、`ge_ev_consume_one`（128 B）、`ge_cmd_open/close`、`ge_cmd_submit`（64 B）、`ge_kick`（BPF\_PROG\_TEST\_RUN） | qumun `hotpath.c` |
| `internal/bpf/wire.go` | `Ev`、`Cmd`、`CpuSlot`、`CpuCtl`、`TaskPub` 與雙向斷言 | qumun `hotpath.go` |
| `internal/bpf/tables.go` | 用 `unix.Mmap` 映射三張 `BPF_F_MMAPABLE` 表 | 新 |
| `internal/bpf/sched.go` | `Open`、`Load`、`Attach`、`Close`、`Consume`、`Submit`、`Kick`、BSS 計數器 | qumun `obj.go`、`bss.go` |
| `internal/bpf/btf.go` | 檢查 `sched_ext_ops` 是否有 `cgroup_set_idle` 等成員 | qumun `util/emun.go`（cilium/ebpf btf） |

### B2. `wire.go`

```go
package bpf

// #include "hot.h"
import "C"
import "unsafe"

const (
	MaxCPUs  = 1024
	MaxTasks = 65536
)

type Ev struct { // = struct ev，128 byte
	Kind                    uint32
	Pid, Tgid, Cpu          int32
	Flags, Now, SumExec     uint64
	Weight, CustomSlice     uint64
	Cgid                    uint64
	WakerPid, WakerCpu      int32
	WakeFlags, Policy       uint32
	NrCpusAllowed, Slot     uint32
	EnqSeq                  uint64
	Arg                     [4]uint64
}

type Cmd struct { // = struct cmd，64 byte
	Kind        uint32
	Pid, Cpu    int32
	Cflags      uint32
	Slice       uint64
	EnqFlags    uint64
	EnqSeq      uint64
	Slot        uint32
	ExpectCurr  int32
	KickFlags   uint64
	_           uint64
}

type CpuSlot struct { // BPF 寫
	CurrPid, Idle             uint32
	RunStart, RunSeq          uint64
	DisplacedNs, ReleasedAt   uint64
	Keeps, PerfApplied, _     uint64
}

type CpuCtl struct { // Go 寫
	NrQueued, KeepSlice, PerfTarget uint64
	_                               [5]uint64
}

type TaskPub struct { // Go 寫，seqcount
	Seq                         uint64
	Pid                         int32
	_                           uint32
	CoreVtime, Vruntime, Deadline uint64
	Lag                         int64
	Weight                      uint64
}
```

欄位順序與 A1.1 的 C struct 完全相同。每個 struct 都加 qumun 式的成對斷言（size、alignment、每個欄位 offset）：

```go
var (
	_ [unsafe.Sizeof(Ev{}) - C.sizeof_struct_ev]byte
	_ [C.sizeof_struct_ev - unsafe.Sizeof(Ev{})]byte
	_ [unsafe.Offsetof(Ev{}.SumExec) - C.EV_OFF_SUM_EXEC]byte
	_ [C.EV_OFF_SUM_EXEC - unsafe.Offsetof(Ev{}.SumExec)]byte
	// ... 每個欄位一對；C.EV_OFF_* 由 hot.h 的 enum { EV_OFF_X = offsetof(struct ev, x) } 產生
)
```

`hot.h` 直接 `#include "../../bpf/intf.h"` 前先定義 `u32/s32/u64/s64` 為 `<stdint.h>` 型別（避免 qumun 遇到的 typedef 衝突），讓 BPF 與 Go 共用同一份 struct 定義。

### B3. 熱路徑 C（`hot.c`）

```c
struct ge_ev_ring { struct ring_buffer *rb; struct ev last; };

static int copy_ev(void *ctx, void *data, size_t size)
{
	struct ge_ev_ring *r = ctx;

	if (size != sizeof(r->last))
		return -EMSGSIZE;
	memcpy(&r->last, data, size);
	return 0;
}

struct ge_consume_args { struct ge_ev_ring *ring; int32_t result; int32_t pad; struct ev out; };

void ge_ev_consume_one(void *opaque)   /* 經 bridge.Call：有界、不阻塞 */
{
	struct ge_consume_args *a = opaque;

	a->result = ring_buffer__consume_n(a->ring->rb, 1);
	if (a->result == 1)
		memcpy(&a->out, &a->ring->last, sizeof(a->out));
}

struct ge_submit_args { struct user_ring_buffer *ring; int32_t error; int32_t pad; struct cmd cmd; };

void ge_cmd_submit(void *opaque)
{
	struct ge_submit_args *a = opaque;
	void *slot = user_ring_buffer__reserve(a->ring, sizeof(a->cmd)); /* 絕不用 reserve_blocking */

	if (!slot) {
		a->error = -errno;
		return;
	}
	memcpy(slot, &a->cmd, sizeof(a->cmd));
	user_ring_buffer__submit(a->ring, slot);
	a->error = 0;
}

struct ge_kick_args { int32_t prog_fd; int32_t error; struct { int32_t cpu; uint32_t pad; uint64_t flags; } ctx; };

void ge_kick(void *opaque)  /* SEC("syscall") do_kick；qumun c_select_cpu_wrapper 同款 */
{
	struct ge_kick_args *a = opaque;
	union bpf_attr attr = {};

	attr.test.prog_fd = a->prog_fd;
	attr.test.ctx_in = (uintptr_t)&a->ctx;
	attr.test.ctx_size_in = sizeof(a->ctx);
	a->error = syscall(__NR_bpf, BPF_PROG_TEST_RUN, &attr, sizeof(attr)) < 0 ? -errno : 0;
}
```

BPF 端對應的 syscall 程式：

```c
struct kick_arg { s32 cpu; u32 pad; u64 flags; };
SEC("syscall") int do_kick(struct kick_arg *a) { scx_bpf_kick_cpu(a->cpu, a->flags); return 0; }
```

### B4. `sched.go`：Go API

```go
type Sched struct {
	skel   unsafe.Pointer
	ev     *C.struct_ge_ev_ring
	cmd    *C.struct_user_ring_buffer
	kickFd int32
	CPU    []CpuSlot // mmap：BPF 寫
	Ctl    []CpuCtl  // mmap：Go 寫
	Task   []TaskPub // mmap：Go 寫
	cnt    struct{ nrQueued, nrScheduled, evDrops, congested, stale, bounces *uint64 }
}

func Open() (*Sched, error)                       // ge_open()
func (s *Sched) SetUserschedTgid(tgid uint32)     // rodata，Load 前
func (s *Sched) DisableOp(name string)            // 舊核心缺的 cgroup_set_idle 等
func (s *Sched) Load() error                      // ge_load()，之後 mmap 三張表、快取 BSS 位址
func (s *Sched) Attach() error
func (s *Sched) Close()                           // 排程 goroutine 結束後才可呼叫

// Consume 非阻塞取一筆；只在 LockOSThread 的排程 goroutine 呼叫。
func (s *Sched) Consume(out *Ev) (bool, error) {
	args := consumeArgs{ring: unsafe.Pointer(s.ev)}
	bridge.Call(consumeFn, unsafe.Pointer(&args))
	runtime.KeepAlive(s)
	switch {
	case args.result == 1:
		*out = args.out
		return true, nil
	case args.result == 0:
		return false, nil
	}
	return false, fmt.Errorf("consume events: %w", syscall.Errno(-args.result))
}

func (s *Sched) Submit(c *Cmd) error             // ge_cmd_submit；ENOSPC 原樣回傳
func (s *Sched) Kick(cpu int32, flags uint64) error // ge_kick，需要立即生效時用
func (s *Sched) DecNrQueued(n uint64)            // 條件式 atomic 減，同 qumun SubUint64
func (s *Sched) SetNrScheduled(n uint64)         // atomic.StoreUint64
func (s *Sched) Yield() error                    // unix.Syscall(SYS_SCHED_YIELD)
```

`Consume` 直接把 C 的 `struct ev` 複製進 Go 的 `Ev`（版面已斷言相同），不需要 qumun 的 `binary.LittleEndian` 逐欄解碼。

### B5. 共享表（`tables.go`）

```go
func mmapArray[T any](fd int, n int) ([]T, error) {
	size := int(unsafe.Sizeof(*new(T))) * n
	size = (size + os.Getpagesize() - 1) &^ (os.Getpagesize() - 1)
	b, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return unsafe.Slice((*T)(unsafe.Pointer(&b[0])), n), nil
}
```

存取規則：BPF 寫的欄位 Go 用 `atomic.Load*`；Go 寫的欄位用 `atomic.Store*`。`TaskPub` 多欄位寫入用 seqcount：`atomic.AddUint64(&tp.Seq, 1)` → 寫欄位 → `atomic.AddUint64(&tp.Seq, 1)`。mmap 記憶體不在 Go heap，GC 不會掃描或搬動，可長期持有 slice。

### B6. 時鐘與 affinity

- Go 取時間：`unix.ClockGettime(unix.CLOCK_MONOTONIC)`，與 `bpf_ktime_get_ns()` 同源。
- cpumask：收到 `EV_CPUMASK` 或第一次見到任務時 `unix.SchedGetaffinity(pid, &set)`；`nr_cpus_allowed` 與快取不符時重讀。

### B7. 綁定層測試

- 每個 wire struct 的斷言編譯通過；`TestEvLayout` 用 cgo 填 C 端 `struct ev` 再轉 Go 比對。
- privileged：Load 不 Attach，用一支測試用 `SEC("syscall")` 往 `events` reserve 一筆、往 `cmds` 讀一筆，驗證 `Consume`/`Submit` 往返與 `ENOSPC`。
- `go test -race ./internal/...` 與 qumun 一樣加 `-ldflags=-checklinkname=0`。

## Part C：Go 端 EEVDF 核心（新 package `eevdf/`，純 Go、無 cgo）

`eevdf/` 不 import cgo 或 `internal/bpf`，對外只靠 `IO` 介面（C4），所以全部可用假 IO 在一般使用者權限下 `go test`。每個檔案開頭的註解要像 PR 一樣寫出對應的 fair.c 函式名稱。

| 檔案 | 內容 | 對應 PR |
| --- | --- | --- |
| `accounting.go` | 純算術（C1、C1b） | accounting.rs |
| `tree.go` | EDQ 增強 treap（C2） | scx\_rustsched dsq::Edq |
| `topo.go` | CPU / core / cluster / LLC / node、capacity、packing tier、sched domain 層級 | scx\_rustsched\_loader 拓撲 |
| `task.go`、`pack.go` | 任務狀態、per-CPU pack（C3） | taskctx.rs、pack.rs |
| `policy.go` | 事件分派、pick、keep\_running、yield、policy（C4） | main.rs |
| `preempt.go` | wakeup preemption、保護（C5） | preempt.rs |
| `placement.go` | select\_task\_rq\_fair（C6） | idle.rs |
| `load.go` | PELT 近似、util\_est、CPU load（C7） | load.rs |
| `newidle.go` | idle pull（C8） | newidle.rs |
| `balance.go` | 週期 balance（C9） | balance.rs、balance\_state.rs |
| `active.go` | active / misfit / asym（C10） | active.rs |
| `cgroup.go` | 有效權重、group shares（C11） | cgroup.rs |
| `bandwidth.go` | cpu.max（C12） | bandwidth.rs |
| `pressure.go`、`cpufreq.go` | 可用容量、頻率目標（C13） | pressure.rs、load.rs |
| `hrtick.go`、`coresched.go`、`debug.go`、`features.go` | C14 | hrtick.rs、main.rs、debug.rs、features.rs |

算術一律 `uint64` 加 wrap 比較，與 Rust 的 `wrapping_*` 和 `as i64` 一一對應：Go 的整數溢位本來就是 wrap，`int64(a-b)` 等於 Rust 的 `a.wrapping_sub(b) as i64`。

### C1. `accounting.go`

```go
package eevdf

const (
	Nice0Weight uint64 = 1024
	MinDLWeight uint64 = Nice0Weight / 4
)

// LoadWeight 把 p->scx.weight（1..10000，100 = nice 0）換成 fair.c 刻度。
func LoadWeight(scx uint64) uint64 { return max(scx*Nice0Weight/100, 1) }

func scaleByDLWeight(request, weight uint64) uint64 {
	return request * Nice0Weight / max(weight, MinDLWeight)
}

func TimeBefore(a, b uint64) bool { return int64(a-b) < 0 }

// vdiv：有號值除以無號數，向零截斷；除數 0 回 0。
func vdiv(v int64, d uint64) int64 {
	if d == 0 {
		return 0
	}
	if v < 0 {
		return -int64(uint64(-v) / d) // uint64(-MinInt64) 正好是 1<<63
	}
	return int64(uint64(v) / d)
}

type Request struct{ Deadline, Slice, Weight uint64 }

// Prepare = update_deadline()：上一個 request 用完才重發；initial 只要一半。
func (r *Request) Prepare(vtime, slice, weight uint64, initial bool) bool {
	weight = max(weight, MinDLWeight)
	pending := r.Slice != 0 && int64(vtime-r.Deadline) < 0
	issued := r.Slice != slice || !pending
	if issued {
		req := slice
		if initial {
			req = slice / 2
		}
		r.Deadline = vtime + scaleByDLWeight(req, weight)
	} else if r.Weight != weight {
		r.Deadline = vtime + (r.Deadline-vtime)*r.Weight/weight
	}
	r.Slice, r.Weight = slice, weight
	return issued
}

// Rebase：vruntime 從 old 搬到 new，未完成的 request 保留相對 deadline。
func (r *Request) Rebase(old, new uint64) {
	if r.Slice != 0 && int64(old-r.Deadline) < 0 {
		r.Deadline = new + (r.Deadline - old)
	} else {
		r.Slice = 0
	}
}

func (r *Request) Sleep() { r.Slice = 0 }

func LagLimit(request, weight uint64) int64 { return int64(scaleByDLWeight(request, weight)) }

func Lag(ref, vtime uint64, limit int64) int64 {
	return min(max(int64(ref-vtime), -limit), limit)
}

// V' = V + w(v-V)/(W+w)
func joinDelta(ref, sum, vtime, w uint64) uint64 {
	return uint64(vdiv(int64(w)*int64(vtime-ref), sum+w))
}

// V' = V + w(V-v)/(W-w)；sum 仍包含離開者。
func leaveDelta(ref, sum, vtime, w uint64) uint64 {
	if sum <= w {
		return 0
	}
	return uint64(vdiv(int64(w)*int64(ref-vtime), sum-w))
}

// place：讓任務加入後 lag 正好是 lag（PLACE_LAG 的 (W+w)/W 放大）。
func place(ref, sum uint64, lag int64, w uint64) uint64 {
	if sum != 0 {
		lag = vdiv(lag*int64(sum+w), sum)
	}
	return ref - uint64(lag)
}

// fold：dV = w·dv/W，remainder 帶著截斷掉的部分。
func fold(w, distance, sum, rem uint64) (advance, rest uint64) {
	if sum == 0 {
		return 0, rem
	}
	acc := w*distance + rem
	advance = acc / sum
	return advance, acc - advance*sum
}

func advance(vtime, delta, w uint64) uint64 { return vtime + delta*Nice0Weight/max(w, 1) }

// delayDebt：DELAY_DEQUEUE 的剩餘負債（Phase 2）。
func delayDebt(lag, adv int64, sum, w uint64) int64 {
	if lag >= 0 {
		return 0
	}
	if adv <= 0 {
		return lag
	}
	return min(lag+vdiv(adv*int64(sum), sum+w), 0)
}

// BaseSlice = fair.c sysctl_sched_base_slice：700us × (1 + ilog2(min(ncpu, 8)))。
func BaseSlice(ncpu int) uint64 {
	f := uint64(4)
	switch {
	case ncpu <= 1:
		f = 1
	case ncpu <= 3:
		f = 2
	case ncpu <= 7:
		f = 3
	}
	return f * 700_000
}
```

溢位邊界：`int64(w)*distance` 中 w 最大約 102400（nice -20），distance 被 lag limit 夾住（數 ms 等級），遠小於 9.2e18。若日後要支援極端權重，改用 `math/bits.Mul64` 做 128 bit 乘法。

### C1b. `accounting.go` 其餘函式（逐一移植 accounting.rs）

```go
// reference_at：pack 的 V，投影 curr 已跑未 charge 的 delta ns。
func referenceAt(ref, sum, w, delta, request uint64) uint64 {
	if w == 0 || sum == 0 || delta >= request {
		return ref
	}
	dist := delta * Nice0Weight / w
	return ref + dist*w/sum
}

// reference_before_join：放置 join 權重任務時用的 V，避免截斷讓 0-lag 任務變 ineligible。
func referenceBeforeJoin(ref, sum, w, delta, request, join uint64) uint64 {
	total := sum + join
	if w == 0 || sum == 0 || total == 0 || delta >= request {
		return ref
	}
	proj := (delta * Nice0Weight / w) * w / total
	return ref + proj*total/sum
}

const UtilHalfLifeNs = 32_000_000

func utilEstUpdate(est, dequeued uint64) uint64 {
	if est <= dequeued {
		return dequeued
	}
	return est - (est-dequeued)>>2
}

func runnableUpdate(est, cycle, runnable uint64) uint64 {
	if cycle == 0 {
		return est
	}
	f := uint64(1024)
	if runnable < cycle {
		f = runnable * 1024 / cycle
	}
	return (est*7 + f) / 8
}

func taskLoad(w, utilEst, runnableEst, asleep uint64) uint64 {
	shift := min(asleep/UtilHalfLifeNs, 63)
	util := max(utilEst, runnableEst) >> shift
	return w * min(util, 1024) / 1024
}

func wakeWide(wakerFlips, wakeeFlips, factor uint64) bool {
	master, slave := wakerFlips, wakeeFlips
	if wakerFlips < wakeeFlips {
		master, slave = wakeeFlips, wakerFlips
	}
	return slave >= factor && master >= slave*factor
}

// wake_affine_weight，等容量 CPU 之間；wakerLoad < 0 代表非 sync 喚醒。
func wakeAffineWeight(thisLoad, prevLoad uint64, wakerLoad int64, tload uint64, sameCore bool) bool {
	thisEff := int64(thisLoad)
	if wakerLoad >= 0 {
		if uint64(wakerLoad) > thisLoad {
			return true
		}
		thisEff -= wakerLoad
	}
	thisEff = (thisEff + int64(tload)) * 100
	pct := int64(100 + (117-100)/2)
	if sameCore {
		pct = 100 + (110-100)/2
	}
	prevEff := (int64(prevLoad) - int64(tload)) * pct
	if wakerLoad >= 0 {
		prevEff++
	}
	return thisEff < prevEff
}

type ForkGroup struct {
	Base, Nr, Idle        uint32
	Load, Util, Cap       uint64
	Recent                uint64
	Runnable, Allowed     uint32
	Local                 bool
}

// forkGroupWins = fork_group_wins()，find_idlest_group() 的排序。逐行照 Rust 移植：
// 先比 spare capacity（restricted）、再比 idle 數、再比 util 或 load/cap（含 117%/125% 邊際）、
// 再以 recent 打破同分，local 群組平手保留。
func forkGroupWins(g ForkGroup, best *ForkGroup, restricted, coreLevel bool) bool {
	if g.Nr == 0 || g.Allowed == 0 {
		return false
	}
	if best == nil {
		return true
	}
	spare := restricted && g.Runnable < g.Allowed
	bestSpare := restricted && best.Runnable < best.Allowed
	if spare != bestSpare {
		return spare
	}
	if g.Idle != best.Idle {
		return g.Idle > best.Idle
	}
	if spare {
		if best.Local {
			return false
		}
		if g.Local {
			return true
		}
		if g.Util != best.Util {
			return g.Util < best.Util
		}
	} else {
		load, bestLoad := g.Load*best.Cap, best.Load*g.Cap
		if restricted && best.Local != g.Local {
			pct := uint64(125)
			if coreLevel {
				pct = 117
			}
			if best.Local {
				return bestLoad*100 > load*pct
			}
			return load*100 <= bestLoad*pct
		}
		if load != bestLoad {
			return load < bestLoad
		}
		if !restricted && coreLevel && g.Recent != best.Recent {
			return g.Recent < best.Recent
		}
		if best.Local {
			return false
		}
		if g.Local {
			return true
		}
	}
	return coreLevel && g.Recent < best.Recent
}

const NewidleDecayNs = 1_000_000_000

func avgIdleUpdate(avg, idle, maxCost uint64) uint64 {
	if idle > avg {
		avg += (idle - avg) / 8
	} else {
		avg -= (avg - idle) / 8
	}
	return min(avg, 2*maxCost)
}

func newidleCostUpdate(record, stamp, cost, now uint64) (uint64, uint64) {
	switch {
	case cost > record:
		return cost, now
	case TimeBefore(stamp+NewidleDecayNs, now):
		return record * 253 / 256, now
	}
	return record, stamp
}

func newidleStatsFold(calls, succ, stamp, ratio, now uint64) (uint64, uint64, uint64, uint64) {
	if calls < 1024 {
		return calls, succ, stamp, ratio
	}
	var delta uint64
	if !TimeBefore(now, stamp) {
		delta = now - stamp
	}
	return calls / 2, succ / 2, now, min((delta>>22)+succ, 1024)
}

// newidleSample：NI_RANDOM；ok=false 代表這次不掃。weight 是成功時的加權。
func newidleSample(ratio uint64, random uint32) (weight uint64, ok bool) {
	if ratio >= 1024 {
		return 1, true
	}
	sample := 1 + ratio
	if uint64(random&1023) > sample {
		return 0, false
	}
	return (1024 + sample/2) / sample, true
}

const (
	GroupWeightShift = 10
	GroupMinShares   = 2
)

func cgroupLoadWeight(w uint64) uint64          { return max((w*Nice0Weight+50)/100, 1) }
func propWeight(fixed, shares, load uint64) uint64 { return fixed * shares / max(load, 1) }
func roundWeight(fixed uint64) uint64 {
	return max((fixed+(1<<(GroupWeightShift-1)))>>GroupWeightShift, 1)
}

// groupShares = calc_concur_shares()：fair.c 預設 concur 模式。
func groupShares(weight, tasks, cpus, load, loadAvg, tgLoadAvg, contrib uint64) uint64 {
	total := min(max(tasks, 1), max(cpus, 1)) * weight
	load = max(load, loadAvg)
	tgLoad := satSub(tgLoadAvg, contrib) + load
	shares := total
	if tgLoad != 0 {
		shares = total * load / tgLoad
	}
	return min(max(shares, GroupMinShares), total)
}

func satSub(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return 0
}
```

`forkGroupWins` 是 accounting.rs 的 `fork_group_wins` 逐行翻譯；移植 `tests/accounting.rs` 裡所有 `fork_*` 測試驗證。

### C2. `tree.go`：EDQ（eligible deadline queue）

依 `(deadline, seq)` 排序；每個節點維護子樹最小 vruntime，`PickEligible(V)` 一次下降找到 deadline 最早的 eligible 節點，等同 fair.c `__pick_eevdf()`。任務在樹內時不可修改 deadline 或 vruntime（先 Remove、改完再 Insert）。

```go
package eevdf

import "math/rand/v2"

type node struct {
	t           *Task
	prio        uint64
	left, right *node
	minV        uint64 // 子樹最小 vruntime（wrap 比較）
	size        int
}

type Tree struct{ root *node }

func less(a, b *Task) bool {
	if a.Req.Deadline != b.Req.Deadline {
		return TimeBefore(a.Req.Deadline, b.Req.Deadline)
	}
	return a.seq < b.seq
}

func (n *node) update() {
	n.minV, n.size = n.t.Vruntime, 1
	for _, c := range [2]*node{n.left, n.right} {
		if c != nil {
			if TimeBefore(c.minV, n.minV) {
				n.minV = c.minV
			}
			n.size += c.size
		}
	}
}

func rotR(n *node) *node { l := n.left; n.left = l.right; l.right = n; n.update(); l.update(); return l }
func rotL(n *node) *node { r := n.right; n.right = r.left; r.left = n; n.update(); r.update(); return r }

func insert(n *node, x *node) *node {
	if n == nil {
		x.update()
		return x
	}
	if less(x.t, n.t) {
		n.left = insert(n.left, x)
		if n.left.prio > n.prio {
			return rotR(n)
		}
	} else {
		n.right = insert(n.right, x)
		if n.right.prio > n.prio {
			return rotL(n)
		}
	}
	n.update()
	return n
}

func remove(n *node, t *Task) *node {
	if n == nil {
		return nil
	}
	switch {
	case n.t == t:
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		if n.left.prio > n.right.prio {
			n = rotR(n)
			n.right = remove(n.right, t)
		} else {
			n = rotL(n)
			n.left = remove(n.left, t)
		}
	case less(t, n.t):
		n.left = remove(n.left, t)
	default:
		n.right = remove(n.right, t)
	}
	n.update()
	return n
}

func (tr *Tree) Insert(t *Task) {
	t.node = &node{t: t, prio: rand.Uint64()}
	tr.root = insert(tr.root, t.node)
}

func (tr *Tree) Remove(t *Task) {
	if t.node != nil {
		tr.root = remove(tr.root, t)
		t.node = nil
	}
}

func (tr *Tree) Len() int {
	if tr.root == nil {
		return 0
	}
	return tr.root.size
}

// Min：deadline 最早者（不看 eligibility）。
func (tr *Tree) Min() *Task {
	n := tr.root
	if n == nil {
		return nil
	}
	for n.left != nil {
		n = n.left
	}
	return n.t
}

// PickEligible：vruntime <= V 的任務中 deadline 最早者；沒有則 nil。
func (tr *Tree) PickEligible(V uint64) *Task {
	elig := func(v uint64) bool { return !TimeBefore(V, v) }
	for n := tr.root; n != nil; {
		if n.left != nil && elig(n.left.minV) {
			n = n.left
			continue
		}
		if elig(n.t.Vruntime) {
			return n.t
		}
		n = n.right
	}
	return nil
}
```

正確性論證（寫進註解）：中序即 deadline 序；若左子樹有任何 eligible，答案必在左；否則若自己 eligible 就是答案；否則只可能在右子樹。

### C3. `task.go`、`pack.go`：任務與 per-CPU pack

每顆 CPU 一個 `Pack` 與一棵 `Tree`。任務是某個 pack 的成員，當它在該 CPU 的 EDQ 排隊、在 local DSQ 等待或正在那裡跑；離開只發生在 SLEEP、DEQUEUE、EXIT、搬到別的 CPU。這是 pack.rs 的單一 runqueue 設計，只是鎖與 atomics 全部省掉。

```go
package eevdf

type State uint8

const (
	Sleeping   State = iota
	Queued           // 在 Tree[cpu]
	Dispatched       // 在 local DSQ 或正在跑
	Parked           // cpu.max 節流中（C12）
)

type Task struct {
	Pid, Tgid   int32
	Slot        uint32 // task_tbl 槽
	EnqSeq      uint64 // BPF 最新序號，指令要回填
	Policy      uint32 // SCHED_NORMAL/BATCH/IDLE
	Nice        uint64 // LoadWeight(p->scx.weight)
	Weight      uint64 // 有效權重：Nice 經 cgroup 縮放（C11）
	CustomSlice uint64
	Allowed     CPUSet // sched_getaffinity
	Cgroup      *Group
	Cpu         int32 // 目前或最後的 CPU
	Vruntime    uint64
	Req         Request

	pack         *Pack // nil = 非成員
	joined       uint64
	joinedWeight uint64
	lag          int64
	delay        delayRec // DELAY_DEQUEUE
	initial      bool
	state        State
	seq          uint64
	node         *node
	lastSum      uint64 // 上次事件的 se.sum_exec_runtime
	lastEvNow    uint64
	ld           taskLoadState // C7
	wake         wakeState     // C6：flips、last_wakee
	migrateTo    int32         // active/pressure 要求的目的地，-1 = 無
	lastStopAt   uint64        // cache hot 判斷
}

type delayRec struct {
	pack           *Pack
	ref, sum, gen  uint64
}

type Pack struct {
	CPU       int32
	Ref, W    uint64
	remainder uint64
	emptyGen  uint64
	members   map[int32]*Task
	core      struct{ origin, gen uint64 } // C14 core_vtime
}
```

#### 投影 curr 未 charge 的服務（`--no-vref-update` 關掉）

```go
// uncharged：curr 自上次事件後在 c 上又跑了多久。run_start 由 BPF 寫。
func (p *Policy) uncharged(c int32, now uint64) (t *Task, delta uint64) {
	cur := &p.cpus[c]
	if cur.task == nil {
		return nil, 0
	}
	if p.io.CurrPid(c) != cur.task.Pid {
		return cur.task, 0
	}
	since := max(p.io.RunStart(c), cur.task.lastEvNow)
	if TimeBefore(since, now) {
		delta = now - since
	}
	return cur.task, delta
}

// refAt = pack_vref_at()：放置與 eligibility 前都用它。
func (p *Policy) refAt(pk *Pack, now uint64) uint64 {
	if !p.F.VrefUpdate {
		return pk.Ref
	}
	t, d := p.uncharged(pk.CPU, now)
	if t == nil || t.pack != pk {
		return pk.Ref
	}
	if len(pk.members) == 1 { // lone_reference()：唯一成員時 V 就是它
		return advance(t.Vruntime, d, t.joinedWeight)
	}
	return referenceAt(pk.Ref, pk.W, t.joinedWeight, d, t.Req.Slice)
}
```

#### join / leave / charge

```go
func (pk *Pack) join(t *Task, w uint64) {
	sum := pk.W
	ref := pk.Ref
	if len(pk.members) == 1 { // 只剩 curr：以它的 joined 當 V，丟掉截斷漂移
		for _, o := range pk.members {
			ref = o.joined
		}
	}
	pk.W += w
	pk.Ref = ref + joinDelta(ref, sum, t.Vruntime, w)
	t.pack, t.joined, t.joinedWeight = pk, t.Vruntime, w
	pk.members[t.Pid] = t
}

func (pk *Pack) leave(t *Task, emptied bool) {
	w, sum := t.joinedWeight, pk.W
	pk.W -= w
	if sum > w {
		pk.Ref += leaveDelta(pk.Ref, sum, t.joined, w)
	} else if emptied {
		pk.emptyGen++
	}
	delete(pk.members, t.Pid)
	t.pack = nil
}

// charge = update_curr()：delta 來自 se.sum_exec_runtime 差值（task clock）。
func (p *Policy) charge(t *Task, sumExec, now uint64) {
	delta := satSub(sumExec, t.lastSum)
	t.lastSum, t.lastEvNow = sumExec, now
	if delta == 0 {
		return
	}
	w := t.Weight
	if t.pack != nil {
		w = t.joinedWeight
	}
	t.Vruntime = advance(t.Vruntime, delta, w)
	p.bw.charge(t, delta, now) // C12
	pk := t.pack
	if pk == nil {
		return
	}
	dist := int64(t.Vruntime - t.joined)
	if dist <= 0 || pk.W == 0 {
		return
	}
	t.joined = t.Vruntime
	adv, rest := fold(t.joinedWeight, uint64(dist), pk.W, pk.remainder)
	pk.remainder, pk.Ref = rest, pk.Ref+adv
}
```

#### place：加入 pack（含跨 pack 搬移與 DELAY\_DEQUEUE 結清）

```go
// place = pack::place()；sleep=true 表示喚醒或新任務。回傳 false = 已是 dst 的成員。
func (p *Policy) place(t *Task, dst *Pack, sleep bool, now uint64) bool {
	if t.pack == dst {
		return false
	}
	w := p.cg.joinWeight(t, dst.CPU) // C11：cgroup 縮放後的有效權重
	var owed int64
	old := t.joinedWeight
	if t.pack == nil {
		owed = p.settle(t, w, now) // 存下的 lag，扣掉 delay 期間已償還的
	} else { // 搬移：lag 以來源 pack 為準
		ref := p.refAt(t.pack, now)
		owed = Lag(ref, t.Vruntime, LagLimit(p.lagReq(t), old))
		p.leaveAll(t, true)
	}
	if old != 0 && old != w {
		owed = vdiv(owed*int64(old), w) // reweight_entity()
	}
	ref := p.refFor(dst, now, w) // reference_before_join()
	var target uint64
	switch {
	case dst.W == 0:
		target = ref
	case p.F.PlaceLag:
		target = place(ref, dst.W, owed, w)
	default:
		target = place(ref, 0, owed, w)
	}
	if sleep || !p.F.PlaceRelDeadline {
		t.Req.Sleep()
	}
	t.Req.Rebase(t.Vruntime, target)
	t.Vruntime, t.lag = target, 0
	p.cg.enter(t, dst.CPU) // 加進 group queue 的 load
	dst.join(t, w)
	return true
}

func (p *Policy) refFor(pk *Pack, now, join uint64) uint64 {
	if !p.F.VrefUpdate {
		return pk.Ref
	}
	t, d := p.uncharged(pk.CPU, now)
	if t == nil || t.pack != pk {
		return pk.Ref
	}
	if len(pk.members) == 1 {
		return advance(t.Vruntime, d, t.joinedWeight)
	}
	return referenceBeforeJoin(pk.Ref, pk.W, t.joinedWeight, d, t.Req.Slice, join)
}

// quiesce = dequeue_entity()；呼叫前已 charge。
func (p *Policy) quiesce(t *Task, sleep bool, now uint64) {
	pk := t.pack
	if pk == nil {
		return
	}
	ref := p.refAt(pk, now)
	t.lag = Lag(ref, t.Vruntime, LagLimit(p.lagReq(t), t.joinedWeight))
	p.leaveAll(t, true)
	if sleep && t.lag < 0 && p.F.DelayDequeue {
		t.delay = delayRec{pk, pk.Ref, pk.W, pk.emptyGen}
	}
}

// settle：DELAY_DEQUEUE/DELAY_ZERO；pack 曾清空則債務免除。
func (p *Policy) settle(t *Task, w, now uint64) int64 {
	d := t.delay
	t.delay = delayRec{}
	if d.pack == nil {
		return t.lag
	}
	if d.pack.emptyGen != d.gen {
		return 0
	}
	prog := int64(p.refAt(d.pack, now) - d.ref)
	return delayDebt(t.lag, prog, d.sum, w)
}

// requeueCPU = delay_requeue_cid()：仍欠債就回原 CPU，不走 placement。
func (p *Policy) requeueCPU(t *Task, now uint64) (int32, bool) {
	d := t.delay
	if d.pack == nil || !p.F.DelayRequeue || d.pack.emptyGen != d.gen {
		return 0, false
	}
	prog := int64(p.refAt(d.pack, now) - d.ref)
	if delayDebt(t.lag, prog, d.sum, t.Weight) == 0 {
		return 0, false
	}
	return d.pack.CPU, t.Allowed.Has(d.pack.CPU)
}

// leaveAll：pack 與 cgroup queue 兩個成員資格一起放掉（pack::leave）。
func (p *Policy) leaveAll(t *Task, emptied bool) {
	p.cg.exit(t)
	t.pack.leave(t, emptied)
}

func (p *Policy) lagReq(t *Task) uint64 { return max(p.request(t), p.Slice) + p.TickNs }
func (p *Policy) request(t *Task) uint64 {
	if t.CustomSlice != 0 {
		return t.CustomSlice
	}
	return p.Slice
}
```

`reweight` = `reweight_eevdf()`，呼叫點是 nice 改變、cgroup shares 改變（C11）與 `running` 時群組權重變動：

```go
func (p *Policy) reweight(t *Task, w, now uint64) {
	pk := t.pack
	if pk == nil || w == 0 || w == t.joinedWeight {
		return
	}
	ref := p.refAt(pk, now)
	owed := vdiv(int64(ref-t.Vruntime)*int64(t.joinedWeight), w) // lag 以實際時間保留
	pk.leave(t, false)                                         // 不算清空：不免除債務
	target := ref - uint64(owed)
	if !p.F.PlaceRelDeadline {
		t.Req.Sleep()
	}
	t.Req.Rebase(t.Vruntime, target)
	t.Vruntime = target
	pk.join(t, w)
	t.Req.Prepare(t.Vruntime, p.request(t), w, false) // 以新權重縮放剩餘 request
}
```

### C4. `policy.go`：事件處理、pick、keep\_running、yield

#### IO 介面（`cmd/goeevdf` 以 `internal/bpf` 實作；測試用假實作）

```go
type IO interface {
	Now() uint64
	CurrPid(cpu int32) int32         // cpu_tbl.curr_pid
	RunStart(cpu int32) uint64        // cpu_tbl.run_start
	RunSeq(cpu int32) uint64
	Idle(cpu int32) bool
	Displaced(cpu int32) uint64       // cpu_tbl.displaced_ns
	SetQueued(cpu int32, n, keepSlice uint64) // ctl_tbl
	SetPerf(cpu int32, perf uint64)
	Publish(slot uint32, pub TaskPub) // task_tbl，內含 seqcount
	Affinity(pid int32) (CPUSet, error)
	Submit(c Cmd) error               // 指令 ring；ENOSPC 原樣回傳
	Kick(cpu int32, flags uint64) error
}

type Event struct{ /* 欄位與 bpf.Ev 一對一，由 cmd 轉換 */ }
type Cmd struct{ /* 欄位與 bpf.Cmd 一對一 */ }
```

#### Policy 狀態

```go
type CPU struct {
	task                                 *Task // Go 派到這顆 CPU、尚未回報離開的任務
	Vruntime, Deadline, Protect, Request uint64
	Weight                               uint64
	idle, sched_idle                     bool  // curr 是 SCHED_IDLE；CPU 只跑 SCHED_IDLE 工作
}

type Policy struct {
	F                  Features
	Slice, TickNs      uint64
	Enum               Enums // SCX_ENQ_WAKEUP/PREEMPT/REENQ、SCX_WAKE_FORK/SYNC，由 BTF 匯入
	io                 IO
	topo               *Topo
	packs              []*Pack
	trees              []Tree
	cpus               []CPU
	tasks              map[int32]*Task
	slots              slotAlloc // task_tbl 槽的 free list
	handoff, idleCPUs  []int32
	seq                uint64
	ld  *Load; nb *Newidle; bal *Balance; act *Active
	cg  *Cgroups; bw *Bandwidth; pr *Pressure; hr *Hrtick
	Stats Stats
}
```

#### 事件分派

```go
func (p *Policy) Handle(e *Event) {
	switch e.Kind {
	case EvEnqueue:
		p.onEnqueue(e)
	case EvSleep, EvDequeue:
		p.onLeave(e, e.Kind == EvSleep)
	case EvExit:
		p.onExit(e)
	case EvKeep:
		p.onKeep(e)
	case EvIdle:
		p.idleCPUs = append(p.idleCPUs, e.Cpu)
	case EvYield:
		p.onYield(e)
	case EvCpumask:
		p.onCpumask(e)
	default:
		p.cg.handle(e) // EV_CGRP_*（C11、C12）
	}
}

func (p *Policy) onEnqueue(e *Event) {
	now := e.Now
	t := p.task(e) // 新任務：配 slot、讀 affinity、initial=true
	if t.state == Queued {
		p.trees[t.Cpu].Remove(t)
	}
	p.charge(t, e.SumExec, now)
	wake := e.Flags&p.Enum.EnqWakeup != 0
	fork := e.WakeFlags&p.Enum.WakeFork != 0
	if wake && t.pack != nil { // 漏了 SLEEP：補做
		p.quiesce(t, true, now)
		p.Stats.MissedSleep++
	}
	from := e.Cpu
	wasCurr := p.isCurr(t, from)
	if wasCurr {
		p.clearCurr(from)
	}
	if nice := LoadWeight(e.Weight); nice != t.Nice { // nice 改變
		t.Nice = nice
		p.reweight(t, p.cg.weight(t), now)
	}
	if wake {
		p.ld.taskWoke(t, now)
	}
	if p.bw.throttled(t, now) { // cpu.max：進 backlog，不排隊（C12）
		p.bw.park(t, now)
		p.after(from, wasCurr, wake)
		return
	}
	dst := from
	switch {
	case t.migrateTo >= 0: // active balance / pressure 指定的目的地
		dst, t.migrateTo = t.migrateTo, -1
	case wake || fork || t.pack == nil:
		dst = p.selectCPU(t, e, fork) // C6
	case e.Flags&p.Enum.EnqReenq != 0: // cpu_release 退回：pressure 決定是否換 CPU
		dst = p.pr.target(t, from, now)
	}
	p.place(t, p.packs[dst], wake || t.initial, now)
	p.prepareRequest(t)
	// keep_running()：slice 到期回來，仍 eligible 且無更早 deadline → 平手也留在原 CPU
	if wasCurr && !wake && dst == from && p.keepRunning(t, from, now) {
		p.dispatch(t, from, false)
		return
	}
	p.enqueueOn(t, dst, wake, now)
	p.after(from, wasCurr, wake)
}

// after：curr 剛離開 from（非喚醒）→ 這顆 CPU 需要 pick。
func (p *Policy) after(from int32, wasCurr, wake bool) {
	if wasCurr && !wake {
		p.handoff = append(p.handoff, from)
	}
}

func (p *Policy) prepareRequest(t *Task) {
	if t.Req.Prepare(t.Vruntime, p.request(t), t.Weight, t.initial) && t.initial {
		t.initial = false
	}
}
```

#### 排進 CPU：閒置直接派、否則排隊並檢查搶佔

```go
func (p *Policy) enqueueOn(t *Task, c int32, wake bool, now uint64) {
	if p.cpus[c].task == nil && p.trees[c].Len() == 0 && p.io.Idle(c) {
		p.dispatch(t, c, false)
		return
	}
	t.Cpu, t.state = c, Queued
	p.seq++
	t.seq = p.seq
	p.trees[c].Insert(t)
	p.syncQueued(c)
	if wake && p.shouldPreempt(t, c, now) { // C5
		p.dispatch(t, c, true)
		p.Stats.Preempts++
		return
	}
	p.hr.joinedLone(c, now) // hrtick_update()：加入只有 curr 的 CPU（C14）
}

func (p *Policy) dispatch(t *Task, c int32, preempt bool) {
	if t.state == Queued {
		p.trees[t.Cpu].Remove(t)
		p.syncQueued(t.Cpu)
	}
	slice := p.bw.capSlice(t, c, p.sliceFor(t)) // cpu.max 本地 runtime 上限
	cmd := Cmd{Kind: CmdDispatch, Pid: t.Pid, Cpu: c, Slice: slice, EnqSeq: t.EnqSeq, Slot: t.Slot}
	if preempt {
		cmd.EnqFlags |= p.Enum.EnqPreempt
		cmd.ExpectCurr = p.io.CurrPid(c)
	}
	if p.F.Hrtick && p.trees[c].Len() > 0 {
		cmd.Cflags |= CmdfHrtick // set_protect_slice + start_with_company
	}
	if err := p.io.Submit(cmd); err != nil { // ENOSPC：放回樹，下一輪重送
		p.Stats.SubmitENOSPC++
		p.enqueueRetry(t, c)
		return
	}
	t.Cpu, t.state = c, Dispatched
	p.publish(c, t, true) // C5：curr 資訊與 RUN_TO_PARITY 保護
	p.publishTask(t)      // task_tbl：core_vtime、vruntime、deadline、lag
	p.ld.cpuRunning(c, true)
}

// sliceFor = curr_dl_in()：到 deadline 的實際時間，最短 HRTICK_MIN_NS（10 us）。
func (p *Policy) sliceFor(t *Task) uint64 {
	d := int64(t.Req.Deadline - t.Vruntime)
	if d <= 0 {
		return 10_000
	}
	return max(uint64(d)*max(t.Req.Weight, MinDLWeight)/Nice0Weight, 10_000)
}

func (p *Policy) syncQueued(c int32) {
	p.io.SetQueued(c, uint64(p.trees[c].Len()), p.Slice) // 續跑 slice = 預設 request
}
```

#### pick（handoff CPU 與 idle CPU）

```go
// pick = pick_eevdf()：EDQ 中 eligible 且 deadline 最早；沒有 eligible 取 head（維持前進）。
func (p *Policy) pick(c int32, now uint64) *Task {
	tr := &p.trees[c]
	if tr.Len() == 0 {
		return nil
	}
	if p.F.EligibleScan {
		if t := tr.PickEligible(p.refAt(p.packs[c], now)); t != nil {
			return t
		}
	}
	return tr.Min()
}

// keepRunning = preempt.rs keep_running()：t 剛 charge 並重發 request。
func (p *Policy) keepRunning(t *Task, c int32, now uint64) bool {
	ref := p.refAt(p.packs[c], now)
	if p.F.Eligibility && TimeBefore(ref, t.Vruntime) {
		return false
	}
	head := p.trees[c].PickEligible(ref)
	if !p.F.EligibleScan {
		head = p.trees[c].Min()
	}
	return head == nil || !TimeBefore(head.Req.Deadline, t.Req.Deadline)
}

func (p *Policy) serveCPU(c int32, now uint64) {
	if p.cpus[c].task != nil {
		return
	}
	if t := p.pick(c, now); t != nil {
		p.dispatch(t, c, false)
		return
	}
	if t := p.nb.pull(c, now); t != nil { // C8：sched_balance_newidle()
		p.dispatch(t, c, false)
		return
	}
	p.act.newidleRequest(c, now) // C10：沒得拉就請忙碌來源交出正在跑的任務
}
```

#### keep、yield、cpumask、睡眠、退出

```go
// onKeep：BPF 自行續跑 prev（Go 對這顆 CPU 無排隊）→ 補 update_curr/update_deadline。
func (p *Policy) onKeep(e *Event) {
	t := p.tasks[e.Pid]
	if t == nil || !p.isCurr(t, e.Cpu) {
		return
	}
	p.charge(t, e.SumExec, e.Now)
	p.prepareRequest(t)
	p.publish(e.Cpu, t, true)
	p.publishTask(t)
}

// onYield = yield_task_fair()：eligible 時把 vruntime 推到 deadline 並重發 request。
func (p *Policy) onYield(e *Event) {
	t := p.tasks[e.Pid]
	if t == nil || t.pack == nil {
		return
	}
	p.charge(t, e.SumExec, e.Now)
	if TimeBefore(p.refAt(t.pack, e.Now), t.Vruntime) {
		return // 不 eligible：不放任 vruntime 跑掉
	}
	dist := t.Req.Deadline - t.Vruntime
	t.Vruntime = t.Req.Deadline
	p.foldInto(t, dist) // 與 charge 同一套 fold，維持 V
	p.prepareRequest(t)
	p.Stats.Yields++
}

func (p *Policy) onCpumask(e *Event) {
	t := p.tasks[e.Pid]
	if t == nil {
		return
	}
	if set, err := p.io.Affinity(t.Pid); err == nil {
		t.Allowed = set
	}
	if t.state == Queued && !t.Allowed.Has(t.Cpu) { // 排在不允許的 CPU：搬走
		p.trees[t.Cpu].Remove(t)
		dst := p.selectCPU(t, e, false)
		p.place(t, p.packs[dst], false, e.Now)
		p.enqueueOn(t, dst, false, e.Now)
	}
}

func (p *Policy) onLeave(e *Event, sleep bool) {
	t := p.tasks[e.Pid]
	if t == nil {
		return
	}
	if t.state == Queued {
		p.trees[t.Cpu].Remove(t)
		p.syncQueued(t.Cpu)
	}
	p.bw.unparkDrop(t)
	p.charge(t, e.SumExec, e.Now)
	p.ld.taskQuiesce(t, sleep, e.Now)
	p.quiesce(t, sleep, e.Now)
	if sleep {
		t.Req.Sleep()
		t.lastStopAt = e.Now
	}
	if p.isCurr(t, t.Cpu) {
		p.clearCurr(t.Cpu)
	}
	t.state = Sleeping
}

func (p *Policy) onExit(e *Event) {
	t := p.tasks[e.Pid]
	if t == nil {
		return
	}
	if t.state == Queued {
		p.trees[t.Cpu].Remove(t)
	}
	p.bw.unparkDrop(t)
	if t.pack != nil {
		p.leaveAll(t, true)
	}
	if p.isCurr(t, t.Cpu) {
		p.clearCurr(t.Cpu)
	}
	p.slots.free(t.Slot)
	delete(p.tasks, e.Pid)
}
```

#### Schedule：每次 drain 之後

```go
func (p *Policy) Schedule(now uint64) {
	for _, c := range p.handoff {
		p.serveCPU(c, now)
	}
	for _, c := range p.idleCPUs {
		p.serveCPU(c, now)
	}
	p.handoff, p.idleCPUs = p.handoff[:0], p.idleCPUs[:0]
	p.bw.unpark(now) // 新 period 的任務回到 EDQ（C12）
}

// Periodic 由主迴圈在心跳時呼叫；各模組自己判斷 interval 是否到期。
func (p *Policy) Periodic(now uint64) {
	p.ld.sample(now)     // C7
	p.cg.tick(now)       // C11
	p.bw.tick(now)       // C12
	p.pr.update(now)     // C13
	p.bal.run(now)       // C9
	p.act.run(now)       // C10
	p.cpufreq(now)       // C13
	p.gc(now)            // Part D
}
```

小 helper 由 agent 補齊：`task(e)` 建立或更新任務；`isCurr`/`clearCurr` 讀寫 `p.cpus[c].task`；`foldInto(t, dist)` 是 `charge` 的後半段（只 fold 進 V，不加 vruntime）；`enqueueRetry` 把任務放回樹並在下一輪 `Schedule` 重送。

slot 配發：`slotAlloc` 是 1..MaxTasks-1 的 free list（0 保留為「無」），新任務取一個，EXIT 歸還；用完時新任務 slot = 0，core scheduling 與 dump 退回預設行為。

### C5. `preempt.go`：wakeup preemption、RUN\_TO\_PARITY、PREEMPT\_SHORT

```go
// publish = pack::publish()：picked=true 時設保護 = set_protect_slice()。
func (p *Policy) publish(c int32, t *Task, picked bool) {
	cur := &p.cpus[c]
	cur.task = t
	cur.Vruntime, cur.Deadline = t.Vruntime, t.Req.Deadline
	cur.Request, cur.Weight = p.request(t), t.joinedWeight
	cur.idle = t.Policy == SchedIdle
	cur.schedIdle = cur.idle || p.cg.inIdleGroup(t)
	if !picked {
		return
	}
	protect := t.Req.Deadline
	if !p.F.RunToParity {
		protect = t.Vruntime
	} else if ms := p.trees[c].MinSlice(); ms != 0 && ms < cur.Request {
		// 最短的競爭者決定 curr 可以跑多久才被再問一次
		if lim := t.Vruntime + ms*Nice0Weight/max(cur.Weight, 1); TimeBefore(lim, protect) {
			protect = lim
		}
	}
	cur.Protect = protect
}

// currVruntimeAt = curr_vruntime_at()。
func (p *Policy) currVruntimeAt(c int32, now uint64) uint64 {
	cur := &p.cpus[c]
	_, d := p.uncharged(c, now)
	return advance(cur.Vruntime, d, cur.Weight)
}

// currOwed = curr_owed_service()：超過一整個 request 就不再欠它。
func (p *Policy) currOwed(c int32, ref, now uint64) bool {
	_, d := p.uncharged(c, now)
	if d >= p.cpus[c].Request {
		return false
	}
	return !TimeBefore(ref, p.currVruntimeAt(c, now))
}

// shouldPreempt = wakeup_preempt_fair()，逐條照 preempt.rs::should_preempt。
func (p *Policy) shouldPreempt(w *Task, c int32, now uint64) bool {
	cur := &p.cpus[c]
	if cur.task == nil || cur.task == w {
		return false
	}
	if !p.F.WakeupPreempt {
		p.hr.start(c, now)
		return false
	}
	// 政策：SCHED_IDLE 的 curr 任何非 IDLE wakee 都可搶；IDLE/BATCH wakee 不搶人
	if cur.idle && w.Policy != SchedIdle {
		p.shortenProtect(c, now, 0)
		return true
	}
	if w.Policy == SchedIdle || w.Policy == SchedBatch {
		p.hr.start(c, now)
		return false
	}
	ref := p.refAt(p.packs[c], now)
	p.trees[c].Remove(w) // head 不含 w 自己
	head := p.trees[c].PickEligible(ref)
	if !p.F.EligibleScan {
		head = p.trees[c].Min()
	}
	minSlice := p.trees[c].MinSlice()
	p.trees[c].Insert(w)

	req := p.request(w)
	preempt := func() bool {
		if p.F.Eligibility && TimeBefore(ref, w.Vruntime) {
			return false // wakee 不 eligible
		}
		if p.F.PreemptShort && req < cur.Request {
			return true // 較短 request 越過保護
		}
		owed := p.F.Eligibility && p.currOwed(c, ref, now)
		if owed && p.F.RunToParity && TimeBefore(p.currVruntimeAt(c, now), cur.Protect) {
			return false
		}
		if (owed || !p.F.Eligibility) && !TimeBefore(w.Req.Deadline, cur.Deadline) {
			return false
		}
		return head == nil || TimeBefore(w.Req.Deadline, head.Req.Deadline)
	}()
	if !preempt && p.F.RunToParity {
		s := min(req, cur.Request)
		if minSlice != 0 {
			s = min(s, minSlice)
		}
		p.shortenProtect(c, now, s) // update_protect_slice()
	}
	if !preempt {
		p.hr.start(c, now)
	}
	return preempt
}

// shortenProtect：保護只往前縮。
func (p *Policy) shortenProtect(c int32, now, slice uint64) {
	cur := &p.cpus[c]
	lim := p.currVruntimeAt(c, now) + slice*Nice0Weight/max(cur.Weight, 1)
	if TimeBefore(lim, cur.Protect) {
		cur.Protect = lim
	}
}
```

`Tree.MinSlice()`：C2 的 treap 再增強一個「子樹最小 `Req.Slice`」，在 `update()` 一併維護；PR 的 `dsq::Head.min_slice` 即此。搶佔成立時 `dispatch(w, c, true)` 帶 `SCX_ENQ_PREEMPT` 與 `ExpectCurr`；BPF 只在 curr 仍是同一任務時真的搶（A1.5），否則 wakee 照常排隊。被搶的 curr 會以帶 slice 剩餘的非 WAKEUP enqueue 回到 Go，因 `PLACE_REL_DEADLINE` 保留未完成的 request（Prepare 不重發）。

### C6. `placement.go`：select\_task\_rq\_fair()

與 idle.rs `select_cid()` 同一順序。BPF 給的 `waker_cpu` 就是 fair.c 的 `this_cpu`，`waker_pid` 用來做 `record_wakee()`。「閒置」在 Go 定義為 `vacant(c) = io.Idle(c) && cpus[c].task == nil && trees[c].Len() == 0`；「佔用」不需要 claim 協定：Go 單執行緒，決定後立即 dispatch 即可。

```go
type wakeState struct {
	flips, decayAt uint64
	lastWakee      int32
	recentUsed     int32 // p->recent_used_cpu，-1 = 無
}

func (w *wakeState) recordWakee(wakee int32, now uint64) { // record_wakee()
	if TimeBefore(w.decayAt+1_000_000_000, now) {
		w.flips >>= 1
		w.decayAt = now
	}
	if w.lastWakee != wakee {
		w.lastWakee = wakee
		w.flips++
	}
}

func (p *Policy) selectCPU(t *Task, e *Event, fork bool) int32 {
	now := e.Now
	prev := e.Cpu
	if !t.Allowed.Has(prev) { // affinity 變了，核心留下過期的 CPU
		prev = p.topo.allowedNear(t.Allowed, prev)
	}
	if c, ok := p.requeueCPU(t, now); ok { // ttwu_runnable()：仍欠債回原 CPU
		return c
	}
	if fork { // find_idlest_cpu()
		anchor := e.WakerCpu
		if anchor < 0 {
			anchor = prev
		}
		if c := p.findIdlestFork(t, anchor, now); c >= 0 {
			return c
		}
	}
	if e.WakeFlags&p.Enum.WakeCurrentCPU != 0 && t.Allowed.Has(e.WakerCpu) {
		return e.WakerCpu
	}
	target := p.wakeAffine(t, e, prev, now)
	if e.WakeFlags&p.Enum.WakeTTWU == 0 {
		return target
	}
	// select_idle_sibling()
	if p.asymFits(t, target, now) {
		if p.vacant(target) || p.schedIdleTarget(t, target) {
			return target
		}
	}
	deferPrev, deferRecent := int32(-1), int32(-1)
	if prev != target {
		if c, deferred := p.cached(t, prev, target, now); c >= 0 {
			return c
		} else if deferred {
			deferPrev = prev
		}
	}
	recent := t.wake.recentUsed
	t.wake.recentUsed = prev
	if recent >= 0 && recent != prev && recent != target {
		if c, deferred := p.cached(t, recent, target, now); c >= 0 {
			return c
		} else if deferred {
			deferRecent = recent
		}
	}
	if p.topo.AsymCapacity && p.topo.asymRange(target) != 0 { // select_idle_capacity()
		if c := p.selectIdleCapacity(t, target, p.ld.taskUtil(t, now)); c >= 0 {
			return c
		}
		return target
	}
	schedIdle := int32(-1)
	if p.anySchedIdle() { // choose_sched_idle_rq()
		if c, fallback := p.schedIdlePick(t, prev, target); c >= 0 {
			return c
		} else {
			schedIdle = fallback
		}
	}
	if c := p.pickByCapacity(t, prev, target, now); c >= 0 {
		return c
	}
	if schedIdle >= 0 {
		return schedIdle
	}
	if deferPrev >= 0 {
		return deferPrev
	}
	if deferRecent >= 0 {
		return deferRecent
	}
	return target
}

// wakeAffine = wake_affine()，含 wake_wide、wake_affine_idle、sync、WA_WEIGHT。
func (p *Policy) wakeAffine(t *Task, e *Event, prev int32, now uint64) int32 {
	this := e.WakerCpu
	if e.WakeFlags&p.Enum.WakeTTWU == 0 || this < 0 {
		return prev
	}
	waker := p.tasks[e.WakerPid]
	if waker != nil {
		waker.wake.recordWakee(t.Pid, now)
	}
	if !t.Allowed.Has(this) || !p.topo.sameNode(prev, this) {
		return prev
	}
	if waker != nil && wakeWide(waker.wake.flips, t.wake.flips, uint64(p.topo.llcSize(this))) {
		return prev
	}
	if p.io.Idle(this) { // wake_affine_idle()：從中斷喚醒
		if p.io.Idle(prev) {
			return prev
		}
		return this
	}
	sync := p.F.WakeSync && e.WakeFlags&p.Enum.WakeSync != 0 && waker != nil
	if sync && p.trees[this].Len() == 0 { // nr_running == 1：waker 即將睡
		return this
	}
	if !p.F.WaWeight || p.io.Idle(prev) {
		return prev
	}
	wl := int64(-1)
	if sync {
		wl = int64(p.ld.taskLoad(waker, now))
	}
	thisLoad := max(p.ld.cpuWakeLoad(this), p.packs[this].W)
	prevLoad := max(p.ld.cpuWakeLoad(prev), p.packs[prev].W)
	sameCore := p.topo.SMT && p.topo.sameCore(prev, this)
	if wakeAffineWeight(thisLoad, prevLoad, wl, p.ld.taskLoad(t, now), sameCore) {
		return this
	}
	return prev
}
```

其餘函式照 idle.rs 的同名函式翻譯，規格如下：

| Go 函式 | 對應 | 行為 |
| --- | --- | --- |
| `cached(t, c, target)` | `cached()` | c 與 target 同 LLC 且容量合適：c 閒置且共享 cache → 回 c；不共享 → deferred；c 只跑 SCHED\_IDLE 工作同理 |
| `pickByCapacity` | `pick_by_capacity()` | 非 asym：`idleScan(target, llcOnly=!F.LLCExtend)`；asym：先試 prev（fits），再依 capacity tier 由快到慢各掃一次 |
| `idleScan` / `scanPasses` | `idle_scan()`、`scan_passes()` | 有閒置整核且 SIS\_UTIL 預算 > 0 → 找整核；`--smt-whole-core` 時可越 LLC 找整核；否則 `select_idle_smt`（prev 的 sibling）；再掃 LLC 內任意閒置（cluster 優先）；`--llc-extend` 時續掃 node 與全機 |
| `findIdlestFork` | `find_idlest_fork()` | 由 anchor 的最高 domain 往下，每層用 `forkGroupWins` 選群組（C1b），最後在核心內選閒置或負載最低的 CPU；記 `fork_placed_at` 打破同分 |
| `selectIdleCapacity` | `select_idle_capacity()` | asym 容量域內依 `capacity_rank` 排名：fits 且閒置 > 只閒置 > 其他 |
| `schedIdlePick` | `sched_idle_pick()` | 只跑 SCHED\_IDLE 工作的 CPU 視同閒置；LLC 有閒置整核時只當備案 |
| `asymFits` | `asym_fits()` | 只有容量不對稱時檢查 `util × 1280 < capacity × 1024` |

SIS\_UTIL（`--no-sis-util` 關閉）：每個 LLC 一個 scan budget，由 C9 的 periodic balance 依 LLC 平均利用率更新（`update_idle_cpu_scan()` 公式）；預算 0 時 `select_idle_cpu` 不掃、任務留在 target。

### C7. `load.go`：負載追蹤（load.rs）

PR 用半衰期 32 ms 的 running average 近似 PELT。Go 在使用者空間可以直接用 `float64` 與 `math.Exp2`，不需要 BPF 的定點技巧；對外仍回傳 0..1024 的整數，讓 C1b 的函式原樣使用。

```go
// Ravg：以 halfLife 指數衰減的時間加權平均。
type Ravg struct {
	val, at float64 // 目前平均值、最後更新時間（ns）
	cur     float64 // 自 at 起正在累積的輸入
}

func (r *Ravg) accumulate(input float64, now uint64, halfLife float64) {
	dt := float64(now) - r.at
	if dt > 0 {
		decay := math.Exp2(-dt / halfLife)
		r.val = r.val*decay + r.cur*(1-decay)
		r.at = float64(now)
	}
	r.cur = input
}

func (r *Ravg) read(now uint64, halfLife float64) float64 {
	dt := float64(now) - r.at
	if dt <= 0 {
		return r.val
	}
	decay := math.Exp2(-dt / halfLife)
	return r.val*decay + r.cur*(1-decay)
}

type taskLoadState struct {
	run         Ravg   // 1 = 正在跑
	utilEst     uint64 // util_est
	runnableEst uint64 // 每個睡-醒週期中可執行比例的平均
	wokeAt      uint64
	sleptAt     uint64
}

type cpuLoad struct {
	run       Ravg   // 這顆 CPU 是否在跑我們的任務：cpufreq 用
	load      Ravg   // pack.W 的平均：cfs_rq->avg.load_avg
	wakeLoad  uint64
	idleStamp uint64
	forkAt    uint64 // fork_placed_at
}

type Load struct {
	p    *Policy
	cpus []cpuLoad
}

const half = float64(UtilHalfLifeNs)

func (l *Load) taskRunning(t *Task, running bool, now uint64) {
	t.ld.run.accumulate(b2f(running), now, half)
}

func (l *Load) taskWoke(t *Task, now uint64) {
	if t.ld.wokeAt != 0 && t.ld.sleptAt != 0 { // 上一個完整週期
		cycle := satSub(now, t.ld.wokeAt)
		runnable := satSub(t.ld.sleptAt, t.ld.wokeAt)
		t.ld.runnableEst = runnableUpdate(t.ld.runnableEst, cycle, runnable)
	}
	t.ld.wokeAt = now
}

func (l *Load) taskQuiesce(t *Task, sleep bool, now uint64) {
	l.taskRunning(t, false, now)
	if sleep {
		t.ld.sleptAt = now
		t.ld.utilEst = utilEstUpdate(t.ld.utilEst, l.taskUtilNow(t, now))
	}
}

func (l *Load) taskUtilNow(t *Task, now uint64) uint64 {
	return uint64(t.ld.run.read(now, half) * 1024)
}

// taskUtil = task_util_est()：平均與上次活化用量取大者。
func (l *Load) taskUtil(t *Task, now uint64) uint64 {
	return max(l.taskUtilNow(t, now), t.ld.utilEst)
}

// taskLoad ≈ task_h_load()。
func (l *Load) taskLoad(t *Task, now uint64) uint64 {
	asleep := uint64(0)
	if t.state == Sleeping {
		asleep = satSub(now, t.ld.sleptAt)
	}
	return taskLoad(t.Weight, l.taskUtil(t, now), t.ld.runnableEst, asleep)
}

func (l *Load) cpuRunning(c int32, running bool) {
	l.cpus[c].run.accumulate(b2f(running), l.p.io.Now(), half)
}

func (l *Load) cpuUtil(c int32, now uint64) uint64 { return uint64(l.cpus[c].run.read(now, half) * 1024) }
func (l *Load) cpuLoad(c int32, now uint64) uint64 { return uint64(l.cpus[c].load.read(now, half)) }
func (l *Load) cpuWakeLoad(c int32) uint64          { return l.cpus[c].wakeLoad }

// sample：心跳時對每顆 CPU 取樣 pack.W（PR 在 tick 與 idle 時取樣）。
func (l *Load) sample(now uint64) {
	for c := range l.cpus {
		w := float64(l.p.packs[c].W)
		l.cpus[c].load.accumulate(w, now, half)
		l.cpus[c].wakeLoad = uint64(l.cpus[c].load.read(now, half))
	}
}
```

呼叫點：`dispatch` → `taskRunning(true)` 延後到確認開始跑（Go 以 `RunSeq` 變化或下一個事件判斷；簡化版可在 dispatch 時就記 true）；所有離開 CPU 的事件 → `taskRunning(false)`、`cpuRunning(false)`。CPU 閒置時（EV\_IDLE）記 `idleStamp`，給 C8 的 avg\_idle。

### C8. `newidle.go`：idle pull（sched\_balance\_newidle）

觸發：`serveCPU(c)` 發現 `trees[c]` 空。三層依序：同 LLC → 同 node 其餘 → 全機其餘，拿到第一個就停。搬移用 `place(t, packs[c], sleep=false)`，lag 以來源 pack 計、未完成的 request 保留相對 deadline。

```go
const (
	lvLLC = iota
	lvNode
	lvSystem
	taskScan = 8 // BALANCE_TASK_SCAN：每個來源最多看 8 個任務
)

type nbCPU struct {
	avgIdle, maxCost      uint64
	idleStamp             uint64
	cost, costAt          [3]uint64 // 每層最貴一次 pull 的紀錄與時間
	calls, succ, statsAt  [3]uint64 // NI_RANDOM
	ratio                 [3]uint64
	failed                uint64    // 連續「看到工作卻沒拿到」次數
	cursor                int32
}

type Newidle struct {
	p           *Policy
	cpus        []nbCPU
	overloaded  int // 有排隊任務的 CPU 數（rd->overloaded）
	rng         *rand.Rand
}

func (n *Newidle) pull(c int32, now uint64) *Task {
	st := &n.cpus[c]
	st.idleStamp = now
	if n.overloaded == 0 {
		return nil // 全機沒有任何 CPU 在排隊：不必掃
	}
	budget := n.p.F.NewidleCost
	if budget && st.avgIdle < st.cost[lvLLC] {
		return nil // 這顆 CPU 閒置得比一次 LLC 掃描還短
	}
	spent := uint64(0)
	for lv, span := range n.p.topo.newidleSpans(c) { // [LLC, node\LLC, system\node]
		if len(span) == 0 {
			continue
		}
		if budget && lv != lvLLC && st.avgIdle < spent+st.cost[lv] {
			break
		}
		weight := uint64(1)
		if n.p.F.NewidleSampling {
			w, ok := newidleSample(st.ratio[lv], n.rng.Uint32())
			if !ok {
				continue
			}
			weight = w
		}
		t0 := n.p.io.Now()
		t, sawWork := n.scan(c, span, lv, now)
		cost := n.p.io.Now() - t0
		spent += cost
		st.cost[lv], st.costAt[lv] = newidleCostUpdate(st.cost[lv], st.costAt[lv], cost, now)
		n.sampled(c, lv, b2u(t != nil)*weight, now)
		if t != nil {
			st.failed = 0
			return t
		}
		if sawWork {
			st.failed++
		}
	}
	return nil
}

// scan：從 cursor 起輪流看 span 內有排隊的 CPU，取第一個可搬的任務。
func (n *Newidle) scan(dst int32, span []int32, lv int, now uint64) (*Task, bool) {
	sawWork := false
	checkHot := n.cpus[dst].failed <= n.p.CacheNiceTries+uint64(lv) // 每層放寬一次
	for i := range span {
		src := span[(int(n.cpus[dst].cursor)+i)%len(span)]
		tr := &n.p.trees[src]
		if tr.Len() == 0 {
			continue
		}
		sawWork = true
		if t := tr.firstN(taskScan, func(t *Task) bool {
			return t.Allowed.Has(dst) && !(checkHot && n.taskHot(t, src, dst, now))
		}); t != nil {
			n.cpus[dst].cursor = int32((int(n.cpus[dst].cursor) + i + 1) % len(span))
			tr.Remove(t)
			n.p.syncQueued(src)
			n.p.place(t, n.p.packs[dst], false, now) // lag 隨任務走
			n.p.Stats.NewidlePulls[lv]++
			return t, true
		}
	}
	return nil, sawWork
}

// taskHot = task_hot()：SMT sibling 共用 cache，不算 hot。
func (n *Newidle) taskHot(t *Task, src, dst int32, now uint64) bool {
	if n.p.topo.SMT && n.p.topo.sameCore(src, dst) {
		return false
	}
	return TimeBefore(now, t.lastStopAt+n.p.MigrationCostNs)
}

// idleEnded：CPU 離開閒置（拿到任務或被喚醒派工）→ 更新 avg_idle。
func (n *Newidle) idleEnded(c int32, now uint64) {
	st := &n.cpus[c]
	if st.idleStamp == 0 {
		return
	}
	st.avgIdle = avgIdleUpdate(st.avgIdle, now-st.idleStamp, st.maxCost)
	st.idleStamp = 0
}

// decay：心跳時每層紀錄每秒衰減 1%，maxCost = max(Σcost, migration_cost)。
func (n *Newidle) decay(now uint64) { /* 照 newidle.rs decay() */ }

func (n *Newidle) sampled(c int32, lv int, w, now uint64) {
	st := &n.cpus[c]
	st.calls[lv]++
	st.succ[lv] += w
	st.calls[lv], st.succ[lv], st.statsAt[lv], st.ratio[lv] =
		newidleStatsFold(st.calls[lv], st.succ[lv], st.statsAt[lv], st.ratio[lv], now)
}
```

初值照 `newidle.rs init_cid()`：`avgIdle = 2 × migration_cost`、`maxCost = migration_cost`（`--migration-cost-us` 預設 500）。`Tree.firstN(n, ok)` 依 deadline 序最多看 n 個節點，回傳第一個 `ok` 為真者。`overloaded` 在 `syncQueued` 時隨 `trees[c].Len()` 由 0↔非 0 的轉換增減。被 active balance kick 的目的地（C10）呼叫 `pull` 時不受 budget 與 sampling 限制，同 PR 的 `kicked`。

### C9. `balance.go`：週期 balance（sched\_balance\_domains）

三層 domain，各自的群組粒度與 balance.rs 相同：LLC domain 的群組是 core、node domain 的群組是 LLC、system domain 的群組是 node。每個 domain 由「本地群組的 leader」在 interval 到期時跑一次；LLC 層的 leader 是該 core 負載最低的 sibling（同分取編號小的）。Go 單執行緒，所以 PR 的「選出 → dispatch 時再 drain」兩段式可以合成一步：直接把任務從來源樹搬到目的樹，再交給目的 CPU 的 pick 決定（等同 attach\_task 後讓 wakeup\_preempt 決定）。

```go
const imbalancePct = 117

type balCPU struct {
	interval, next [3]uint64 // ms、下次到期（ns）
	failed         [3]uint64 // nr_balance_failed
}

type Balance struct {
	p    *Policy
	cpus []balCPU
}

func (b *Balance) run(now uint64) {
	for _, d := range b.p.topo.Domains { // 每個 domain：Level、CPU 範圍、子群組列表
		for _, g := range d.Groups {
			dst := b.leader(d, g, now)
			if dst < 0 || !b.due(dst, d, now) {
				continue
			}
			moved := b.balanceDomain(dst, d, g, now)
			b.backoff(dst, d, moved, now)
		}
	}
}

// due：interval = max(nr_cpus × busy_factor − 1, 1) ms，首次依 CPU 編號錯開相位。
func (b *Balance) due(dst int32, d *Domain, now uint64) bool {
	st := &b.cpus[dst]
	minMs := max(uint64(len(d.CPUs))*b.p.BusyBalanceFactor, 1)
	if minMs > 1 {
		minMs--
	}
	if st.interval[d.Level] == 0 {
		st.interval[d.Level] = minMs
		st.next[d.Level] = now + (1+uint64(dst)%minMs)*1_000_000
		return false
	}
	return !TimeBefore(now, st.next[d.Level])
}

// backoff：找到東西就回到最小 interval，否則加倍到 2×min。
func (b *Balance) backoff(dst int32, d *Domain, moved bool, now uint64) {
	st := &b.cpus[dst]
	minMs := max(uint64(len(d.CPUs))*b.p.BusyBalanceFactor, 1)
	if minMs > 1 {
		minMs--
	}
	iv := minMs
	if !moved {
		iv = min(st.interval[d.Level]*2, 2*minMs)
	}
	st.interval[d.Level] = iv
	st.next[d.Level] = now + iv*1_000_000
}

func (b *Balance) balanceDomain(dst int32, d *Domain, local *Group, now uint64) bool {
	p := b.p
	// 1. 整個 domain 的平均 load / capacity（capacity 已扣 pressure，C13）
	var total, cap, util uint64
	for _, c := range d.CPUs {
		total += p.ld.cpuLoad(c, now)
		cap += p.pr.balanceCapacity(c)
		util += p.ld.cpuUtil(c, now)
	}
	if d.Level == lvLLC && p.F.SISUtil {
		p.topo.setScanBudget(d, sisBudget(uint64(len(d.CPUs)), util)) // update_idle_cpu_scan()
	}
	if cap == 0 {
		return false
	}
	avgNorm := total * 1024 / cap
	// 2. 本地群組：低於平均才拉；room = 它還能吸收的 load
	localLoad, localCap := b.group(local, now)
	fair := avgNorm * localCap / 1024
	if localLoad >= fair {
		return false
	}
	room := fair - localLoad
	localNorm := localLoad * 1024 / max(localCap, 1)
	// 3. 最忙的其他群組：高於平均、有排隊；本地過載時還要超過 117% 邊際
	srcG, excess := b.busiestGroup(d, local, avgNorm, localNorm, now)
	if srcG == nil {
		return false
	}
	// 4. 該群組中 load 最高、非閒置、有排隊、高於平均的 CPU
	src := b.busiestCPU(srcG, avgNorm, now)
	if src < 0 {
		return false
	}
	// 5. detach_tasks()：最多看 8 個，權重 >> failed 不超過預算、cold（失敗夠多次後放寬）
	budget := min(room, excess)
	failed := b.cpus[dst].failed[d.Level]
	t := p.trees[src].firstN(taskScan, func(t *Task) bool {
		return (t.joinedWeight>>min(failed, 63)) <= budget &&
			!(p.nb.taskHot(t, src, dst, now) && failed <= p.CacheNiceTries)
	})
	if t == nil {
		b.cpus[dst].failed[d.Level]++
		return false
	}
	to := dst
	if !t.Allowed.Has(dst) { // can_migrate_task() 的 new_dst_cpu：本地群組內另一顆
		if to = local.firstAllowed(t.Allowed); to < 0 {
			b.cpus[dst].failed[d.Level]++
			return false
		}
	}
	p.trees[src].Remove(t)
	p.syncQueued(src)
	p.place(t, p.packs[to], false, now)
	p.enqueueOn(t, to, false, now)
	if p.cpus[to].task == nil {
		p.serveCPU(to, now)
	}
	b.cpus[dst].failed[d.Level] = 0
	p.Stats.BalanceMoves[d.Level]++
	return true
}

// sisBudget = update_idle_cpu_scan()：y = 1024 − min(x²·117²/(10000·1024), 1024)。
func sisBudget(nr, utilSum uint64) uint64 {
	x := utilSum / max(nr, 1)
	scaled := min(x*x*imbalancePct*imbalancePct/(10000*1024), 1024)
	return nr * (1024 - scaled) / 1024
}
```

`busiestGroup`、`busiestCPU`、`group`、`leader` 照 balance.rs 的 `balance_find_group`、`balance_find_cid`、`balance_group_delta`、LLC leader 規則翻譯：比較時一律用 `load × 1024 / balance_capacity`，跳過 CPU 閒置或無排隊者。一次 pass 只搬一個任務（PR 也是一次 drain 一個，並只在它贏得 pick 時跑）。`--busy-balance-factor` 預設 16 ms/CPU。

### C10. `active.go`：active balance、misfit、asym packing

搬「正在跑」的任務。PR 需要 reservation + request + 跨回呼交接（FREE/RESERVED/PUBLISHED）；Go 單執行緒只要三步：在任務上記 `migrateTo = dst`、把 dst 標成 reserved、送 `CMD_STOP(src, pid)`（或用 `Kick` 走 `do_kick` 立即生效）。任務被 kick 後以非 WAKEUP 的 EV\_ENQUEUE 回來，`onEnqueue` 的 `migrateTo` 分支把它放到 dst（C4）。

```go
const activeMaxIntervalMs = 512

const ( // 失衡種類，數字越大越優先（active.rs 的 NONE..LOCAL_PACKING）
	imbNone = iota
	imbCapacity    // misfit：任務太大，搬到容量最高的閒置 CPU
	imbRemotePack  // 同 node 其他 LLC 有更好 packing tier 的閒置 CPU
	imbLocalSMT    // 任務與忙碌 sibling 同核，而 LLC 有整顆閒置核心
	imbLocalPack   // 同 LLC 有更好 packing tier 的閒置 CPU
)

type actCPU struct {
	reserved    bool
	reservedAt  uint64
	interval    uint64 // ms，目的地上的 backoff
	next        uint64
}

type Active struct {
	p    *Policy
	cpus []actCPU
}

// run：心跳時由「來源端」檢查（PR 的 source tick），每顆忙碌 CPU 一次。
func (a *Active) run(now uint64) {
	p := a.p
	for c := range p.cpus {
		src := int32(c)
		t := p.cpus[src].task
		if t == nil || t.Allowed.Count() == 1 { // 綁核任務不搬
			continue
		}
		dst, kind := a.bestDestination(t, src, now)
		if kind == imbNone {
			continue
		}
		a.request(t, src, dst, now)
	}
	for c := range a.cpus { // reserved 超過 1 個 tick 沒等到交接 → 釋放
		if a.cpus[c].reserved && now-a.cpus[c].reservedAt > p.TickNs {
			a.cpus[c].reserved = false
			a.miss(int32(c), now)
		}
	}
}

func (a *Active) bestDestination(t *Task, src int32, now uint64) (int32, int) {
	p := a.p
	if p.topo.AsymCapacity && !fitsCapacity(p.ld.taskUtil(t, now), p.topo.Capacity[src]) {
		if d := p.topo.idleHighestCapacity(t.Allowed, p.vacantFn()); d >= 0 { // update_misfit_status()
			return d, imbCapacity
		}
	}
	if p.topo.AsymPacking {
		if d := p.topo.idleBetterTier(src, t.Allowed, p.vacantFn(), true); d >= 0 {
			return d, imbLocalPack
		}
	}
	if p.topo.SMT && p.siblingBusy(src) {
		if d := p.topo.idleWholeCore(src, t.Allowed, p.vacantFn()); d >= 0 {
			return d, imbLocalSMT
		}
	}
	if p.topo.AsymPacking {
		if d := p.topo.idleBetterTier(src, t.Allowed, p.vacantFn(), false); d >= 0 {
			return d, imbRemotePack
		}
	}
	return -1, imbNone
}

func (a *Active) request(t *Task, src, dst int32, now uint64) {
	st := &a.cpus[dst]
	if st.reserved || TimeBefore(now, st.next) || t.migrateTo >= 0 {
		return
	}
	st.reserved, st.reservedAt = true, now
	t.migrateTo = dst
	_ = a.p.io.Submit(Cmd{Kind: CmdStop, Pid: t.Pid, Cpu: src})
	a.p.Stats.ActiveRequests++
}

// handedOver：onEnqueue 用掉 migrateTo 後呼叫；成功則 interval 回到最小。
func (a *Active) handedOver(dst int32, now uint64) {
	st := &a.cpus[dst]
	st.reserved, st.interval = false, 0
	a.p.Stats.ActiveMoved++
}

func (a *Active) miss(dst int32, now uint64) {
	st := &a.cpus[dst]
	st.interval = min(max(st.interval*2, 1), activeMaxIntervalMs)
	st.next = now + st.interval*1_000_000
}

// newidleRequest = nohz/active 的目的地端：閒置且 pull 不到東西 →
// 找「只跑一個任務、且搬走它收益最大」的來源（最高 load/capacity，
// 且 balance 在該層已失敗 > cache_nice_tries+2 次，fair.c need_active_balance()）。
func (a *Active) newidleRequest(dst int32, now uint64) { /* 照 active.rs request()/target() */ }
```

細節：

- `siblingBusy(src)`：同 core 其他 thread 的 `cpus[x].task != nil` 或 `!io.Idle(x)`。
- packing tier 來自 sysfs 的 ITMT/asym 優先權（`/sys/devices/system/cpu/cpu*/acpi_cppc/highest_perf` 或 `topology/` 下的 `cpu_capacity`），由 `topo.go` 依 `--capacity-tier-tolerance-pct` 分層。
- 交接時若 dst 已不閒置，任務照樣放到 dst 的 EDQ（`enqueueOn`），與 PR「依一般規則放到目的地」相同。
- `onEnqueue` 的 `migrateTo` 分支結束後呼叫 `act.handedOver(dst, now)`。

### C11. `cgroup.go`：cpu.weight、cpu.idle（單一 runqueue 的有效權重）

照 cgroup.rs：fair.c 自 commit 85570f10a4c6 起是單一 runqueue，cgroup 的任務直接加入 CPU 的 pack，權重 = nice 權重乘上每一層 `shares / load`（`__calc_prop_weight()`）。每個 cgroup 在每顆 CPU 有一個 `Queue`，代表 fair.c 該 group 在該 CPU 的 cfs\_rq。Go 的好處：cgroup 樹、cpuset、cpu.max 可以直接從 cgroupfs 讀，不需要 PR 的 registry、sweep 預算與 free list。

```go
const (
	maxDepth     = 8
	weightIdle   = 3          // WEIGHT_IDLEPRIO（scale_load_down 後）
	sumNs        = 1_000_000  // shares 最多每 1 ms 更新一次
	cpusRefresh  = 1_000_000_000
)

type Group struct {
	ID        uint64 // cgroup kn->id（= cgroup2 目錄 inode）
	Parent    *Group // nil = root；root 下的任務不縮放
	Path      string // /sys/fs/cgroup/...，由 inode 對應而來
	Weight    uint64 // cgroupLoadWeight(cpu.weight)；idle 時為 weightIdle
	Idle      bool
	NrCPUs    uint64 // cpuset.cpus.effective 的 CPU 數，每秒重讀
	LoadAvg   uint64 // Σ queue.loadAvgContrib（tg->load_avg）
	NrAvg     uint64 // Σ queue.nrAvgContrib
	queues    []Queue
	bw        *bwGroup // C12；nil = 無 cpu.max
}

type Queue struct {
	g                    *Group
	cpu                  int32
	load, nr             uint64 // 成員在此 CPU 的權重和與數量
	shares, contrib      uint64 // 在父層的份額；有成員時 contrib = shares
	loadAvg, nrAvg       Ravg
	loadAvgContrib       uint64
	nrAvgContrib         uint64
	sharesAt             uint64
}

func (q *Queue) parent() *Queue {
	if q.g.Parent == nil || q.g.Parent.Parent == nil { // 父層是 root：沒有 queue
		return nil
	}
	return &q.g.Parent.queues[q.cpu]
}

// groupWeight = cgroup.rs group_weight()：joining 時把自己算進各層 load。
func groupWeight(q *Queue, nice uint64, joining bool) uint64 {
	add := uint64(0)
	if joining {
		add = nice
	}
	fixed := nice << GroupWeightShift
	for i := 0; q != nil && i < maxDepth; i++ {
		load := q.load + add
		add = 0
		if joining && q.contrib == 0 {
			add = q.shares // 這層第一次有成員，會把 shares 帶進父層
		}
		fixed = propWeight(fixed, q.shares, load)
		q = q.parent()
	}
	return roundWeight(fixed)
}

type Cgroups struct {
	p      *Policy
	byID   map[uint64]*Group
	poll   bool // 核心沒有 cgroup_set_idle/bandwidth：改輪詢 cgroupfs
}

func (cg *Cgroups) queue(t *Task, cpu int32) *Queue {
	if !cg.p.F.Cgroups || t.Cgroup == nil || t.Cgroup.Parent == nil {
		return nil
	}
	return &t.Cgroup.queues[cpu]
}

func (cg *Cgroups) joinWeight(t *Task, cpu int32) uint64 {
	if q := cg.queue(t, cpu); q != nil {
		return groupWeight(q, t.Nice, true)
	}
	return t.Nice
}

// enter / exit：任務在 CPU 上加入/離開其 group queue，load 沿樹往上傳。
func (cg *Cgroups) enter(t *Task, cpu int32) {
	for q, add := cg.queue(t, cpu), t.Nice; q != nil && add != 0; q = q.parent() {
		q.load += add
		q.nr++
		if q.contrib != 0 {
			break // 父層已經計入這層的 shares
		}
		q.contrib = q.shares
		add = q.shares
	}
}

func (cg *Cgroups) exit(t *Task) { /* enter 的反向：load -= ，nr 歸零時 contrib = 0 並從父層扣掉 shares */ }

// updateShares = update_cfs_group() 的 concur 模式；由 tick 對正在跑任務的每一層呼叫。
func (cg *Cgroups) updateShares(q *Queue, now uint64) {
	q.loadAvg.accumulate(float64(q.load), now, half)
	q.nrAvg.accumulate(float64(q.nr), now, half)
	if now-q.sharesAt < sumNs {
		return
	}
	q.sharesAt = now
	avg := uint64(q.loadAvg.read(now, half))
	if diff(avg, q.loadAvgContrib) > q.loadAvgContrib/64 {
		q.g.LoadAvg = q.g.LoadAvg - q.loadAvgContrib + avg
		q.loadAvgContrib = avg
	}
	nr := uint64(q.nrAvg.read(now, half))
	if diff(nr, q.nrAvgContrib) > q.nrAvgContrib/64 {
		q.g.NrAvg = q.g.NrAvg - q.nrAvgContrib + nr
		q.nrAvgContrib = nr
	}
	shares := groupShares(q.g.Weight, q.g.NrAvg, q.g.NrCPUs, q.load, avg, q.g.LoadAvg, q.loadAvgContrib)
	if shares != q.shares {
		q.setShares(shares) // 有成員時同步改父層 load（group_contrib_sync）
	}
}

// tick：心跳時對每顆 CPU 正在跑的任務做 group_tick()，權重變了就 reweight。
func (cg *Cgroups) tick(now uint64) {
	for c := range cg.p.cpus {
		t := cg.p.cpus[c].task
		if t == nil {
			continue
		}
		for q, i := cg.queue(t, int32(c)), 0; q != nil && i < maxDepth; q, i = q.parent(), i+1 {
			cg.updateShares(q, now)
		}
		if w := groupWeight(cg.queue(t, int32(c)), t.Nice, false); w != t.joinedWeight {
			cg.p.reweight(t, w, now)
		}
	}
	cg.sweep(now) // 沒人跑的 queue 衰減其 contrib（update_blocked_averages）、每秒重讀 cpuset
}
```

事件處理（`handle`）：

| 事件 | 動作 |
| --- | --- |
| EV\_CGRP\_INIT | 建 `Group`，`Parent = byID[arg0]`，`Weight = cgroupLoadWeight(arg1)`，以 inode 找 `Path`，讀 `cpuset.cpus.effective`、`cpu.idle`；`arg2/arg3` 非 0 時建 cpu.max（C12） |
| EV\_CGRP\_EXIT | 從 `byID` 刪除；仍有成員時先把它們視為 root 任務 |
| EV\_CGRP\_MOVE | 任務若是 pack 成員：`leaveAll` → 換 `t.Cgroup` → `place(sleep=false)`，即 PR `cpuctl_move` |
| EV\_CGRP\_WEIGHT / EV\_CGRP\_IDLE | 更新 `Weight`；idle 時用 `weightIdle`，並標記 `inIdleGroup` 給 C5 與 C6 |
| EV\_ENQUEUE 帶的 `cgid` 與 `t.Cgroup` 不同 | 同 MOVE（第一次見到任務時的初始化也走這裡） |

深度超過 8 層的 cgroup 共用第 8 層祖先的 queue（同 PR）。`inIdleGroup(t)`：任一祖先 `Idle`。`--disable-cgroups` 時 `queue()` 一律回 nil。

### C12. `bandwidth.go`：cpu.max

照 bandwidth.rs：每個有限額的 cgroup 有一個 pool，每 `period` 補 `quota`（加上 carry-over，總量不超過 `quota + burst`）；CPU 一次從 pool 借一個 slice（5 ms）到本地 runtime，charge 時逐層扣；某層 pool 與本地都用完就節流（throttle），其任務不得執行：正在跑的被停下，排隊的進 group 自己的 backlog，下個 period 依進入順序放回。祖先的限額同樣約束子孫。

```go
const (
	bwSliceNs     = 5_000_000 // sched_cfs_bandwidth_slice
	quotaInf      = ^uint64(0)
)

type bwGroup struct {
	period, quota, burst uint64 // ns；quota = quotaInf 代表無限
	pool                 uint64 // 本 period 剩餘
	nextRefill           uint64
	local                []int64  // 每 CPU 借到、尚未用完的 runtime
	throttled            []bool   // 每 CPU 是否節流（cfs_rq throttled）
	backlog              []*Task  // 被 park 的任務，FIFO
	nrThrottled          uint64   // 統計
}

type Bandwidth struct {
	p       *Policy
	limited []*Group // 有 cpu.max 的 group
}

// levels：任務所在 group 到 root 之間所有有限額的祖先（含自己）。
func (b *Bandwidth) levels(t *Task) []*bwGroup { /* 由 t.Cgroup 往上收集 g.bw != nil */ }

// charge：每層扣本地 runtime；不夠時向 pool 借 slice，借不到 → 該層在此 CPU 節流。
func (b *Bandwidth) charge(t *Task, delta, now uint64) {
	c := t.Cpu
	for _, g := range b.levels(t) {
		g.local[c] -= int64(delta)
		if g.local[c] > 0 {
			continue
		}
		if !b.assign(g, c) {
			g.throttled[c] = true
			g.nrThrottled++
			if cur := b.p.cpus[c].task; cur != nil && b.under(cur, g) {
				_ = b.p.io.Submit(Cmd{Kind: CmdStop, Pid: cur.Pid, Cpu: c}) // check_cfs_rq_runtime()
			}
		}
	}
}

// assign：從 pool 借 slice（+ 補足已透支的部分）。
func (b *Bandwidth) assign(g *bwGroup, c int32) bool {
	if g.quota == quotaInf {
		g.local[c] = bwSliceNs
		return true
	}
	want := uint64(bwSliceNs - g.local[c]) // local ≤ 0，順便還債
	take := min(want, g.pool)
	g.pool -= take
	g.local[c] += int64(take)
	return g.local[c] > 0
}

func (b *Bandwidth) throttled(t *Task, now uint64) bool {
	if !b.p.F.CpuMax {
		return false
	}
	for _, g := range b.levels(t) {
		if g.throttled[t.Cpu] && !b.assign(g, t.Cpu) {
			return true
		}
		g.throttled[t.Cpu] = false
	}
	return false
}

// park：任務離開 pack 與 group load，進最內層節流 group 的 backlog（PR：park）。
func (b *Bandwidth) park(t *Task, now uint64) {
	b.p.quiesce(t, false, now) // 保留 lag 與相對 deadline
	g := b.innermostThrottled(t)
	g.backlog = append(g.backlog, t)
	t.state = Parked
}

// tick：period 到期就補 pool、清節流、把 backlog 依序放回（C4 Schedule 的 unpark）。
func (b *Bandwidth) tick(now uint64) {
	for _, grp := range b.limited {
		g := grp.bw
		if TimeBefore(now, g.nextRefill) {
			continue
		}
		g.pool = min(g.pool+g.quota, g.quota+g.burst) // __refill_cfs_bandwidth_runtime()
		g.nextRefill = now + g.period
		for c := range g.throttled {
			g.throttled[c] = false
		}
	}
}

func (b *Bandwidth) unpark(now uint64) {
	for _, grp := range b.limited {
		g := grp.bw
		for len(g.backlog) > 0 {
			t := g.backlog[0]
			if b.throttled(t, now) {
				break // 還沒輪到，保持順序
			}
			g.backlog = g.backlog[1:]
			dst := b.p.selectCPU(t, &Event{Cpu: t.Cpu, Now: now}, false)
			b.p.place(t, b.p.packs[dst], false, now)
			b.p.prepareRequest(t)
			b.p.enqueueOn(t, dst, false, now)
		}
	}
}

// capSlice：dispatch 時 slice 不超過所有層在該 CPU 的本地 runtime。
func (b *Bandwidth) capSlice(t *Task, c int32, slice uint64) uint64 {
	for _, g := range b.levels(t) {
		if g.local[c] > 0 {
			slice = min(slice, uint64(g.local[c]))
		}
	}
	return max(slice, 10_000)
}

// quietReturn：某 group 在某 CPU 沒有成員時，把剩餘本地 runtime 還給 pool（PR：give back）。
func (b *Bandwidth) quietReturn(g *bwGroup, c int32) {
	if g.local[c] > 0 && g.quota != quotaInf {
		g.pool += uint64(g.local[c])
		g.local[c] = 0
	}
}
```

設定來源：EV\_CGRP\_INIT / EV\_CGRP\_BW（`period_us`、`quota_us`、`burst_us`）；核心不支援這些回呼時，`Cgroups.poll` 每秒讀一次 `cpu.max`（格式 `"<quota|max> <period>"`）與 `cpu.max.burst`。`unparkDrop(t)`：任務在 backlog 中睡著或退出時把它移除。`--disable-cpu-max` 時 `throttled` 恆為 false。

### C13. `pressure.go`、`cpufreq.go`：實際可用容量與頻率

pressure.rs 的公式不變：32 ms 視窗內 `available = 1024 − lost × 1024 / elapsed`，以 3:1 平滑；balance 用 `capacity × avail / 1024` 當分母；低於 117% 邊際的 CPU 視為 reduced，被高優先 class 擠掉的任務改排到別處。差別在 `lost` 的來源：

| 損失 | PR 推估方式 | scx\_goeevdf |
| --- | --- | --- |
| RT / DL / stop class | stopping 時 slice 有剩 → 到下次 running | `cpu_release` → `cpu_acquire` 精確計時（A2.3 的 `displaced_ns`） |
| IRQ、softirq | rq clock 與 task clock 的差 | Go 讀 `/proc/stat` 每 CPU 的 irq + softirq |
| steal | 同上 | `/proc/stat` 的 steal |

```go
const (
	evalNs = 32_000_000
	idleDropNs = 1_000_000_000 // 沒有需求 1 秒就丟掉估計
)

type prCPU struct {
	demand        bool   // 這顆 CPU 有我們的工作（curr 或排隊）
	at            uint64 // 視窗起點
	displacedAt   uint64 // 視窗起點時的 displaced_ns
	stolenAt      uint64 // 視窗起點時 irq+softirq+steal（ns）
	avail         uint64 // 0..1024，平滑後
	valid         bool
	idleAt        uint64
	cap           uint64 // balance capacity
	migrateNext   uint64 // reduced 時搬移的節奏
	migrateFailed uint64
}

type Pressure struct {
	p     *Policy
	cpus  []prCPU
	stat  *procStat // 解析 /proc/stat，USER_HZ → ns
}

func (pr *Pressure) update(now uint64) {
	if !pr.p.F.CapacityPressure {
		return
	}
	stolen := pr.stat.read() // 每 CPU 累計 ns
	for c := range pr.cpus {
		st := &pr.cpus[c]
		demand := pr.p.cpus[c].task != nil || pr.p.trees[c].Len() > 0
		if !demand {
			if st.demand {
				st.idleAt = now
			}
			if st.valid && now-st.idleAt > idleDropNs {
				st.valid = false
			}
			st.demand = false
			continue
		}
		disp := pr.p.io.Displaced(int32(c))
		if !st.demand { // 新視窗
			st.demand, st.at, st.displacedAt, st.stolenAt = true, now, disp, stolen[c]
			continue
		}
		elapsed := now - st.at
		if elapsed < evalNs {
			continue
		}
		lost := (disp - st.displacedAt) + satSub(stolen[c], st.stolenAt)
		available := 1024 - min(lost*1024/elapsed, 1024)
		if st.valid {
			st.avail = (3*st.avail + available) / 4
		} else {
			st.avail, st.valid = available, true
		}
		st.at, st.displacedAt, st.stolenAt = now, disp, stolen[c]
		st.cap = max(pr.p.topo.Capacity[c]*st.avail/1024, 1)
	}
}

func (pr *Pressure) balanceCapacity(c int32) uint64 {
	if st := &pr.cpus[c]; pr.p.F.CapacityPressure && st.demand && st.valid && st.cap != 0 {
		return st.cap
	}
	return pr.p.topo.Capacity[c]
}

// reduced = check_cpu_capacity()。
func (pr *Pressure) reduced(c int32) bool {
	st := &pr.cpus[c]
	return pr.p.F.CapacityPressure && st.demand && st.valid &&
		st.cap*imbalancePct < pr.p.topo.Capacity[c]*100
}

// target = pressure_target()：cpu_release 退回（SCX_ENQ_REENQ）的任務該去哪。
func (pr *Pressure) target(t *Task, src int32, now uint64) int32 {
	st := &pr.cpus[src]
	if !pr.reduced(src) || TimeBefore(now, st.migrateNext) {
		return src
	}
	// 以 LLC 的 busy balance interval 為節奏，挑 load/balance capacity 最低、
	// 任務允許、且 src 比它重超過 117% 的 CPU；搬的權重不超過失衡量，
	// 失敗次數多了放寬 cache hot 與權重限制（同 detach_tasks()）。
	dst := pr.leastLoaded(t, src, now)
	st.migrateNext = now + pr.p.bal.llcInterval(src)*1_000_000
	if dst < 0 {
		st.migrateFailed++
		return src
	}
	st.migrateFailed = 0
	pr.p.Stats.PressureMoves++
	return dst
}
```

`/proc/stat` 的單位是 USER\_HZ（通常 100），單次只有 10 ms 解析度；32 ms 視窗搭配 3:1 平滑後誤差可接受（cgroup 的 `cpu.stat` 不分 CPU，無法替代）。T4 會在 `stress-ng --irq` 下確認 `avail` 確實下降。

cpufreq（load.rs `update_cpufreq`，`--disable-cpufreq` 關閉）：每個 PELT period（1 ms，心跳）對每顆 CPU 算 `perf = min(cpuUtil × 1280 / 1024, 1024)`（schedutil 的 1.25 倍 headroom），與上次不同才 `io.SetPerf(c, perf)`；BPF 在下一個 tick 套用（A2.4）。新任務剛放到 CPU 時（`place` 回傳 true）立刻更新一次，對應 PR 的 `attach` 參數。

### C14. hrtick、core scheduling、proxy execution、debug、features

#### `hrtick.go`（hrtick.rs）

```go
const hrtickMinNs = 10_000

type Hrtick struct {
	p  *Policy
	at []uint64 // 每 CPU 已武裝的到期時間；0 = 無
}

// start = hrtick::start()：有 wakee 在 curr 後面等，就在 curr 的 request 結束時叫醒。
func (h *Hrtick) start(c int32, now uint64) {
	p := h.p
	if !p.F.Hrtick || p.cpus[c].task == nil || p.trees[c].Len() == 0 {
		return
	}
	in := p.currDeadlineIn(c, now) // (deadline − currVruntimeAt) × w / 1024
	if in < hrtickMinNs {
		return // 已到期：下一個 tick 會處理
	}
	when := now + in
	if h.at[c] != 0 && !TimeBefore(when, h.at[c]) && TimeBefore(now, h.at[c]) {
		return // 已經有更早的 timer
	}
	h.at[c] = when
	_ = p.io.Submit(Cmd{Kind: CmdHrtick, Cpu: c, Slice: when, Slot: uint32(p.io.RunSeq(c))})
}

// joinedLone = hrtick_update()：EDQ 從 0 變 1 時 curr 才有同伴。
func (h *Hrtick) joinedLone(c int32, now uint64) {
	if h.p.trees[c].Len() == 1 {
		h.start(c, now)
	}
}
```

BPF 端 `hrtick_fire` 以 `run_seq` 低 32 位比對（指令的 `slot` 欄位只有 32 位），timer 對已結束的執行只會空轉一次。dispatch 時帶 `CMDF_HRTICK` 的情況由 BPF `running` 直接武裝，Go 同步把 `at[c]` 設為 `now + slice`。

#### `coresched.go`（main.rs `core_sched_before`、pack.rs `core_vtime`）

```go
// coreVtime：從 pack 上次清空時的 V 量起，避免換 pack 的任務沿用舊座標。
func (p *Policy) coreVtime(t *Task, now uint64) (uint64, bool) {
	pk := t.pack
	if pk == nil {
		return 0, false
	}
	gen := pk.emptyGen + 1
	if pk.core.gen != gen {
		pk.core.origin, pk.core.gen = pk.Ref, gen
	}
	v := t.Vruntime
	if p.isCurr(t, pk.CPU) {
		v = p.currVruntimeAt(pk.CPU, now)
	}
	return v - pk.core.origin, true
}

func (p *Policy) publishTask(t *Task) {
	if t.Slot == 0 {
		return
	}
	pub := TaskPub{Pid: t.Pid, Vruntime: t.Vruntime, Deadline: t.Req.Deadline, Lag: t.lag, Weight: t.joinedWeight}
	if v, ok := p.coreVtime(t, p.io.Now()); ok {
		pub.CoreVtime = v
	}
	p.io.Publish(t.Slot, pub) // seqcount：Seq 先 +1（奇數）、寫欄位、再 +1
}
```

正在跑的任務只在事件時重發布；BPF 的 `core_vtime()` 可再加上 `(now − run_start) × 1024 / weight` 投影（`task_pub.weight` 已提供），與 PR 的 `running` 投影一致。

#### Proxy execution（`--no-proxy-exec`）

只在核心對 sched\_ext 支援 proxy execution 時啟用：BPF 把 enqueue 的 blocked 旗標原樣傳到 `ev.flags`（PR 的 `EnqFlags::blocked()`）。Go 對 blocked donor：保留 vruntime 與 deadline、放回它自己的 CPU、跳過所有 shortcut；balance 與 newidle 的 `firstN` 過濾掉 blocked 任務（`can_migrate_task()` 拒絕 `task_is_blocked()`）。

#### `debug.go`

BPF `ge_dump_task` 從 `task_tbl[slot]` 印 vruntime、deadline、lag、weight，與 debug.rs 相同欄位；Go 的 `--stats` 另外印每 CPU 的 V、W、EDQ 長度、curr 與保護。

#### `features.go`：開關全部照 scx\_reevdf 的 CLI

| Go 欄位 | CLI | 預設 |
| --- | --- | --- |
| `WakeupPreempt` | `--no-wakeup-preempt` | 開 |
| `Eligibility` | `--no-eligibility`（連帶關 EligibleScan、RunToParity） | 開 |
| `EligibleScan` | `--no-eligible-scan` | 開 |
| `RunToParity` | `--no-run-to-parity` | 開 |
| `PreemptShort` | `--no-preempt-short` | 開 |
| `PlaceLag` | `--no-place-lag` | 開 |
| `PlaceRelDeadline` | `--no-place-rel-deadline` | 開 |
| `DelayDequeue` | `--no-delay-dequeue`（連帶關 DelayRequeue） | 開 |
| `DelayRequeue` | `--no-delay-requeue` | 開 |
| `ProxyExec` | `--no-proxy-exec` | 開（核心支援時） |
| `Hrtick` | `--no-hrtick` | 開 |
| `VrefUpdate` | `--no-vref-update` | 開 |
| `WakeSync` | `--no-wake-sync` | 開 |
| `WaWeight` | `--no-wa-weight` | 開 |
| `SISUtil` | `--no-sis-util` | 開 |
| `NewidleCost` / `NewidleSampling` | `--no-newidle-cost` / `--no-newidle-sampling` | 開 |
| `CapacityPressure` | `--no-capacity-pressure` | 開 |
| `Cgroups` / `CpuMax` | `--disable-cgroups` / `--disable-cpu-max` | 開 |
| `Cpufreq` | `--disable-cpufreq` | 開 |
| `LLCExtend`、`SMTWholeCore` | `--llc-extend`、`--smt-whole-core` | 關 |
| 拓撲 | `--disable-numa`、`--disable-smt`、`--uniform-capacity`、`--asym-capacity`、`--capacity-tier-tolerance-pct`、`--disable-asym-packing`、`--smt-asym-packing` | 照 PR |
| 數值 | `--slice-us`（0 = BaseSlice）、`--migration-cost-us 500`、`--cache-nice-tries 1`、`--busy-balance-factor 16`、`--tick-us` | 照 PR |

`--no-task-clock`：改用 `ev.now` 差值（牆鐘，含 IRQ 與 steal）代替 `sum_exec_runtime` 差值。

## Part D：主迴圈、計時器、設定與統計（`cmd/goeevdf/`）

### D1. 啟動順序

```go
func main() {
	cfg := parseFlags()                      // C14 的完整 CLI
	_ = unix.Mlockall(unix.MCL_CURRENT | unix.MCL_FUTURE) // 照 qumun：排程器不能被換頁
	topo, err := eevdf.LoadTopology(cfg)     // sysfs：possible CPUs、core/cluster/LLC/node、capacity、tier
	s, err := bpf.Open()
	s.SetUserschedTgid(uint32(os.Getpid()))
	for _, op := range bpf.MissingOps() {    // BTF 檢查 cgroup_set_idle、cgroup_set_bandwidth
		s.DisableOp(op)
	}
	if err := s.Load(); err != nil { ... }  // 之後 rodata 不可再寫；mmap 三張表
	enums := bpf.ImportEnums()               // SCX_ENQ_*、SCX_WAKE_*、SCX_DEQ_SLEEP，照 qumun util/emun.go
	pol := eevdf.NewPolicy(cfg, topo, enums, schedIO{s})
	pol.LoadCgroups("/sys/fs/cgroup")        // 先建好既有 cgroup 樹，INIT 事件只補新建的
	if err := s.Attach(); err != nil { ... }
	ctx, cancel := signal.NotifyContext(context.Background(), unix.SIGINT, unix.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- run(ctx, s, pol) }()
	statsLoop(ctx, s, pol, cfg.Stats)        // 主 goroutine：統計、UEI 檢查
	cancel()
	<-done
	s.Close()                                // 迴圈結束後才釋放 C ring
}
```

### D2. 排程迴圈

```go
const maxBatch = 512

func run(ctx context.Context, s *bpf.Sched, pol *eevdf.Policy) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var raw bpf.Ev
	var e eevdf.Event
	nextPeriodic := uint64(0)
	for ctx.Err() == nil {
		got, enq := 0, uint64(0)
		for got < maxBatch {
			ok, err := s.Consume(&raw)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			got++
			if raw.Kind == bpf.EvEnqueue {
				enq++
			}
			convert(&raw, &e)
			pol.Handle(&e)
		}
		if enq > 0 {
			s.DecNrQueued(enq) // 只有 EV_ENQUEUE 計入 nr_queued
		}
		now := monotonicNow()
		pol.Schedule(now)
		if !eevdf.TimeBefore(now, nextPeriodic) {
			pol.Periodic(now)
			nextPeriodic = now + 1_000_000 // 1 ms，與 BPF 心跳一致
		}
		s.SetNrScheduled(uint64(pol.QueuedTotal())) // usersched_has_pending() 用
		pol.PublishStats()
		if got == 0 {
			if err := s.Yield(); err != nil { // 沒事就讓出；BPF 有待辦才會再派排程執行緒
				return err
			}
		}
	}
	return nil
}
```

`Policy` 只被這條 goroutine 觸碰，不需要鎖；統計用 `atomic.Pointer[Snapshot]` 發布。不要設 `GOMAXPROCS=1`（qumun README 的同一條警告）。Go GC 的 STW 會讓排程暫停：啟動時 `debug.SetGCPercent(400)`、熱路徑不配置記憶體（`Event`、`Cmd` 都在 stack 上，`Tree` 節點用 free list 重用），並在統計中記錄 `runtime/metrics` 的 GC pause。

### D3. 死任務 GC

每秒掃一次 `pol.tasks`：`state != Sleeping` 且 `now − lastEvNow > 2 s` 的任務用 `unix.Kill(pid, 0)` 檢查，`ESRCH` 就走 `onExit`；`nr_ev_drops` 非 0 時縮短到 100 ms。限時 200 us，未掃完下次續掃。

### D4. 統計（`--stats <interval>`、`--monitor <interval>`）

每個 interval 印一行 JSON，欄位對應 scx\_reevdf 的 `--stats`：

- 事件：每秒各 `EV_*` 數、`nr_ev_drops`、`nr_congested`、`nr_stale_cmds`、`nr_bounces`、`SubmitENOSPC`、`MissedSleep`。
- 排程：`Preempts`、`Yields`、BPF `keeps` 總和、各層 `NewidlePulls`、`BalanceMoves`、`ActiveRequests/Moved`、`PressureMoves`。
- 狀態：每 CPU 的 V、W、EDQ 長度、avail（pressure）、perf；cgroup 節流中的 group 數與 backlog 長度。
- 延遲：Go 迴圈每輪耗時直方圖（log2 bucket，照 qumun `latency.go`）、GC pause。

`--monitor` 從另一個行程讀同一份資料：Go 把 Snapshot 寫到一個 `BPF_F_MMAPABLE` 的 stats map，monitor 以 bpftool 找到 map id 後 mmap 唯讀。

### D5. Makefile

目前 `internal/bpf` 以 `bpf_object__open_file()` 載入 BPF 物件，並直接使用系統 libbpf；因此 Makefile 編譯 `bpf/goeevdf.bpf.c`、由核心 BTF 產生 `bpf/vmlinux.h`，再以 `go build -ldflags "-checklinkname=0" ./cmd/goeevdf` 建置。cgroup bandwidth callback 僅在建置用的 `vmlinux.h` 有該欄位時編入；init args 的 bandwidth 欄位以 CO-RE 檢查，供較舊核心使用。`bpftool gen skeleton` 可作為獨立目標，但目前載入路徑不依賴它。

qumun 的靜態 libbpf、`scx/` 標頭與 skeleton 流程不能直接複製：本 repo 目前沒有 libbpf/scx 子模組，且 bridge 使用直接 libbpf API。若日後改成 skeleton 載入，再一併引入並固定相應來源與版本。

## Part E：per-LLC 分片（Phase 5）

單一排程 goroutine 的上限是一顆 CPU 能處理的事件量。分片把控制平面切成每個 LLC 一個 shard：shard 擁有該 LLC 內 CPU 的 pack、EDQ、curr 視圖與目前住在那裡的任務，各自跑一條 `LockOSThread` 的迴圈。這對應 fair.c 的實際結構：LLC 內的決策（pick、preempt、select\_idle\_sibling、LLC 層 balance）不需要看別的 LLC，跨 LLC 的只有 node/system 層的 balance、newidle 與少數喚醒。

**何時啟用**：`--shards auto`（預設）在 possible CPU ≤ 64 時等於 1，即前面各 Part 的設計；超過時每個 LLC 一個 shard，少於 4 顆 CPU 的相鄰 LLC 合併。T4 若觀察到單一迴圈忙碌率 > 50%，或 `nr_ev_drops` > 0，也應改用分片。`--shards 1` 強制關閉。

### E1. 不變量

1. **一個任務同時只有一個 owner shard**；只有 owner 能改它的狀態。owner 記在 `owner[slot]`（`atomic.Int32`，全域陣列）。
2. **shard 只對自己的 CPU 送 DISPATCH、STOP、HRTICK**。要把任務放到別的 shard，先移交所有權，由目的 shard 自己派。
3. **跨 shard 只讀快照**：別的 shard 的 per-CPU load、util、W、EDQ 長度、pressure 容量由 owner 以 atomic 發布；讀到的值可能落後一輪，與 PR 用 arena atomics 跨 CPU 讀的語意相同。
4. **跨 shard 的寫入只有三種**：任務移交、steal 請求、cgroup/bandwidth 的全域彙總（atomic 加減與 CAS）。

### E2. BPF 端變更

```c
#define MAX_SHARDS 32
const volatile u32 cpu_shard[MAX_CPUS];      /* Go 在 Load 前填 */
const volatile u32 nr_shards = 1;

/* 事件 ring：每 shard 一個，ringbuf 作為 map-in-map 的 inner map */
struct ev_ring { __uint(type, BPF_MAP_TYPE_RINGBUF); __uint(max_entries, 4 << 20); };
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY_OF_MAPS);
	__uint(max_entries, MAX_SHARDS);
	__type(key, u32);
	__array(values, struct ev_ring);
} ev_rings SEC(".maps");

static __always_inline void *shard_ring(s32 cpu)
{
	u32 s = cpu >= 0 && cpu < MAX_CPUS ? cpu_shard[cpu] : 0;
	return bpf_map_lookup_elem(&ev_rings, &s);
}

/* 每 shard 的待辦計數與排程執行緒 DSQ */
volatile u64 nr_queued_s[MAX_SHARDS], nr_scheduled_s[MAX_SHARDS], needed_s[MAX_SHARDS];
#define SCHED_DSQ_BASE 0x1000 /* SCHED_DSQ_BASE + shard */

struct { __uint(type, BPF_MAP_TYPE_HASH); __type(key, u32); __type(value, u32);
	 __uint(max_entries, MAX_SHARDS); } usersched_tids SEC(".maps"); /* tid → shard */
```

- `ev_reserve(kind)` 改成 `ev_reserve(kind, cpu)`，以事件所屬 CPU（enqueue 用 `scx_bpf_task_cpu(p)`、idle 用該 CPU）選 ring；cgroup 事件一律送 shard 0。
- 指令 ring：每 shard 一個 USER\_RINGBUF。若目標核心不支援 USER\_RINGBUF 當 inner map，就用巨集靜態宣告 `cmds_0` … `cmds_31`，`ge_dispatch(cpu)` 以 `switch (cpu_shard[cpu])` 只 drain 自己 shard 的 ring（不變量 2 保證指令只會在本 shard 的 CPU 生效）。
- 排程執行緒：每 shard 一條，Go 把它 `sched_setaffinity` 綁在本 shard 的 CPU 上；`enqueue` 以 `usersched_tids` 查到 shard，插入 `SCHED_DSQ_BASE + s`；`ge_dispatch` 只在本 shard 有待辦時消費它。
- `update_idle`、心跳 timer 改為設定 `needed_s[cpu_shard[cpu]]`。

### E3. Go 結構

```go
type Shard struct {
	ID     int
	CPUs   []int32
	pol    *eevdf.Policy // 只含本 shard 的 CPU；其餘 CPU 透過 Remote 介面
	inbox  chan Msg      // MPSC，容量 4096
	ev     *bpf.EvRing
	cmd    *bpf.CmdRing
}

type MsgKind uint8

const (
	MsgAdopt     MsgKind = iota // 移交任務：task 已離開來源 pack，lag 已算好
	MsgSteal                    // 請把一個可搬任務交給 dst（newidle / balance / active）
	MsgForward                  // 事件送錯 shard（核心改了 task_cpu）：轉給 owner
	MsgStopFor                  // active balance：停下 pid，交接到 dst
)

type Msg struct {
	Kind  MsgKind
	Task  *eevdf.Task
	Dst   int32
	Ev    eevdf.Event
	Level int
}

// Remote：Policy 碰到不屬於本 shard 的 CPU 時呼叫。
type Remote interface {
	Owns(cpu int32) bool
	Snap(cpu int32) *CPUSnap                          // atomic 快照
	Adopt(t *eevdf.Task, dst int32, wake bool)         // 移交所有權
	Steal(from, dst int32, level int)                  // 非同步請求
	StopFor(cpu int32, pid int32, dst int32)
}

type CPUSnap struct {
	Load, Util, W, Queued, BalanceCap, WakeLoad atomic.Uint64
}
```

### E4. Policy 的改動點

| 位置 | 單 shard | 分片後 |
| --- | --- | --- |
| `onEnqueue` 的 `selectCPU` 結果 dst | `place` → `enqueueOn` | `!Owns(dst)`：在本 shard 用來源 pack 算 lag、`leaveAll`，`Adopt(t, dst, wake)`；目的 shard 以 `settle` 拿到的 lag 呼叫 `place`（等同 PR 跨 pack 的 `place`） |
| `selectCPU` 讀其他 CPU 的 load | `p.ld.cpuLoad(c)` | `Snap(c).Load.Load()` |
| `vacant(c)` | Go 視圖 + `io.Idle` | 非本 shard：`io.Idle(c) && Snap(c).Queued == 0` |
| `newidle.pull` 的 node/system 層 | 直接拿 | 依快照挑最忙的 CPU，`Steal(src, dst, lv)`；本輪先回 nil，收到 `MsgAdopt` 時再 `serveCPU` |
| balance 的 node/system 層 | 直接搬 | leader 用快照決定來源，`Steal`；來源 shard 以同樣的 `firstN` 條件挑任務並 `Adopt` |
| active balance 跨 shard | `CMD_STOP` | `StopFor(src, pid, dst)` 給來源 shard，由它設 `migrateTo` 並送 STOP；回來的 enqueue 走 `Adopt` |
| cgroup group 彙總 `LoadAvg`、`NrAvg` | 一般欄位 | `atomic.Uint64`，與 PR 的 `group.load_avg().add()` 相同；`Queue` 仍是 per-CPU，屬於 CPU 的 shard |
| cpu.max pool | 一般欄位 | `atomic.Uint64` + CAS 借 slice；refill 由 shard 0 負責；`throttled[c]`、`local[c]`、backlog 依 CPU 分屬各 shard |
| 事件的 `Pid` 不屬於本 shard | — | 查 `owner[slot]`，`MsgForward` 給 owner |

### E5. 迴圈與喚醒

每個 shard 的迴圈就是 D2，多兩步：每輪先非阻塞 drain `inbox`（`select { case m := <-inbox: ... default: }`，一次最多 256 筆），結束前把本 shard 每顆 CPU 的 `CPUSnap` 更新一次。送訊息給別的 shard 後，若對方可能已 `sched_yield`，就 `StoreUint64(&needed_s[dst], 1)` 並用 `do_kick` 踢對方 shard 的一顆閒置 CPU（沒有就踢其排程執行緒所在 CPU），讓 BPF 把它的排程執行緒派出來。`Periodic` 由各 shard 自己跑；shard 0 額外負責 cpu.max refill、cgroupfs 輪詢與 GC 的全域部分。

### E6. 測試

- **確定性模擬**：`sim` 以單一 goroutine 輪流步進每個 shard，訊息佇列在步與步之間交付，結果可重現；T2 的所有情境在 `shards = nr_llc` 下重跑，公平性門檻不變。
- **所有權不變量**：每一步檢查每個任務恰好在一個 shard 的 `tasks` 中、`owner[slot]` 一致、每個 pack 的 `W` 等於其成員權重和。
- **競態**：真實 goroutine + `go test -race`，在 4 個 shard 間隨機移交 10^6 次。
- **VM**：`vng --cpus 64` 以兩種 `--shards` 跑 T4 的壓力與公平性列，比較事件吞吐、迴圈忙碌率與 wakeup latency。

## 測試與驗證計畫

四層：T1、T2 不需 root、跑在 CI；T3 需 clang；T4 在 `vng` VM 內以 root 執行。每個 Part C 模組都要有 T2 的對應測試才算完成。

### T1. 算術（`eevdf/accounting_test.go`）

`tests/accounting.rs` 全部 31 個測試逐一翻成 Go，數值不改：`weighted_deadline`、`preemption_preserves_unfinished_request`、`completion_and_yield_charge_renew_request`、`sleep_discards_remaining_deadline`、`placement_carries_remaining_request`、`reweight_rescales_remaining_service`、`wrapped_zero_deadline_remains_valid`、`zero_weight_is_guarded`、`joins_track_the_weighted_average`、`placement_preserves_lag_after_join`、`lag_is_clamped`、`charge_carries_the_remainder`、`projection_matches_a_charge`、`first_request_asks_for_half`、`deadlines_floor_the_weight`、`time_before_wraps`、`delayed_debt_is_paid_by_the_pack`、`zero_lag_joiner_stays_eligible`、`util_estimate_rises_at_once_and_falls_slowly`、`runnable_fraction_is_averaged_over_cycles`、`task_load_decays_while_asleep`、`wake_wide_needs_both_to_flip_often`、`wake_affine_weight_balances_the_two_cids`、`fork_groups_order_by_idle_then_load`、`restricted_forks_prefer_spare_capacity_and_the_local_group`、`avg_idle_moves_an_eighth_and_is_capped`、`newidle_cost_rises_at_once_and_decays_per_second`、`newidle_sampling_folds_and_weighs`、`cgroup_weights_follow_the_kernel`、`hierarchy_weight_is_rounded_once`、`concur_shares_scale_with_the_cids_in_use`（以及 helper `group`/`wins` 對應的 Go helper）。

### T2. 模組與模擬（`eevdf/*_test.go`、`eevdf/sim/`）

- **Tree**：10^5 步隨機 Insert/Remove/PickEligible/MinSlice/firstN，與 O(n) 暴力解比對，含 2^64 wrap。
- **Pack 不變量**：任意 join/leave/charge/place/reweight 序列後 `W == Σ joinedWeight`、`|V − Σw·joined/W| ≤ 成員數`；跨 pack 搬移前後 lag 不變（誤差 ≤ 1）。
- **離散事件模擬器 `sim`**：實作假 `IO`（N 顆 CPU、拓撲可設 SMT/LLC/node/asym、時鐘、每 CPU curr、slice 到期產生非 WAKEUP enqueue、hrtick、cpu\_release 注入、cgroup 樹）。斷言：

| 情境 | 斷言 |
| --- | --- |
| 同一 CPU：nice 0 vs nice −5 CPU-bound 各 1 | CPU 時間比 3121/1024 ± 5% |
| 8 CPU、2 LLC、24 個 CPU-bound | 每 LLC 的任務數差 ≤ 1（periodic balance）；任一 CPU 不會在有人排隊時閒置超過 1 個 balance interval |
| 互動任務（跑 1 ms 睡 9 ms）+ 8 CPU-bound | 喚醒到執行 p99 ≤ base slice；RUN\_TO\_PARITY 關閉時 preemption 次數上升 |
| fork 風暴 | `findIdlestFork` 把新任務分散到閒置 core |
| SMT：兩任務擠同核、另一核閒置 | 一個 balance interval 內 active balance 把其中一個搬走 |
| asym 容量（4 大 4 小） | 高 util 任務被 misfit 搬到大核 |
| cgroup A:B = 200:100，各 4 個 CPU-bound，4 CPU | A/B CPU 時間比 2.0 ± 5% |
| cpu.max 50ms/100ms，4 個 CPU-bound | 每 period 用量 ≤ quota + 一個 slice |
| 注入 50% RT 佔用到 CPU0 | CPU0 的 avail ≈ 512；被擠的任務搬離 |
| 任意事件序列 + 隨機丟 5% SLEEP/EXIT | 不 panic、`MissedSleep` 計數正確、GC 回收所有死任務 |
| 每個 feature 關閉 | 至少一個情境的觀測值改變（例：`--no-place-lag` 時反覆睡醒的任務 lag 會流失） |

### T3. BPF 回呼（clang + mock helper，照 qumun `scripts/test_bpf_progress.py`）

enqueue 送出的 `ev` 欄位正確且 `nr_queued` 先加後發布；ring 滿時 congested 路徑把任務插到原 CPU；`do_dispatch` 對 `enq_seq` 不符、affinity 不符、`expect_curr` 不符三種情況的處理；`ge_dispatch` 在 `nr_queued == 0` 時續跑並送 EV\_KEEP；`hrtick_fire` 只在 `run_seq` 相同且有排隊時 kick；`core_sched_before` 在 seqcount 為奇數或 pid 不符時退回 FIFO；`cpu_release/acquire` 累計 `displaced_ns`。

### T4. VM 整合（`scripts/`，`vng --cpus 8 --memory 4G`）

| 測試 | 來源 | 通過條件 |
| --- | --- | --- |
| `smoke.sh`：載入、attach、detach、權重 | PR `tests/smoke.sh` | 無 UEI 錯誤退出；內含 `fairness.py --nice 0 6 --workers 1` 比值 2.5–5.0 |
| `fairness.py` | PR `tests/fairness.py` | 預設 nice 0 vs 5，比值 2.0–4.5 |
| `yield.py` | PR `tests/yield.py` | 反覆 `sched_yield` 的任務依 forfeit 比例落後 |
| `churn.sh`：fork/exit 風暴 | 對應 PR `tests/arena.sh` | slot free list 不漏、`members` 回到背景數量 |
| cgroup 權重 | 新 | 兩個 cgroup `cpu.weight` 200:100 各綁 2 個 CPU-bound，比值 2.0 ± 10% |
| cpu.max | 新 | `cpu.max "50000 100000"` 的 group 用量 50% ± 5% |
| pressure | 新 | `chrt -f 50` 的 RT hog 佔一顆 CPU 50% 時，`avail` < 600 且 CFS 任務搬離 |
| IRQ pressure | 新 | `stress-ng --irq` 下對應 CPU 的 `avail` 下降 |
| hrtick | 新 | `schbench` p99 wakeup latency 在 `--no-hrtick` 時上升 |
| core scheduling | 新 | `prctl(PR_SCHED_CORE)` 建兩個 cookie，同核 sibling 不同時跑不同 cookie，且公平 |
| 壓力與長跑 | 新 | `stress-ng -c $(nproc)*2`、`hackbench`、10 分鐘桌面負載：不觸發 5 s watchdog，`nr_ev_drops == 0` |

比較基準：同一 VM 跑核心內建 EEVDF（不載入 sched\_ext）與 scx\_goeevdf，記錄同一組指標；有 cid-form sched\_ext 的核心時再加 scx\_reevdf。

## 給其他代理人的任務拆分與驗收標準

十六個任務。純 Go 線（A\*）只依賴前一個 A 任務與模擬器，可以在沒有 BPF 的情況下完成並驗證；BPF 線（B\*）與 Go 線平行；整合（D、V\*）最後。A1、A2、B1、S1 可同時開工。

| 任務 | 範圍 | 依賴 | 驗收 | Phase |
| --- | --- | --- | --- | --- |
| A1 算術 | C1、C1b | 無 | T1 的 31 個測試全過；`go vet` 乾淨 | 1 |
| A2 EDQ 與拓撲 | C2（含 MinSlice、firstN）、`topo.go` | 無 | Tree property test 10^5 步；sysfs 解析對 3 種拓撲樣本（SMT、多 LLC、asym）正確 | 1 |
| S1 模擬器骨架 | `eevdf/sim`：假 IO、時鐘、CPU、slice 到期、事件注入 | 無 | 可跑「全部 CPU-bound」情境並輸出每任務 CPU 時間 | 1 |
| A3 核心 EEVDF | C3、C4、C5 | A1、A2、S1 | Pack 不變量；T2 表格前 3 列；DELAY\_DEQUEUE 與 yield 單元測試 | 1 |
| A4 放置 | C6 | A3 | fork 風暴與互動任務情境；wake\_affine 單元測試覆蓋 sync / wide / idle 三條路 | 1 |
| B1 BPF 資料平面 | A1.1–A1.6 | 無 | verifier 通過（load 不 attach）；T3 enqueue/dispatch/續跑案例 | 1 |
| B2 BPF 核心內機制 | A2.1–A2.6 | B1 | verifier 通過；T3 hrtick、core\_sched、cpu\_release 案例；舊核心缺回呼時能 DisableOp 載入 | 1–4 |
| B3 Go 綁定 | Part B | B1 | 所有 wire 斷言編譯；Consume/Submit 往返 privileged 測試；`go test -race ./internal/...` | 1 |
| D1 主程式 | Part D | A4、B3 | VM 內 `smoke.sh` 通過 | 1 |
| A5 負載、pressure、cpufreq | C7、C13 | A3 | 「50% RT 佔用」情境 avail ≈ 512；util\_est 單元測試 | 2 |
| A6 三種 balance | C8、C9、C10 | A4、A5 | T2 的 LLC 平衡、SMT、asym 三列 | 2 |
| A7 cgroup 與 cpu.max | C11、C12 | A3、A5 | T2 cgroup 與 cpu.max 兩列；cgroupfs 輪詢在無回呼核心可用 | 3 |
| A8 hrtick、core sched、proxy、features | C14 | A3、B2 | 每個 feature 關閉都有觀測差異；CLI 與 scx\_reevdf `--help` 一一對應 | 4 |
| V1 BPF 測試 harness | T3 | B1、B2 | 所有 T3 案例自動化，進 CI | 1–4 |
| V2 VM 整合 | T4 全部 | D1 + 各 Phase 的 A 任務 | T4 表格逐列通過，結果貼回 PR 描述 | 每期結束 |
| E1 分片 | Part E：per-shard ring 與排程執行緒、Remote 介面、移交與 steal 訊息、全域 atomic 彙總 | A6、A7、B2、D1 | E6 確定性模擬在 shards = nr\_llc 下通過全部 T2 情境；所有權不變量每步成立；-race 10^6 次移交乾淨；64 CPU VM 比較報告 | 5 |

### 每個代理人都要拿到的共同脈絡

- 本文件的「目標與範圍」「整體設計」與自己負責的段落全文。
- 規則：`eevdf/` 不得 import cgo 或 `internal/bpf`；新的 wire struct 必須有雙向編譯期斷言；熱路徑只用 `bridge.Call`，可能阻塞的 C 呼叫用一般 cgo；排程 goroutine 是唯一能碰 `Policy` 的地方；熱路徑不配置記憶體。
- 每個 Go 檔案開頭註解寫出對應的 PR 檔案與 fair.c 函式名稱；PR 的每個 `//!` 區塊說明是該檔案的規格來源。
- 參考原始碼：PR #3926 commit `aa45a29` 的 `scheds/experimental/scx_reevdf/bpf/src/*.rs` 與 `tests/`；qumun commit `5cb4a45` 的 `hotpath.c`、`goland_core/hotpath.go`、`internal/schedbridge/`、`ebpf/main.bpf.c`（心跳 timer、SCHED\_DSQ、`usersched_has_pending_tasks`）。
- 有疑義時以 fair.c 行為為準，其次 scx\_reevdf，最後才是本文件的簡化。

## 來源

- [sched-ext/scx PR #3926](https://github.com/sched-ext/scx/pull/3926)：scx\_reevdf，commit `aa45a29`，讀過 `bpf/src/` 全部模組、`src/main.rs`（CLI 預設值）與 `tests/`
- [Gthulhu/qumun](https://github.com/Gthulhu/qumun)：commit `5cb4a45`，讀過 `ebpf/main.bpf.c`、`hotpath.c`、`goland_core/`、`internal/schedbridge/`、`Makefile`
