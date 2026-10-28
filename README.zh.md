# scx_goeevdf

[English](./README.md) | [繁體中文](./README.zh.md)

`scx_goeevdf` 是一個以 Go 語言實作的 Linux [`sched_ext`](https://www.kernel.org/doc/html/latest/scheduler/sched-ext.html) 使用者空間 CPU 排程器，完整移植了 Linux 核心 `fair.c` 的 **EEVDF（Earliest Eligible Virtual Deadline First）** 排程演算法（設計對照 [`scx_reevdf`](https://github.com/sched-ext/scx/pull/3926)，並借鏡 [`Gthulhu/qumun`](https://github.com/Gthulhu/qumun) 的 eBPF / cgo 通訊技巧）。

所有排程決策與狀態——包含 per-CPU Pack 虛擬時間、擴充型 Eligible-Deadline 樹（EDQ）、拓撲感知的喚醒放置（Placement）、三階段負載平衡、cgroup v2 階層權重、`cpu.max` 頻寬限流、容量壓力（Capacity Pressure）以及 `cpufreq` 頻率調整——全部在純 Go 套件（`eevdf/`）中維護並具備完整單元測試；核心端的 eBPF 程式（`bpf/goeevdf.bpf.c`）則作為精簡、非阻塞的資料平面（Data Plane），負責將核心事件送上使用者空間、執行 Go 下達的派發指令，並處理必須在核心回呼內同步完成的快速路徑。

---

## 架構設計：eBPF 程式如何與 Golang 應用互動

![eBPF 與 Go 排程器互動流程（Manim 動畫）](./docs/assets/ebpf_go_interaction.gif)

<details>
<summary>檢視高解析度靜態架構圖 (PNG)</summary>

![eBPF 與 Go 排程器互動架構圖 (Static)](./docs/assets/ebpf_go_interaction.png)

</details>

> 上方互動流程圖與動畫由 [Manim 腳本 (`docs/manim/ebpf_go_interaction.py`)](./docs/manim/ebpf_go_interaction.py) 繪製產生。

### 核心與 Go 應用之間的四條互動通道

`scx_goeevdf` 透過四種零拷貝（Zero-Copy）或非阻塞通道，將核心端的機制與 Go 使用者空間的排程策略解耦：

| # | 互動通道 | 方向 | eBPF 端 (`bpf/goeevdf.bpf.c`) | Go 應用端 (`internal/bpf` & `cmd/goeevdf`) | 核心職責 |
|---|---|---|---|---|---|
| **1** | **事件環形緩衝區** (`BPF_MAP_TYPE_RINGBUF`，置於 `ev_rings` `ARRAY_OF_MAPS`) | **eBPF → Go** | `ev_reserve()` + `bpf_ringbuf_submit()` 寫入 128-byte 的 `struct ev` 事件（`EV_ENQUEUE`、`EV_SLEEP`、`EV_DEQUEUE`、`EV_YIELD`、`EV_IDLE`、`EV_CPUMASK`、`EV_CGRP_*`） | `Sched.ConsumeShard(s, &ev)` 以非阻塞方式逐筆取出事件（`ring_buffer__consume`） | 將任務喚醒、時間片耗盡、睡眠／退出、CPU 進入閒置、以及 cgroup 變更等事件即時串流給對應的 Go 分片 |
| **2** | **共享記憶體表** (`BPF_F_MMAPABLE` 陣列 + `.bss` + `.rodata`) | **雙向（零拷貝 `mmap`）** | 直接讀寫 `cpu_tbl`、`ctl_tbl`、`task_tbl` 與 `.bss` 計數器（`nr_queued_s`、`nr_scheduled_s`、`nr_ev_drops`） | 以 `syscall.Mmap` 將 BPF map 直接映射為 Go slice（`s.CPU`、`s.Ctl`、`s.Task`、`s.bss`），搭配 `sync/atomic` 讀寫 | 免除系統呼叫的雙向狀態同步：BPF 回報當前執行 PID、`run_seq`、idle 狀態與 RT/DL 搶佔時間（`cpu_tbl`）；Go 發布 `nr_queued`、`keep_slice`、`perf_target`（`ctl_tbl`）與帶 Seq-lock 的任務虛擬時間／Deadline（`task_tbl`） |
| **3** | **使用者指令環形緩衝區** (`BPF_MAP_TYPE_USER_RINGBUF`，`cmds_0..31`) | **Go → eBPF** | `ge_dispatch` 內由 `drain_shard()` 呼叫 `bpf_user_ringbuf_drain(&cmds_s, apply_cmd, ...)`，將任務插入每顆 CPU 的 DSQ（`DSQ_BASE + cpu`）或停下執行中任務 | `Sched.SubmitShard(s, &cmd)` 寫入並送出 64-byte 的 `struct cmd` 指令（`CMD_DISPATCH`、`CMD_STOP`、`CMD_KICK`） | 將 Go EEVDF 決策出的排程結果（目標 `pid`、`cpu`、`slice`、`enq_seq`、`slot`、`CMDF_HRTICK`）批次送回核心 |
| **4** | **Syscall 觸發程式** (`SEC("syscall")` 搭配 `BPF_PROG_TEST_RUN`) | **Go → eBPF** | `ge_kick_prog` 執行 `scx_bpf_kick_cpu(cpu, flags)`；`ge_hrtick_prog` 透過 `hrtick_arm_at()` 設定每 CPU 的 `bpf_timer` | `Sched.Kick(cpu, flags)` 與 `Sched.Hrtick(cpu, runSeq, when)` | 當 Go 送出搶佔／派發指令後立即喚醒或搶佔目標 CPU，或直接啟動高精確度 `hrtick` 計時器 |

### 閉迴路排程生命週期（Step-by-Step）

1. **排程執行緒豁免與專屬 DSQ**：在呼叫 `Load()` 前，Go 會將自身行程的 `usersched_tgid` 以及 CPU 對應的分片表（`cpu_shard[]`、`nr_shards`）寫入 `.rodata`。每條綁定 OS 執行緒（`runtime.LockOSThread()`）的 Go 排程迴圈會將其 Linux TID 註冊進 `usersched_tids` 雜湊表。當排程執行緒本身被喚醒時，eBPF 會直接將它排入專屬的 `SCHED_DSQ_BASE + shard`，避免排程器等待自己而發生死鎖。
2. **任務入列與核心快速路徑（Fast Path）**：當一般任務進入 `ge_enqueue` 時：
   - 若目標 CPU 目前閒置且該分片沒有待處理積壓（`nr_queued_s[shard] == 0` 且 `nr_scheduled_s[shard] == 0`），eBPF 會直接把任務派發至 `SCX_DSQ_LOCAL` 立即執行，同時送出 `EV_ENQUEUE` 讓 Go 端的 EEVDF 狀態保持同步。
   - 否則，eBPF 將任務標記為 `waiting = true`、遞增 `nr_queued_s[shard]`、透過 `ev_rings[shard]` 送出 `EV_ENQUEUE`，並在需要時喚醒該分片的 Go 排程執行緒。
3. **Go 端 EEVDF 決策與狀態發布**：Go 主迴圈每輪最多非阻塞讀取 512 筆事件，更新各 CPU 的 `Pack`（加權平均虛擬時間 $V$ 與總權重 $W$）與擴充紅黑樹（`eevdf/tree.go`），計算 Lag、Virtual Deadline 與喚醒搶佔，並透過 `atomic` 更新 `ctl_tbl[cpu]`（`nr_queued`、`keep_slice`）與 `task_tbl[slot]`（`core_vtime`、`vruntime`、`deadline`、`lag`）。
4. **指令派發與核心 DSQ 消費**：Go 將 `CMD_DISPATCH` 寫入 `cmds_s`，若需要搶佔或喚醒閒置 CPU 則呼叫 `ge_kick_prog`。目標 CPU 進入 `ge_dispatch` 時會先排空 `cmds_s`，驗證 `t->waiting && t->enq_seq == c->enq_seq`（自動丟棄過期指令以防誤派），再呼叫 `scx_bpf_dsq_insert()` 放入 `DSQ_BASE + c->cpu` 並以 `scx_bpf_dsq_move_to_local()` 移交執行。
5. **核心同步協作機制**：
   - **時間片續跑（`keep_running`）**：當任務在 `ge_stopping` 或 `ge_tick` 耗盡時間片，若 Go 已在 `ctl_tbl[cpu].keep_slice` 授權續跑，eBPF 會在核心內直接補充 `p->scx.slice` 並回報 `EV_KEEP`，免去使用者空間來回延遲。
   - **高解析度搶佔計時器（`hrtick`）**：若指令帶有 `CMDF_HRTICK` 或由 `ge_hrtick_prog` 設定，eBPF 會啟動該 CPU 的 `bpf_timer`（`BPF_F_TIMER_ABS`），在精確的搶佔時間點觸發 `scx_bpf_kick_cpu(cpu, SCX_KICK_PREEMPT)`。
   - **Core Scheduling 與除錯傾印**：`ge_core_sched_before` 與 `ge_dump_task` 會在核心回呼中直接以 Seq-lock 讀取 Go 發布在 `task_tbl[slot]` 的比較鍵與排程狀態。

---

## 核心特性

- **完整 EEVDF 演算法實作（`eevdf/accounting.go`, `eevdf/pack.go`, `eevdf/tree.go`）**：
  - 每顆 CPU 獨立維護 `Pack` 加權虛擬時間 $V$（含餘數累積防止截斷誤差）。
  - 擴充型 Eligible-Deadline 樹（以子樹最小 `vruntime` 剪枝），可在 $O(\log N)$ 內找出具資格（Eligible）且虛擬截止時間最早的任務。
  - 支援 Lag 夾限、`PLACE_LAG`、`PLACE_REL_DEADLINE`、`RUN_TO_PARITY` 時間片保護、`PREEMPT_SHORT` 短請求搶佔，以及 `DELAY_DEQUEUE` / `DELAY_REQUEUE` 睡眠負債償還。
- **拓撲感知放置與三階段負載平衡（`eevdf/placement.go`, `eevdf/balance.go`, `eevdf/newidle.go`, `eevdf/active.go`）**：
  - 涵蓋 SMT Core、LLC、NUMA Node 到全系統的階層式喚醒選核（`WAKE_SYNC`、`WA_WEIGHT`、`SIS_UTIL`）。
  - 支援 Newly-Idle 拉取（`newidle`）、週期性負載平衡，以及主動遷移（Active Balance，透過 `CMD_STOP` 停下遠端任務後重新派發）。
- **Cgroup v2 權重與 `cpu.max` 頻寬控制（`eevdf/cgroup.go`, `eevdf/bandwidth.go`）**：
  - 階層式任務群組有效權重折算（`cpu.weight`）。
  - 支援 `cpu.max` 配額／週期／Burst 權杖桶限流、節流佇列凍結與解凍，以及跨分片共享配額池（`SharedQuotaPool`）。
- **容量壓力追蹤與動態頻率調整（`eevdf/pressure.go`, `eevdf/cpufreq.go`）**：
  - 結合 `cpu_release`/`cpu_acquire` 與 `/proc/stat` 追蹤 RT/DL/IRQ/Steal 對可用算力的侵蝕。
  - 依據 CPU 使用率與負載動態計算目標頻率，透過 `scx_bpf_cpuperf_set` 套用。
- **Per-LLC 多分片擴展架構（`internal/shard/`, `cmd/goeevdf/sharded.go`）**：
  - 在超過 64 顆可能 CPU 的大型機器上可自動依 LLC 切分排程分片（`-shards auto`），每個分片擁有獨立的 eBPF 事件／指令環、OS 執行緒與 `eevdf.Policy`，透過無鎖快照與顯式所有權移交（Handoff / Steal）協作。

---

## 專案目錄結構

| 路徑 | 說明 |
|---|---|
| [`bpf/intf.h`](./bpf/intf.h) | C 與 Go 共用的傳輸結構定義（`struct ev`、`struct cmd`、`struct cpu_slot`、`struct cpu_ctl`、`struct task_pub`） |
| [`bpf/goeevdf.bpf.c`](./bpf/goeevdf.bpf.c) | `sched_ext` `struct_ops` eBPF 程式、每分片環形緩衝區、可 `mmap` 共享表、`hrtick` 計時器與 syscall 程式 |
| [`internal/bpf/`](./internal/bpf) | `libbpf` C 語言橋接層（`bridge.c`、`bridge.h`）與 Go 封裝（`sched.go`、`wire.go`） |
| [`eevdf/`](./eevdf) | 純 Go EEVDF 排程核心（虛擬時間算術、Pack、EDQ 樹、選核、搶佔、負載平衡、cgroup、頻寬限流、壓力、cpufreq、hrtick、CoreSched） |
| [`internal/shard/`](./internal/shard) | Per-LLC 分片協調器、任務 Slot 所有權路由、跨分片 Handoff 與工作竊取（Steal）引擎 |
| [`cmd/goeevdf/`](./cmd/goeevdf) | CLI 進入點、sysfs 拓撲與 cgroup 讀取器、單迴圈與多分片執行時期迴圈 |
| [`docs/manim/`](./docs/manim) | 使用 Manim 繪製 eBPF ↔ Go 互動架構圖與動畫的 Python 腳本 |

---

## 編譯與執行

### 環境需求

- **Linux 核心**：6.12 以上，啟用 `CONFIG_SCHED_CLASS_EXT=y`（`sched_ext`）與 BTF（`/sys/kernel/btf/vmlinux`）
- **建置工具鏈**：
  - Go 1.24+
  - `clang`（支援 `-target bpf`）
  - `bpftool`
  - `libbpf`（`libbpf.so.1` 與標頭檔）、`libelf`、`zlib`

### 編譯專案

```bash
make build
```

此指令會自動從當前核心的 `/sys/kernel/btf/vmlinux` 產生 `bpf/vmlinux.h`、編譯 `bpf/goeevdf.bpf.o`，並在 `bin/goeevdf` 產出 Go 排程器執行檔。

### 執行測試

```bash
# 執行所有單元測試與並發模擬測試（不需 root 權限）
go test ./...

# 執行需要載入核心 BPF 程式的整合測試（需 root 或 CAP_BPF 權限）
sudo GOEEVDF_TEST_BPF_LOAD=1 go test -v ./internal/bpf
```

### 啟動排程器

載入並掛載 `sched_ext` 排程器需要 `root` 權限（或 `CAP_BPF` + `CAP_SYS_ADMIN`）：

```bash
sudo ./bin/goeevdf
```

#### 常用命令列參數

```text
  -bpf-object string
        指定編譯好的 sched_ext BPF 物件檔路徑 (預設 "bpf/goeevdf.bpf.o")
  -shards string
        超過 64 顆可能 CPU 時的 per-LLC 分片模式："auto" 或 "1" (預設 "auto")
  -no-wakeup-preempt
        停用喚醒搶佔 (WAKEUP_PREEMPT)
  -no-eligibility
        停用 EEVDF Eligibility 資格檢查
  -no-eligible-scan
        停用 Eligible 子樹快速搜尋
  -no-run-to-parity
        停用 RUN_TO_PARITY 時間片保護
  -no-preempt-short
        停用短請求搶佔 (PREEMPT_SHORT)
  -no-place-lag
        停用放置時的 Lag 還原 (PLACE_LAG)
  -no-place-rel-deadline
        停用遷移時的相對截止時間保留 (PLACE_REL_DEADLINE)
  -no-delay-dequeue
        停用負 Lag 延遲出列 (DELAY_DEQUEUE)
  -no-delay-requeue
        停用延遲重入列 (DELAY_REQUEUE)
  -no-proxy-exec
        停用 Proxy Execution 阻塞捐贈處理
  -no-hrtick
        停用高精確度搶佔計時器 (HRTICK)
  -no-sis-util
        停用使用率感知的閒置選核 (SIS_UTIL)
  -no-newidle-cost
        停用 newidle 搜尋成本上限
  -no-newidle-sampling
        停用 newidle 抽樣搜尋
  -no-capacity-pressure
        停用容量壓力折減
  -disable-cgroups
        停用 cgroup 權重與配額支援
  -disable-cpu-max
        停用 cgroup cpu.max 頻寬限流
  -disable-cpufreq
        停用 CPU 頻率調整更新
```

---

## 重新渲染 Manim 架構圖與動畫

若修改了 [`docs/manim/ebpf_go_interaction.py`](./docs/manim/ebpf_go_interaction.py)，可透過以下指令重新產生 `docs/assets/` 下的圖片與動畫：

```bash
# 渲染高解析度靜態架構圖 (PNG)
manim -qh -s docs/manim/ebpf_go_interaction.py EbpfGoInteraction

# 渲染動畫影片並轉為最佳化調色盤 GIF
manim -qm docs/manim/ebpf_go_interaction.py EbpfGoInteraction
```
