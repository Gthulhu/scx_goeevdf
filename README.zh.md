# scx_goeevdf

[English](./README.md) | [繁體中文](./README.zh.md)

`scx_goeevdf` 是以 Go 實作的實驗性 Linux [`sched_ext`](https://www.kernel.org/doc/html/latest/scheduler/sched-ext.html) 使用者空間 CPU 排程器，實作源自 Linux `fair.c` 的 **EEVDF（Earliest Eligible Virtual Deadline First）** 機制（設計對照 [`scx_reevdf`](https://github.com/sched-ext/scx/pull/3926)，並借鏡 [`Gthulhu/qumun`](https://github.com/Gthulhu/qumun) 的 eBPF / cgo 通訊技巧）。

排程決策與狀態——包含 Pack 虛擬時間、Eligible-Deadline 樹、放置、負載平衡、cgroup 權重與配額、容量壓力及 cpufreq 目標——由 Go 套件（`eevdf/`）維護。eBPF 程式（`bpf/goeevdf.bpf.c`）負責事件傳送、派發指令與核心同步回呼。

**目前並非完整移植，也尚未達到正式環境可用的成熟度。**
已實作部分、量測結果與剩餘工作見 [ALIGNMENT.md](./ALIGNMENT.md)。
完整放置策略、跨分片群組負載、`cpu.max.burst`、動態分片配額、監控，
以及完整模擬／VM 驗收矩陣仍待完成。

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
| **3** | **使用者指令環形緩衝區** (`BPF_MAP_TYPE_USER_RINGBUF`，`cmds_0..31`) | **Go → eBPF** | `ge_dispatch` 消費指令，直接放入 `SCX_DSQ_LOCAL_ON \| cpu` 或停止任務 | `Sched.SubmitShard(s, &cmd)` 送出 64-byte DISPATCH、STOP、KICK 指令 | 傳送 EEVDF 決策 |
| **4** | **Syscall 觸發程式** (`SEC("syscall")` 搭配 `BPF_PROG_TEST_RUN`) | **Go → eBPF** | `ge_kick_prog` 呼叫 `scx_bpf_kick_cpu`；`ge_hrtick_prog` 設定受 run-sequence 保護的計時器 | `Sched.Kick(cpu, flags)`；`SubmitShard` 遇到 `CmdHrtick` 直接呼叫 hrtick syscall，不走指令環形緩衝區 | 喚醒／搶佔 CPU 與設定計時器；沒有獨立的 Go `Hrtick` 方法 |

### 閉迴路排程生命週期（Step-by-Step）

1. **排程執行緒豁免與專屬 DSQ**：在呼叫 `Load()` 前，Go 會將自身行程的 `usersched_tgid` 以及 CPU 對應的分片表（`cpu_shard[]`、`nr_shards`）寫入 `.rodata`。每條綁定 OS 執行緒（`runtime.LockOSThread()`）的 Go 排程迴圈會將其 Linux TID 註冊進 `usersched_tids` 雜湊表。當排程執行緒本身被喚醒時，eBPF 會直接將它排入專屬的 `SCHED_DSQ_BASE + shard`，避免排程器等待自己而發生死鎖。
2. **任務入列**：一般任務進入 `ge_enqueue` 後被標記為等待、增加分片積壓並送出 `EV_ENQUEUE`。目前沒有一般性的 idle-CPU 直送快速路徑；事件環形緩衝區壅塞時才以核心本地派發維持進度。
3. **Go 端 EEVDF 決策與狀態發布**：每批最多消費 512 筆事件，更新各 CPU 的 `Pack` 與擴充 treap（`eevdf/tree.go`），計算虛擬時間、Deadline 與搶佔，並透過 atomic 發布 CPU 控制資訊與任務比較鍵。
4. **指令派發與核心 DSQ 消費**：Go 送出 `CMD_DISPATCH` 並 kick 目標 CPU。`ge_dispatch` 檢查等待狀態、enqueue sequence 與 affinity，直接放入 `SCX_DSQ_LOCAL_ON | cpu`。`SCHED_DSQ_BASE + shard` 是排程執行緒專用的獨立佇列。
5. **核心同步協作機制**：
   - **時間片續跑（`keep_running`）**：`ge_dispatch` 可依 Go 的 `keep_slice` 授權補充前一任務的時間片並送出 `EV_KEEP`；不是在 `ge_tick`／`ge_stopping` 補充。
   - **高解析度搶佔計時器（`hrtick`）**：若指令帶有 `CMDF_HRTICK` 或由 `ge_hrtick_prog` 設定，eBPF 會啟動該 CPU 的 `bpf_timer`（`BPF_F_TIMER_ABS`），在精確的搶佔時間點觸發 `scx_bpf_kick_cpu(cpu, SCX_KICK_PREEMPT)`。
   - **Core Scheduling 與除錯傾印**：`ge_core_sched_before` 與 `ge_dump_task` 會在核心回呼中直接以 Seq-lock 讀取 Go 發布在 `task_tbl[slot]` 的比較鍵與排程狀態。

---

## 核心特性

- **EEVDF 機制（`eevdf/accounting.go`, `eevdf/pack.go`, `eevdf/tree.go`）**：
  - 每顆 CPU 獨立維護 `Pack` 加權虛擬時間 $V$（含餘數累積防止截斷誤差）。
  - 擴充 treap 以子樹最小 `vruntime` 剪枝，具預期 $O(\log N)$ 查找成本並重用節點；不是紅黑樹。
  - 支援 Lag 夾限、`PLACE_LAG`、`PLACE_REL_DEADLINE`、`RUN_TO_PARITY` 時間片保護、`PREEMPT_SHORT` 短請求搶佔，以及 `DELAY_DEQUEUE` / `DELAY_REQUEUE` 睡眠負債償還。
- **拓撲感知放置與三階段負載平衡（`eevdf/placement.go`, `eevdf/balance.go`, `eevdf/newidle.go`, `eevdf/active.go`）**：
  - 涵蓋 SMT Core、LLC、NUMA Node 到全系統的階層式喚醒選核（`WAKE_SYNC`、`WA_WEIGHT`、`SIS_UTIL`）。
  - 支援 Newly-Idle 拉取（`newidle`）、週期性負載平衡，以及主動遷移（Active Balance，透過 `CMD_STOP` 停下遠端任務後重新派發）。
- **Cgroup v2 權重與 `cpu.max` 頻寬控制（`eevdf/cgroup.go`, `eevdf/bandwidth.go`）**：
  - 階層式任務群組有效權重折算（`cpu.weight`）。
  - 支援 `cpu.max` 配額／週期、節流與解凍及共享配額池。尚未實作 `cpu.max.burst` 與動態多分片配額更新。
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

- **Linux 核心**：啟用 `CONFIG_SCHED_CLASS_EXT=y` 與 BTF（`/sys/kernel/btf/vmlinux`）。
  已於 Ubuntu `6.14.0-37-generic` 驗證載入與掛載；不保證所有 6.12 以後的
  sched_ext 核心均相容。排程旗標由執行中核心 BTF 匯入；缺少
  `SCX_ENQ_BLOCKED` 時明確停用 proxy execution，選用 cgroup 回呼在編譯／載入時偵測。
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
# 單元測試與確定性模擬（不需 root 權限）
go test ./...
go test -race ./...
python3 scripts/test_bpf_progress.py

# 僅於獨立 VM 驗證載入、attach、公平性與 detach
go test -c -o bin/bpf-load.test ./internal/bpf
sudo sh scripts/test_vm.sh /boot/vmlinuz-6.14.0-37-generic
```

### 啟動排程器

請使用可拋棄的測試 VM，不要在共用主機掛載。載入與掛載需要足夠的
BPF／排程權限；啟動時會鎖定目前及未來配置的記憶體，memlock 不足即回報錯誤：

```bash
sudo sh -c 'ulimit -l unlimited; exec ./bin/goeevdf'
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
  -no-vref-update
        停用執行中任務的 reference-time 投影
  -no-wake-sync
        停用同步喚醒的 wake-affine 放置
  -no-wa-weight
        停用加權 wake-affine 放置
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
