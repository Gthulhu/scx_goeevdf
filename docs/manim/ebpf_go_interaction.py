"""Manim visualization of how scx_goeevdf's eBPF data plane interacts with the Go control plane.

Render static overview PNG:
    manim -qk -s docs/manim/ebpf_go_interaction.py EbpfGoInteraction

Render animated GIF:
    manim -ql --format=gif docs/manim/ebpf_go_interaction.py EbpfGoInteraction
"""

from manim import *


class EbpfGoInteraction(Scene):
    def construct(self):
        self.camera.background_color = "#0D1117"

        # Colors
        C_BG_KERNEL = "#121D2F"
        C_BORDER_KERNEL = "#388BFD"
        C_BG_GO = "#122620"
        C_BORDER_GO = "#2EA043"
        C_BG_MEM = "#271E14"
        C_BORDER_MEM = "#D29922"
        C_EV = "#58A6FF"
        C_CMD = "#3FB950"
        C_MMAP = "#F0883E"
        C_SYS = "#BC8CFF"
        C_TEXT = "#E6EDF3"
        C_MUTED = "#8B949E"

        # Title
        title = Text(
            "scx_goeevdf: eBPF Data Plane  <->  Go EEVDF Control Plane",
            font="DejaVu Sans Mono",
            font_size=24,
            color=C_TEXT,
            weight=BOLD,
        ).to_edge(UP, buff=0.28)

        # Left Box: Kernel / eBPF
        kernel_box = RoundedRectangle(
            corner_radius=0.18,
            width=4.5,
            height=6.2,
            fill_color=C_BG_KERNEL,
            fill_opacity=0.95,
            stroke_color=C_BORDER_KERNEL,
            stroke_width=2.5,
        ).move_to([-4.55, -0.35, 0])

        kernel_hdr = Text(
            "Linux Kernel (sched_ext eBPF)",
            font="DejaVu Sans",
            font_size=18,
            color=C_EV,
            weight=BOLD,
        ).next_to(kernel_box.get_top(), DOWN, buff=0.22)

        def make_subbox(w, h, title_str, items, border_col, fill_col="#161B22"):
            box = RoundedRectangle(
                corner_radius=0.1,
                width=w,
                height=h,
                fill_color=fill_col,
                fill_opacity=0.95,
                stroke_color=border_col,
                stroke_width=1.6,
            )
            t = Text(
                title_str,
                font="DejaVu Sans Mono",
                font_size=14,
                color=C_TEXT,
                weight=BOLD,
            )
            lines = VGroup(
                *[
                    Text(line, font="DejaVu Sans Mono", font_size=11.5, color=C_MUTED)
                    for line in items
                ]
            ).arrange(DOWN, aligned_edge=LEFT, buff=0.07)
            content = VGroup(t, lines).arrange(DOWN, aligned_edge=LEFT, buff=0.1)
            content.move_to(box.get_center())
            return VGroup(box, content)

        k_ops = make_subbox(
            4.1,
            1.65,
            "struct_ops/ge_ops Callbacks",
            [
                "select_cpu / enqueue / dispatch",
                "running / stopping / quiescent",
                "yield / update_idle / cgroup_*",
                "core_sched_before / dump_task",
            ],
            C_BORDER_KERNEL,
        )
        k_fast = make_subbox(
            4.1,
            1.6,
            "Synchronous Kernel Fast Paths",
            [
                "Usersched Thread -> SCHED_DSQ_BASE+s",
                "Idle CPU Wakeup  -> SCX_DSQ_LOCAL",
                "keep_running     -> refill keep_slice",
                "hrtick bpf_timer -> SCX_KICK_PREEMPT",
            ],
            C_BORDER_KERNEL,
        )
        k_drain = make_subbox(
            4.1,
            1.55,
            "Command Execution & Syscalls",
            [
                "bpf_user_ringbuf_drain(cmds_s)",
                "  -> scx_bpf_dsq_insert(DSQ_BASE+cpu)",
                "SEC(\"syscall\") ge_kick_prog",
                "SEC(\"syscall\") ge_hrtick_prog",
            ],
            C_BORDER_KERNEL,
        )

        k_stack = (
            VGroup(k_ops, k_fast, k_drain)
            .arrange(DOWN, buff=0.18)
            .next_to(kernel_hdr, DOWN, buff=0.22)
        )
        kernel_group = VGroup(kernel_box, kernel_hdr, k_stack)

        # Right Box: Go Userspace
        go_box = RoundedRectangle(
            corner_radius=0.18,
            width=4.5,
            height=6.2,
            fill_color=C_BG_GO,
            fill_opacity=0.95,
            stroke_color=C_BORDER_GO,
            stroke_width=2.5,
        ).move_to([4.55, -0.35, 0])

        go_hdr = Text(
            "User Space (Go Scheduler)",
            font="DejaVu Sans",
            font_size=18,
            color=C_CMD,
            weight=BOLD,
        ).next_to(go_box.get_top(), DOWN, buff=0.22)

        g_bridge = make_subbox(
            4.1,
            1.45,
            "internal/bpf (libbpf + cgo)",
            [
                "Open / SetShards / Load / Attach",
                "ConsumeShard(s, &ev)  [non-blocking]",
                "SubmitShard(s, &cmd) + Kick / Hrtick",
            ],
            C_BORDER_GO,
        )
        g_eevdf = make_subbox(
            4.1,
            1.9,
            "eevdf.Policy (Pure Go Fair/EEVDF)",
            [
                "Per-CPU Pack (V, W) & Eligible Tree",
                "Lag, Virtual Deadline & Preemption",
                "DELAY_DEQUEUE & RUN_TO_PARITY",
                "Load Balance, Newidle, Active, Cpufreq",
                "Cgroup Weights, cpu.max & CoreSched",
            ],
            C_BORDER_GO,
        )
        g_shard = make_subbox(
            4.1,
            1.45,
            "Runtime Loop / Per-LLC Shards",
            [
                "runtime.LockOSThread() per shard",
                "internal/shard Coordinator (>64 CPUs)",
                "1ms Periodic Tick & /proc/stat Pressure",
            ],
            C_BORDER_GO,
        )

        g_stack = (
            VGroup(g_bridge, g_eevdf, g_shard)
            .arrange(DOWN, buff=0.18)
            .next_to(go_hdr, DOWN, buff=0.22)
        )
        go_group = VGroup(go_box, go_hdr, g_stack)

        # Center Channels (BPF Maps / Boundary)
        def make_channel(y_pos, title_str, sub_str, col):
            box = RoundedRectangle(
                corner_radius=0.1,
                width=3.6,
                height=1.12,
                fill_color=C_BG_MEM,
                fill_opacity=0.98,
                stroke_color=col,
                stroke_width=2.0,
            ).move_to([0, y_pos, 0])
            t1 = Text(
                title_str,
                font="DejaVu Sans Mono",
                font_size=13,
                color=col,
                weight=BOLD,
            )
            t2_lines = VGroup(
                *[
                    Text(line, font="DejaVu Sans Mono", font_size=10.5, color=C_TEXT)
                    for line in sub_str.split("\n")
                ]
            ).arrange(DOWN, aligned_edge=LEFT, buff=0.06)
            txt = VGroup(t1, t2_lines).arrange(DOWN, buff=0.09).move_to(box.get_center())
            return VGroup(box, txt)

        ch_ev = make_channel(
            1.85,
            "1. BPF_MAP_TYPE_RINGBUF",
            "ev_rings[s] : struct ev (128B)\nENQUEUE / SLEEP / YIELD / IDLE / CGRP",
            C_EV,
        )
        ch_mmap = make_channel(
            0.40,
            "2. BPF_F_MMAPABLE Arrays + .bss",
            "cpu_tbl (BPF->Go) | ctl_tbl (Go->BPF)\ntask_tbl (Seq-lock) | .bss counters",
            C_MMAP,
        )
        ch_cmd = make_channel(
            -1.05,
            "3. BPF_MAP_TYPE_USER_RINGBUF",
            "cmds_0..31 : struct cmd (64B)\nCMD_DISPATCH / CMD_STOP / CMD_KICK",
            C_CMD,
        )
        ch_sys = make_channel(
            -2.50,
            "4. BPF_PROG_TEST_RUN (syscall)",
            "ge_kick_prog   -> scx_bpf_kick_cpu\nge_hrtick_prog -> bpf_timer_start",
            C_SYS,
        )

        # Arrows connecting Kernel <-> Channels <-> Go
        arr_kw = dict(stroke_width=3.2, max_tip_length_to_length_ratio=0.18, buff=0.08)

        # 1. Event Ring: Kernel -> Go
        a_ev_1 = Arrow(
            [-2.3, 1.85, 0], ch_ev[0].get_left(), color=C_EV, **arr_kw
        )
        a_ev_2 = Arrow(
            ch_ev[0].get_right(), [2.3, 1.85, 0], color=C_EV, **arr_kw
        )

        # 2. Shared Mmap: Bidirectional zero-copy
        a_mm_1 = DoubleArrow(
            [-2.3, 0.40, 0], ch_mmap[0].get_left(), color=C_MMAP, **arr_kw
        )
        a_mm_2 = DoubleArrow(
            ch_mmap[0].get_right(), [2.3, 0.40, 0], color=C_MMAP, **arr_kw
        )

        # 3. User Ringbuf: Go -> Kernel
        a_cmd_1 = Arrow(
            [2.3, -1.05, 0], ch_cmd[0].get_right(), color=C_CMD, **arr_kw
        )
        a_cmd_2 = Arrow(
            ch_cmd[0].get_left(), [-2.3, -1.05, 0], color=C_CMD, **arr_kw
        )

        # 4. Syscall progs: Go -> Kernel
        a_sys_1 = Arrow(
            [2.3, -2.50, 0], ch_sys[0].get_right(), color=C_SYS, **arr_kw
        )
        a_sys_2 = Arrow(
            ch_sys[0].get_left(), [-2.3, -2.50, 0], color=C_SYS, **arr_kw
        )

        # Footer caption
        footer = Text(
            "Closed Loop: [1] Kernel emits task/CPU event -> [2] Go updates EEVDF tree & mmap tables -> [3] Go submits cmd -> [4] Syscall kicks CPU to drain DSQ",
            font="DejaVu Sans Mono",
            font_size=11.5,
            color=C_MUTED,
        ).to_edge(DOWN, buff=0.18)

        # Animate step-by-step so both GIF and final frame (-s) look clean
        self.play(FadeIn(title), FadeIn(kernel_group), FadeIn(go_group), run_time=0.8)
        self.play(
            FadeIn(ch_ev),
            GrowArrow(a_ev_1),
            GrowArrow(a_ev_2),
            run_time=0.7,
        )
        self.play(
            FadeIn(ch_mmap),
            FadeIn(a_mm_1),
            FadeIn(a_mm_2),
            run_time=0.7,
        )
        self.play(
            FadeIn(ch_cmd),
            GrowArrow(a_cmd_1),
            GrowArrow(a_cmd_2),
            run_time=0.7,
        )
        self.play(
            FadeIn(ch_sys),
            GrowArrow(a_sys_1),
            GrowArrow(a_sys_2),
            FadeIn(footer),
            run_time=0.7,
        )

        # Pulse packet animation across the 4 stages
        dot_ev = Dot(color=C_EV, radius=0.09).move_to([-2.2, 1.85, 0])
        self.play(MoveAlongPath(dot_ev, Line([-2.2, 1.85, 0], [2.2, 1.85, 0])), run_time=0.7)
        self.play(FadeOut(dot_ev, run_time=0.15))

        self.play(Indicate(ch_mmap[0], color=C_MMAP, scale_factor=1.03), run_time=0.5)

        dot_cmd = Dot(color=C_CMD, radius=0.09).move_to([2.2, -1.05, 0])
        self.play(MoveAlongPath(dot_cmd, Line([2.2, -1.05, 0], [-2.2, -1.05, 0])), run_time=0.7)
        self.play(FadeOut(dot_cmd, run_time=0.15))

        dot_sys = Dot(color=C_SYS, radius=0.09).move_to([2.2, -2.50, 0])
        self.play(MoveAlongPath(dot_sys, Line([2.2, -2.50, 0], [-2.2, -2.50, 0])), run_time=0.7)
        self.play(FadeOut(dot_sys, run_time=0.15))
        self.wait(0.8)
