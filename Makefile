CLANG ?= clang
BPFTOOL ?= $(firstword $(wildcard /usr/lib/linux-tools/*/bpftool))
BPFTOOL := $(if $(BPFTOOL),$(BPFTOOL),bpftool)
BPF_HOST_ARCH := $(shell uname -m)
BPF_ARCH ?= $(if $(filter x86_64 amd64,$(BPF_HOST_ARCH)),x86,$(if $(filter aarch64 arm64,$(BPF_HOST_ARCH)),arm64,$(BPF_HOST_ARCH)))
BPF_CFLAGS ?= -O2 -g -target bpf -D__TARGET_ARCH_$(BPF_ARCH) -Ibpf

.PHONY: all bpf go build skeleton clean
all: build

bpf: bpf/goeevdf.bpf.o

skeleton: internal/bpf/goeevdf.skel.h

go: bpf
	mkdir -p bin
	CGO_ENABLED=1 go build -ldflags "-checklinkname=0" -o bin/goeevdf ./cmd/goeevdf

build: go

bpf/vmlinux.h: /sys/kernel/btf/vmlinux
	$(BPFTOOL) btf dump file /sys/kernel/btf/vmlinux format c > $@

bpf/goeevdf.bpf.o: bpf/goeevdf.bpf.c bpf/intf.h bpf/vmlinux.h Makefile
	@features=; \
	if printf '%s\n' '#include "vmlinux.h"' \
		'int probe(void) { return ((struct sched_ext_ops *)0)->cgroup_set_bandwidth != 0; }' | \
		$(CLANG) -x c -fsyntax-only -Ibpf - >/dev/null 2>&1; then \
		features="$$features -DGOEEVDF_HAVE_CGROUP_SET_BANDWIDTH"; \
	fi; \
	if printf '%s\n' '#include "vmlinux.h"' \
		'int probe(void) { return ((struct sched_ext_ops *)0)->cgroup_set_idle != 0; }' | \
		$(CLANG) -x c -fsyntax-only -Ibpf - >/dev/null 2>&1; then \
		features="$$features -DGOEEVDF_HAVE_CGROUP_SET_IDLE"; \
	fi; \
	$(CLANG) $(BPF_CFLAGS) $$features -c $< -o $@

internal/bpf/goeevdf.skel.h: bpf/goeevdf.bpf.o
	$(BPFTOOL) gen skeleton $< > $@

clean:
	rm -f bpf/goeevdf.bpf.o bpf/vmlinux.h internal/bpf/goeevdf.skel.h bin/goeevdf goeevdf
