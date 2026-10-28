CLANG ?= clang
BPFTOOL ?= $(firstword $(wildcard /usr/lib/linux-tools/*/bpftool))
BPFTOOL := $(if $(BPFTOOL),$(BPFTOOL),bpftool)
BPF_ARCH ?= x86

.PHONY: all bpf go build skeleton clean
all: go

bpf: bpf/goeevdf.bpf.o

skeleton: internal/bpf/goeevdf.skel.h

go: bpf
	mkdir -p bin
	go build -o bin/goeevdf ./cmd/goeevdf

build: go

bpf/vmlinux.h:
	$(BPFTOOL) btf dump file /sys/kernel/btf/vmlinux format c > $@

bpf/goeevdf.bpf.o: bpf/goeevdf.bpf.c bpf/intf.h bpf/vmlinux.h
	$(CLANG) -O2 -g -target bpf -D__TARGET_ARCH_$(BPF_ARCH) -Ibpf -c $< -o $@

internal/bpf/goeevdf.skel.h: bpf/goeevdf.bpf.o
	$(BPFTOOL) gen skeleton $< > $@

clean:
	rm -f bpf/goeevdf.bpf.o bpf/vmlinux.h internal/bpf/goeevdf.skel.h bin/goeevdf goeevdf
