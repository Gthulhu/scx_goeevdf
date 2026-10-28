#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
	echo "Usage: sudo sh scripts/test_vm.sh /path/to/kernel-image" >&2
	echo "First run: make build && go test -c -o bin/bpf-load.test ./internal/bpf" >&2
	exit 2
fi
test -r "$1"
test -x bin/goeevdf
test -x bin/bpf-load.test
# Keep the host filesystem read-only. Workload state and logs live in guest /tmp.
exec timeout 120s vng --run "$1" --user root --cpus 8 --memory 4G \
	--append goeevdf_test_vm=1 --exec 'sh scripts/vm_smoke.sh'
