#!/bin/sh
set -eu

if ! grep -q 'goeevdf_test_vm=1' /proc/cmdline; then
	echo "Refusing to attach scheduler outside the marked disposable test VM" >&2
	exit 2
fi
if [ "$#" -lt 1 ]; then
	echo "Usage: sh scripts/vm_adaptive.sh /path/to/schtest [goeevdf flags...]" >&2
	exit 2
fi
benchmark=$1
shift
test -x "$benchmark"
test -x ./bin/goeevdf
test "$(cat /sys/kernel/sched_ext/state)" = disabled
ulimit -l unlimited
echo "Kernel: $(uname -r); CPUs: $(getconf _NPROCESSORS_ONLN)"

log=$(mktemp /tmp/goeevdf-adaptive.XXXXXX)
scheduler=
cleanup() {
	if [ -n "$scheduler" ]; then
		if kill -0 "$scheduler" 2>/dev/null; then
			kill -TERM "$scheduler"
		fi
		wait "$scheduler" || true
	fi
	cat "$log"
	rm -f "$log"
}
trap cleanup EXIT

status=0
echo "Built-in EEVDF adaptive_priority baseline"
if timeout 90s "$benchmark" --filter adaptive_priority; then
	echo "Baseline passed"
else
	echo "Baseline failed" >&2
	status=1
fi

for attempt in 1 2 3; do
	echo "scx_goeevdf adaptive_priority attempt $attempt"
	./bin/goeevdf -shards 1 "$@" >"$log" 2>&1 &
	scheduler=$!
	readiness=0
	while [ "$(cat /sys/kernel/sched_ext/state)" != enabled ]; do
		if ! kill -0 "$scheduler" 2>/dev/null; then
			echo "Scheduler exited before attachment" >&2
			exit 1
		fi
		readiness=$((readiness + 1))
		if [ "$readiness" -ge 100 ]; then
			echo "Timed out waiting for scheduler attachment" >&2
			exit 1
		fi
		sleep 0.1
	done
	if timeout 90s "$benchmark" --filter adaptive_priority; then
		echo "Attempt $attempt passed"
	else
		echo "Attempt $attempt failed" >&2
		status=1
	fi
	kill -TERM "$scheduler"
	wait "$scheduler"
	scheduler=
	test "$(cat /sys/kernel/sched_ext/state)" = disabled
	cat "$log"
	: >"$log"
done
exit "$status"
