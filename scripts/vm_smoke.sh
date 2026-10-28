#!/bin/sh
set -eu

if ! grep -q 'goeevdf_test_vm=1' /proc/cmdline; then
	echo "Refusing to attach scheduler outside the marked disposable test VM" >&2
	exit 2
fi

ulimit -l unlimited
(cd internal/bpf && GOEEVDF_TEST_BPF_LOAD=1 ../../bin/bpf-load.test -test.v)
log=$(mktemp /tmp/goeevdf-smoke.XXXXXX)
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
./bin/goeevdf -shards 1 "$@" >"$log" 2>&1 &
scheduler=$!
attempts=0
while [ "$(cat /sys/kernel/sched_ext/state)" != enabled ]; do
	if ! kill -0 "$scheduler" 2>/dev/null; then
		echo "Scheduler exited before attachment" >&2
		exit 1
	fi
	attempts=$((attempts + 1))
	if [ "$attempts" -ge 100 ]; then
		echo "Timed out waiting for scheduler attachment" >&2
		exit 1
	fi
	sleep 0.1
done
kill -0 "$scheduler"
python3 scripts/fairness.py
kill -TERM "$scheduler"
wait "$scheduler"
scheduler=
test "$(cat /sys/kernel/sched_ext/state)" = disabled
echo "VM attach/fairness/detach smoke passed"
