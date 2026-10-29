#include "bridge.h"
#include <bpf/bpf.h>
#include <bpf/btf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <linux/bpf.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/syscall.h>
#include <unistd.h>

struct ge_handle {
	struct bpf_object *obj;
	struct bpf_link *link;
	struct ring_buffer *events[MAX_SHARDS];
	struct user_ring_buffer *cmds[MAX_SHARDS];
	struct ev last[MAX_SHARDS];
	uint32_t cpu_shards[MAX_CPUS], nr_shards;
};

int ge_enum_value(const char *name, uint64_t *value)
{
	struct btf *btf = btf__load_vmlinux_btf();
	int err = -ENOENT;

	if (libbpf_get_error(btf))
		return libbpf_get_error(btf);
	for (uint32_t id = 1; id < btf__type_cnt(btf); id++) {
		const struct btf_type *t = btf__type_by_id(btf, id);

		if (!btf_is_enum(t) && !btf_is_enum64(t))
			continue;
		for (int i = 0; i < btf_vlen(t); i++) {
			uint32_t off = btf_is_enum(t) ? btf_enum(t)[i].name_off :
				btf_enum64(t)[i].name_off;
			if (strcmp(btf__name_by_offset(btf, off), name))
				continue;
			if (btf_is_enum(t))
				*value = btf_kflag(t) ? (uint64_t)(int64_t)btf_enum(t)[i].val :
					(uint32_t)btf_enum(t)[i].val;
			else
				*value = btf_enum64_value(&btf_enum64(t)[i]);
			err = 0;
			goto out;
		}
	}
out:
	btf__free(btf);
	return err;
}

static struct bpf_map *find_map(struct ge_handle *h, const char *name)
{
	struct bpf_map *map;
	bpf_object__for_each_map(map, h->obj) {
		const char *n = bpf_map__name(map);
		if (!strcmp(n, name) || (name[0] == '.' && strstr(n, name)))
			return map;
	}
	return NULL;
}

static int var_offset(struct ge_handle *h, const char *section, const char *name)
{
	const struct btf *btf = bpf_object__btf(h->obj);
	const struct btf_type *sec;
	const struct btf_var_secinfo *vars;
	int id, i;
	if (!btf)
		return -ENOENT;
	id = btf__find_by_name_kind(btf, section, BTF_KIND_DATASEC);
	if (id < 0)
		return id;
	sec = btf__type_by_id(btf, id);
	vars = btf_var_secinfos(sec);
	for (i = 0; i < btf_vlen(sec); i++) {
		const struct btf_type *v = btf__type_by_id(btf, vars[i].type);
		if (v && !strcmp(btf__name_by_offset(btf, v->name_off), name))
			return vars[i].offset;
	}
	return -ENOENT;
}

static int member_offset(const struct btf *btf, int type_id, const char *name)
{
	const struct btf_type *type = btf__type_by_id(btf, type_id);
	const struct btf_member *members;
	int i;

	if (!type || !btf_is_struct(type))
		return -ENOENT;
	members = btf_members(type);
	for (i = 0; i < btf_vlen(type); i++)
		if (!strcmp(btf__name_by_offset(btf, members[i].name_off), name))
			return btf_member_bit_offset(type, i) / 8;
	return -ENOENT;
}

static int clear_ops_field(struct ge_handle *h, const char *field)
{
	struct bpf_map *map = find_map(h, "ge_ops");
	const struct btf *btf = bpf_object__btf(h->obj);
	size_t size;
	void *data;
	int type_id, offset;

	if (!map || !btf)
		return -ENOENT;
	type_id = btf__find_by_name_kind(btf, "sched_ext_ops", BTF_KIND_STRUCT);
	if (type_id < 0)
		return type_id;
	offset = member_offset(btf, type_id, field);
	if (offset < 0)
		return offset;
	data = bpf_map__initial_value(map, &size);
	if (!data || (size_t)offset + sizeof(void *) > size)
		return -EINVAL;
	memset((char *)data + offset, 0, sizeof(void *));
	return 0;
}

static int disable_missing_ops(struct ge_handle *h)
{
	static const struct {
		const char *field, *program;
	} optional[] = {
		{ "tick", "ge_tick" },
		{ "core_sched_before", "ge_core_sched_before" },
		{ "dump_task", "ge_dump_task" },
		{ "cgroup_set_bandwidth", "ge_cgroup_set_bandwidth" },
		{ "cgroup_set_idle", "ge_cgroup_set_idle" },
	};
	struct btf *kernel = btf__load_vmlinux_btf();
	int kernel_id, i, err = 0;

	if (libbpf_get_error(kernel))
		return 0;
	kernel_id = btf__find_by_name_kind(kernel, "sched_ext_ops", BTF_KIND_STRUCT);
	if (kernel_id < 0)
		goto out;
	for (i = 0; i < sizeof(optional) / sizeof(optional[0]); i++) {
		struct bpf_program *prog;

		if (member_offset(kernel, kernel_id, optional[i].field) >= 0)
			continue;
		prog = bpf_object__find_program_by_name(h->obj, optional[i].program);
		if (!prog)
			continue;
		err = bpf_program__set_autoload(prog, false);
		if (err)
			break;
		err = clear_ops_field(h, optional[i].field);
		if (err)
			break;
	}
out:
	btf__free(kernel);
	return err;
}

int ge_disable_op(struct ge_handle *h, const char *name)
{
	struct bpf_program *prog = bpf_object__find_program_by_name(h->obj, name);

	return prog ? bpf_program__set_autoload(prog, false) : -ENOENT;
}

struct ge_handle *ge_open(const char *path)
{
	struct ge_handle *h = calloc(1, sizeof(*h));
	int err;
	if (!h)
		return NULL;
	h->obj = bpf_object__open_file(path, NULL);
	err = libbpf_get_error(h->obj);
	if (!err)
		return h;
	errno = -err;
	free(h);
	return NULL;
}

static int set_rodata(struct ge_handle *h, const char *name, const void *value, size_t len)
{
	struct bpf_map *map = find_map(h, ".rodata");
	size_t size;
	void *data;
	int offset = var_offset(h, ".rodata", name);
	if (!map || offset < 0)
		return offset < 0 ? offset : -ENOENT;
	data = bpf_map__initial_value(map, &size);
	if (!data || (size_t)offset + len > size)
		return -EINVAL;
	memcpy((char *)data + offset, value, len);
	return 0;
}

int ge_set_tgid(struct ge_handle *h, uint32_t tgid)
{
	return set_rodata(h, "usersched_tgid", &tgid, sizeof(tgid));
}

int ge_set_usersched_cpus(struct ge_handle *h, const uint32_t *cpus, uint32_t nr)
{
	uint32_t mapping[MAX_SHARDS] = {};
	if (!cpus || !nr || nr > MAX_SHARDS)
		return -EINVAL;
	for (uint32_t i = 0; i < nr; i++) {
		if (cpus[i] >= MAX_CPUS)
			return -EINVAL;
		mapping[i] = cpus[i];
	}
	return set_rodata(h, "usersched_cpus", mapping, sizeof(mapping));
}

int ge_set_shards(struct ge_handle *h, const uint32_t *cpus, uint32_t len, uint32_t nr)
{
	uint32_t mapping[MAX_CPUS] = {};
	int err;
	if (!nr || nr > MAX_SHARDS || len > MAX_CPUS || (len && !cpus))
		return -EINVAL;
	for (uint32_t i = 0; i < len; i++) {
		if (cpus[i] >= nr)
			return -EINVAL;
		mapping[i] = cpus[i];
	}
	err = set_rodata(h, "cpu_shard", mapping, sizeof(mapping));
	if (err)
		return err;
	err = set_rodata(h, "nr_shards", &nr, sizeof(nr));
	if (!err) {
		memcpy(h->cpu_shards, mapping, sizeof(mapping));
		h->nr_shards = nr;
	}
	return err;
}

int ge_register_usersched(struct ge_handle *h, uint32_t tid, uint32_t shard)
{
	if (shard >= (h->nr_shards ? h->nr_shards : 1))
		return -EINVAL;
	return bpf_map_update_elem(ge_fd(h, "usersched_tids"), &tid, &shard, BPF_ANY) ?
		-errno : 0;
}

int ge_fd(struct ge_handle *h, const char *name)
{
	struct bpf_map *map = find_map(h, name);
	return map ? bpf_map__fd(map) : -ENOENT;
}
int ge_counter_offset(struct ge_handle *h, const char *name)
{
	return var_offset(h, ".bss", name);
}
int ge_bss_size(struct ge_handle *h)
{
	struct bpf_map *m = find_map(h, ".bss");
	return m ? bpf_map__value_size(m) : -ENOENT;
}
static int copy_one(void *ctx, void *data, size_t size)
{
	struct ev *last = ctx;
	if (size != sizeof(*last))
		return -EMSGSIZE;
	memcpy(last, data, size);
	/* libbpf 1.3 has no consume_n(); only a negative callback result
	 * stops the drain, after advancing the consumer past this one record. */
	return -ECANCELED;
}

int ge_load(struct ge_handle *h)
{
	int err = disable_missing_ops(h);
	uint32_t nr = h->nr_shards ? h->nr_shards : 1;
	int outer;
	if (err)
		return err;
	/* Inactive static USER_RINGBUF maps still exist; keep their footprint
	 * at one page instead of allocating 4 MiB each. */
	for (uint32_t i = nr; i < MAX_SHARDS; i++) {
		char name[16];
		struct bpf_map *m;
		snprintf(name, sizeof(name), "cmds_%u", i);
		m = find_map(h, name);
		if (!m)
			return -ENOENT;
		err = bpf_map__set_max_entries(m, 4096);
		if (err)
			return err;
	}
	err = bpf_object__load(h->obj);
	if (err)
		return err;
	outer = ge_fd(h, "ev_rings");
	if (outer < 0)
		return outer;
	for (uint32_t i = 0; i < nr; i++) {
		char name[16];
		int fd;
		if (i) {
			fd = bpf_map_create(BPF_MAP_TYPE_RINGBUF, NULL, 0, 0, 4 << 20, NULL);
			if (fd < 0)
				return -errno;
			err = bpf_map_update_elem(outer, &i, &fd, BPF_ANY);
			if (err) {
				err = -errno;
				close(fd);
				return err;
			}
		} else {
			fd = ge_fd(h, "events");
		}
		h->events[i] = ring_buffer__new(fd, copy_one, &h->last[i], NULL);
		err = libbpf_get_error(h->events[i]);
		if (i)
			close(fd);
		if (err) {
			h->events[i] = NULL;
			return err;
		}
		snprintf(name, sizeof(name), "cmds_%u", i);
		h->cmds[i] = user_ring_buffer__new(ge_fd(h, name), NULL);
		err = libbpf_get_error(h->cmds[i]);
		if (err) {
			h->cmds[i] = NULL;
			return err;
		}
	}
	return 0;
}
int ge_attach(struct ge_handle *h)
{
	struct bpf_map *m = find_map(h, "ge_ops");
	int err;
	if (!m)
		return -ENOENT;
	h->link = bpf_map__attach_struct_ops(m);
	err = libbpf_get_error(h->link);
	if (err) {
		h->link = NULL;
		return err;
	}
	return 0;
}
int ge_consume(struct ge_handle *h, struct ev *out)
{
	return ge_consume_shard(h, 0, out);
}
int ge_consume_shard(struct ge_handle *h, uint32_t shard, struct ev *out)
{
	if (shard >= (h->nr_shards ? h->nr_shards : 1) || !h->events[shard])
		return -EINVAL;
	int ret = ring_buffer__consume(h->events[shard]);
	if (ret == -ECANCELED) {
		memcpy(out, &h->last[shard], sizeof(*out));
		return 1;
	}
	return ret;
}

int ge_wait_shard(struct ge_handle *h, uint32_t shard)
{
	struct epoll_event event;
	int ret;

	if (shard >= (h->nr_shards ? h->nr_shards : 1) || !h->events[shard])
		return -EINVAL;
	ret = epoll_wait(ring_buffer__epoll_fd(h->events[shard]), &event, 1, 1);
	return ret < 0 ? -errno : 0;
}

int ge_submit(struct ge_handle *h, const struct cmd *cmd)
{
	if (cmd->cpu < 0 || cmd->cpu >= MAX_CPUS)
		return -EINVAL;
	return ge_submit_shard(h, h->cpu_shards[cmd->cpu], cmd);
}
int ge_submit_shard(struct ge_handle *h, uint32_t shard, const struct cmd *cmd)
{
	void *slot;
	if (shard >= (h->nr_shards ? h->nr_shards : 1) || !h->cmds[shard] ||
	    cmd->cpu < 0 || cmd->cpu >= MAX_CPUS || h->cpu_shards[cmd->cpu] != shard)
		return -EINVAL;
	errno = 0;
	slot = user_ring_buffer__reserve(h->cmds[shard], sizeof(*cmd));
	if (!slot)
		return errno ? -errno : -ENOSPC;
	memcpy(slot, cmd, sizeof(*cmd));
	user_ring_buffer__submit(h->cmds[shard], slot);
	return 0;
}
int ge_kick(struct ge_handle *h, int32_t cpu, uint64_t flags)
{
	struct bpf_program *prog = bpf_object__find_program_by_name(h->obj, "ge_kick_prog");
	struct { int32_t cpu; uint32_t pad; uint64_t flags; } arg = {cpu, 0, flags};
	struct bpf_test_run_opts opts = {
		.sz = sizeof(opts),
		.ctx_in = &arg,
		.ctx_size_in = sizeof(arg),
	};
	if (!prog)
		return -ENOENT;
	return bpf_prog_test_run_opts(bpf_program__fd(prog), &opts);
}
int ge_hrtick(struct ge_handle *h, int32_t cpu, uint32_t run_seq, uint64_t when)
{
	struct bpf_program *prog = bpf_object__find_program_by_name(h->obj, "ge_hrtick_prog");
	struct { int32_t cpu; uint32_t run_seq; uint64_t when; } arg = {cpu, run_seq, when};
	struct bpf_test_run_opts opts = {
		.sz = sizeof(opts),
		.ctx_in = &arg,
		.ctx_size_in = sizeof(arg),
	};
	if (!prog)
		return -ENOENT;
	return bpf_prog_test_run_opts(bpf_program__fd(prog), &opts);
}
int ge_test_emit(struct ge_handle *h, int32_t cpu, uint64_t sequence)
{
	struct bpf_program *prog = bpf_object__find_program_by_name(h->obj, "ge_test_emit_prog");
	struct { int32_t cpu; uint32_t pad; uint64_t sequence; } arg = {cpu, 0, sequence};
	struct bpf_test_run_opts opts = {
		.sz = sizeof(opts),
		.ctx_in = &arg,
		.ctx_size_in = sizeof(arg),
	};
	if (!prog)
		return -ENOENT;
	return bpf_prog_test_run_opts(bpf_program__fd(prog), &opts);
}
void ge_close(struct ge_handle *h)
{
	if (!h)
		return;
	bpf_link__destroy(h->link);
	for (uint32_t i = 0; i < MAX_SHARDS; i++) {
		user_ring_buffer__free(h->cmds[i]);
		ring_buffer__free(h->events[i]);
	}
	bpf_object__close(h->obj);
	free(h);
}
