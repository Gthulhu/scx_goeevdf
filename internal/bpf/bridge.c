#include "bridge.h"
#include <bpf/bpf.h>
#include <bpf/btf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <linux/bpf.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

struct ge_handle {
	struct bpf_object *obj;
	struct bpf_link *link;
	struct ring_buffer *events;
	struct user_ring_buffer *cmds;
	struct ev last;
};

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

int ge_set_tgid(struct ge_handle *h, uint32_t tgid)
{
	struct bpf_map *map = find_map(h, ".rodata");
	size_t size;
	void *data;
	int offset = var_offset(h, ".rodata", "usersched_tgid");
	if (!map || offset < 0)
		return offset < 0 ? offset : -ENOENT;
	data = bpf_map__initial_value(map, &size);
	if (!data || (size_t)offset + sizeof(tgid) > size)
		return -EINVAL;
	memcpy((char *)data + offset, &tgid, sizeof(tgid));
	return 0;
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
	struct ge_handle *h = ctx;
	if (size != sizeof(h->last))
		return -EMSGSIZE;
	memcpy(&h->last, data, size);
	/* libbpf 1.3 has no consume_n(); only a negative callback result
	 * stops the drain, after advancing the consumer past this one record. */
	return -ECANCELED;
}

int ge_load(struct ge_handle *h)
{
	int err = bpf_object__load(h->obj);
	if (err)
		return err;
	h->events = ring_buffer__new(ge_fd(h, "events"), copy_one, h, NULL);
	err = libbpf_get_error(h->events);
	if (err) {
		h->events = NULL;
		return err;
	}
	h->cmds = user_ring_buffer__new(ge_fd(h, "cmds"), NULL);
	err = libbpf_get_error(h->cmds);
	if (err) {
		h->cmds = NULL;
		return err;
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
	int ret = ring_buffer__consume(h->events);
	if (ret == -ECANCELED) {
		memcpy(out, &h->last, sizeof(*out));
		return 1;
	}
	return ret;
}
int ge_submit(struct ge_handle *h, const struct cmd *cmd)
{
	void *slot;
	errno = 0;
	slot = user_ring_buffer__reserve(h->cmds, sizeof(*cmd));
	if (!slot)
		return errno ? -errno : -ENOSPC;
	memcpy(slot, cmd, sizeof(*cmd));
	user_ring_buffer__submit(h->cmds, slot);
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
	user_ring_buffer__free(h->cmds);
	ring_buffer__free(h->events);
	bpf_object__close(h->obj);
	free(h);
}
