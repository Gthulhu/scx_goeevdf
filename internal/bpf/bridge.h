#ifndef GOEEVDF_BRIDGE_H
#define GOEEVDF_BRIDGE_H
#include <stdint.h>
typedef uint32_t u32;
typedef int32_t s32;
typedef uint64_t u64;
typedef int64_t s64;
#include "../../bpf/intf.h"

struct ge_handle;
struct ge_handle *ge_open(const char *path);
int ge_set_tgid(struct ge_handle *, uint32_t);
int ge_set_shards(struct ge_handle *, const uint32_t *, uint32_t, uint32_t);
int ge_register_usersched(struct ge_handle *, uint32_t, uint32_t);
int ge_disable_op(struct ge_handle *, const char *);
int ge_load(struct ge_handle *);
int ge_attach(struct ge_handle *);
int ge_fd(struct ge_handle *, const char *);
int ge_counter_offset(struct ge_handle *, const char *);
int ge_bss_size(struct ge_handle *);
int ge_consume(struct ge_handle *, struct ev *);
int ge_submit(struct ge_handle *, const struct cmd *);
int ge_consume_shard(struct ge_handle *, uint32_t, struct ev *);
int ge_submit_shard(struct ge_handle *, uint32_t, const struct cmd *);
int ge_kick(struct ge_handle *, int32_t, uint64_t);
int ge_hrtick(struct ge_handle *, int32_t, uint32_t, uint64_t);
int ge_test_emit(struct ge_handle *, int32_t, uint64_t);
void ge_close(struct ge_handle *);
#endif
