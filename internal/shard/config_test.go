package shard

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

type configFakeEngine struct {
	*fakeEngine
	trace []string
}

func (e *configFakeEngine) Configure(c Config) error {
	e.trace = append(e.trace, fmt.Sprintf("config:%d:%d", c.Kind, c.ID))
	return nil
}
func (e *configFakeEngine) Handle(slot int32, ev eevdf.Event) error {
	e.trace = append(e.trace, fmt.Sprintf("event:%d", ev.Pid))
	return e.fakeEngine.Handle(slot, ev)
}

func TestConfigBroadcastPrecedesRoutedEventsOnEveryOwner(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	engines := make([]Engine, 2)
	fakes := make([]*configFakeEngine, 2)
	for id := range fakes {
		fakes[id] = &configFakeEngine{fakeEngine: &fakeEngine{id: id, tasks: make(map[int32]int32)}}
		engines[id] = fakes[id]
	}
	c, _ := New(topo, engines)
	parent := Config{Kind: ConfigGroup, ID: 7, Weight: 100}
	child := Config{Kind: ConfigGroup, ID: 8, Parent: 7, Weight: 200}
	if err := c.BroadcastConfig(parent); err != nil {
		t.Fatal(err)
	}
	if err := c.BroadcastConfig(child); err != nil {
		t.Fatal(err)
	}
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 11, Cpu: 40}); err != nil {
		t.Fatal(err)
	}
	for _, e := range fakes {
		if len(e.trace) != 0 {
			t.Fatal("broadcast called Policy on nonowner goroutine")
		}
	}
	stepAll(t, c)
	if !reflect.DeepEqual(fakes[0].trace, []string{"config:1:7", "config:1:8"}) ||
		!reflect.DeepEqual(fakes[1].trace, []string{"config:1:7", "config:1:8", "event:11"}) {
		t.Fatalf("config/task order broken: %v / %v", fakes[0].trace, fakes[1].trace)
	}
	if err := c.BroadcastConfig(Config{Kind: ConfigGroup, ID: 8, Parent: 7, Weight: 0}); err == nil {
		t.Fatal("invalid broadcast accepted")
	}
	if err := c.BroadcastConfig(Config{Kind: ConfigRemoveGroup, ID: 8}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	for _, e := range fakes {
		if last := e.trace[len(e.trace)-1]; last != "config:3:8" {
			t.Fatalf("group removal was not broadcast: %v", e.trace)
		}
	}
}

func TestRealPolicyGroupConfigBroadcast(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	engines := make([]Engine, 2)
	for id := range engines {
		io := &shardPolicyIO{queued: make(map[int32]uint64), allowed: cpusFrom(0, 80), reject: true}
		engine, err := NewPolicyEngine(io, topo.CPUs[id], eevdf.Enums{})
		if err != nil {
			t.Fatal(err)
		}

		engines[id] = engine
	}
	c, _ := New(topo, engines)
	if err := c.BroadcastConfig(Config{Kind: ConfigGroup, ID: 7, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 71, Cpu: 0, Cgid: 7, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if err := c.Handoff(71, 0, 40); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if id, ok := c.Owner(71, 0); !ok || id != 1 {
		t.Fatal("destination rejected group absent from its Policy")
	}
	if err := c.BroadcastConfig(Config{Kind: ConfigRemoveGroup, ID: 7}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if err := c.BroadcastConfig(Config{Kind: ConfigQuota, ID: 7, Quota: 100_000, Period: 1_000_000}); err == nil {
		t.Fatal("multi-shard quota accepted without shared accounting")
	}
}

func TestRootAndIdleGroupConfiguration(t *testing.T) {
	io := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0}, reject: true}
	engine, err := NewPolicyEngine(io, []int32{0}, eevdf.Enums{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []Config{
		{Kind: ConfigGroup, ID: 1, Weight: 100, Root: true},
		{Kind: ConfigGroup, ID: 2, Parent: 1, Weight: 200, Idle: true},
	} {
		if err := cfg.validate(); err != nil {
			t.Fatal(err)
		}
		if err := engine.Configure(cfg); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.Shard.Policy.SetCgroupIdle(1, true); err == nil {
		t.Fatal("broadcast root was registered as an ordinary weighted group")
	}
	if err := (Config{Kind: ConfigGroup, ID: 1, Weight: 100, Root: true, Idle: true}).validate(); err == nil {
		t.Fatal("idle root configuration accepted")
	}
}

func TestQuotaConfigBroadcastRequiresAndUsesSharedPool(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	pool := eevdf.NewSharedQuotaPool()
	engines := make([]Engine, 2)
	for id := range engines {
		io := &shardPolicyIO{queued: make(map[int32]uint64), allowed: cpusFrom(0, 80), reject: true}
		engine, err := NewPolicyEngineWithQuota(io, topo.CPUs[id], eevdf.Enums{}, pool)
		if err != nil {
			t.Fatal(err)
		}
		engines[id] = engine
	}
	c, _ := New(topo, engines)
	if err := c.BroadcastConfig(Config{Kind: ConfigGroup, ID: 7, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	if err := pool.Configure(7, 100_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := c.BroadcastConfig(Config{Kind: ConfigQuota, ID: 7, Quota: 100_000, Period: 1_000_000}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if err := pool.Configure(7, 200_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := c.BroadcastConfig(Config{Kind: ConfigQuota, ID: 7, Quota: 200_000, Period: 1_000_000}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
}

func TestSharedQuotaParkedHandoffAndRefill(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	pool := eevdf.NewSharedQuotaPool()
	engines := make([]Engine, 2)
	ios := make([]*shardPolicyIO, 2)
	for id := range engines {
		ios[id] = &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, idle: true}
		engine, err := NewPolicyEngineWithQuota(ios[id], topo.CPUs[id], eevdf.Enums{}, pool)
		if err != nil {
			t.Fatal(err)
		}
		engines[id] = engine
	}
	c, _ := New(topo, engines)
	if err := pool.Configure(7, 10_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []Config{
		{Kind: ConfigGroup, ID: 7, Weight: 100},
		{Kind: ConfigQuota, ID: 7, Quota: 10_000, Period: 1_000_000},
	} {
		if err := c.BroadcastConfig(cfg); err != nil {
			t.Fatal(err)
		}
	}
	stepAll(t, c)
	pool.Refill(10_000_000)
	for _, ev := range []eevdf.Event{
		{Kind: eevdf.EvEnqueue, Pid: 11, Cpu: 0, Cgid: 7, Weight: 100},
		{Kind: eevdf.EvEnqueue, Pid: 12, Cpu: 0, Cgid: 7, Weight: 100},
	} {
		if err := c.Route(0, ev); err != nil {
			t.Fatal(err)
		}
	}
	stepAll(t, c)
	if len(ios[0].commands) != 1 || ios[0].commands[0].Slice != 10_000 {
		t.Fatalf("source over-allocated quota: %+v", ios[0].commands)
	}
	if err := c.Handoff(12, 0, 40); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if owner, ok := c.Owner(12, 0); !ok || owner != 1 {
		t.Fatalf("parked task not adopted: %d %v", owner, ok)
	}
	if len(ios[1].commands) != 0 {
		t.Fatalf("destination dispatched without shared budget: %+v", ios[1].commands)
	}
	pool.Refill(11_000_000)
	if err := c.Periodic(1, 11_000_000); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if len(ios[1].commands) != 1 || ios[1].commands[0].Pid != 12 ||
		ios[1].commands[0].Slice != 10_000 {
		t.Fatalf("destination did not dispatch after refill: %+v", ios[1].commands)
	}
}

func TestSharedQuotaRemoteRunningEnqueueParksAndTransfers(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	pool := eevdf.NewSharedQuotaPool()
	engines := make([]Engine, 2)
	ios := make([]*shardPolicyIO, 2)
	for id := range engines {
		ios[id] = &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, idle: true}
		engine, err := NewPolicyEngineWithQuota(ios[id], topo.CPUs[id], eevdf.Enums{}, pool)
		if err != nil {
			t.Fatal(err)
		}
		engines[id] = engine
	}
	c, _ := New(topo, engines)
	if err := pool.Configure(7, 10_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []Config{
		{Kind: ConfigGroup, ID: 7, Weight: 100},
		{Kind: ConfigQuota, ID: 7, Quota: 10_000, Period: 1_000_000},
	} {
		if err := c.BroadcastConfig(cfg); err != nil {
			t.Fatal(err)
		}
	}
	stepAll(t, c)
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 0, Cgid: 7, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if len(ios[0].commands) != 1 {
		t.Fatalf("task was not dispatched: %+v", ios[0].commands)
	}
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23,
		Cpu: 40, Cgid: 7, Weight: 100, SumExec: 10_000, EnqSeq: 2}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if owner, ok := c.Owner(23, 0); !ok || owner != 1 {
		t.Fatalf("remote enqueue was not transferred: %d %v", owner, ok)
	}
	if len(ios[0].commands) != 1 || len(ios[1].commands) != 0 {
		t.Fatalf("exhausted task dispatched before refill: %v / %v", ios[0].commands, ios[1].commands)
	}
	pool.Refill(10_000_000)
	pool.Refill(11_000_000)
	if err := c.Periodic(1, 11_000_000); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if len(ios[1].commands) != 1 || ios[1].commands[0].Pid != 23 {
		t.Fatalf("transferred task did not resume: %+v", ios[1].commands)
	}
}
