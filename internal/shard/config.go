package shard

import "fmt"

// ConfigKind identifies one cgroup policy update. Parents must be broadcast
// before children, and the CLI must broadcast a task's group configuration
// before routing its first enqueue.
type ConfigKind uint8

const (
	ConfigGroup ConfigKind = iota + 1
	ConfigQuota
	ConfigRemoveGroup
)

// Config is copied into every shard mailbox. Quota zero disables cpu.max;
// finite quota requires a nonzero period. Group changes do not reset quota.
type Config struct {
	Kind               ConfigKind
	ID, Parent, Weight uint64
	Quota, Period      uint64
	Idle, Root         bool
}

func (c Config) validate() error {
	if c.ID == 0 {
		return fmt.Errorf("cgroup ID must be nonzero")
	}
	switch c.Kind {
	case ConfigGroup:
		if c.ID == c.Parent || c.Weight < 1 || c.Weight > 10000 {
			return fmt.Errorf("invalid cgroup parent or weight")
		}
		if c.Root && (c.Parent != 0 || c.Idle) {
			return fmt.Errorf("root cgroup cannot have a parent or be idle")
		}
	case ConfigQuota:
		if c.Quota != 0 && c.Period == 0 {
			return fmt.Errorf("finite quota requires a period")
		}
	case ConfigRemoveGroup:
	default:
		return fmt.Errorf("unknown config kind %d", c.Kind)
	}
	return nil
}

// BroadcastConfig enqueues the same validated configuration on every shard.
// Mailbox order is shared with Route: a task event routed after this call
// cannot overtake the update on its owner. No engine callback occurs on the
// caller's goroutine. Run/Step returns an error if an owner cannot apply it.
func (c *Coordinator) BroadcastConfig(cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	for id := range c.workers {
		if _, ok := c.workers[id].engine.(interface{ Configure(Config) error }); !ok {
			return fmt.Errorf("shard %d engine cannot apply cgroup configuration", id)
		}
		if cfg.Kind == ConfigQuota && cfg.Quota != 0 && len(c.workers) > 1 {
			quota, ok := c.workers[id].engine.(interface{ SharedQuotaBound() bool })
			if !ok || !quota.SharedQuotaBound() {
				return fmt.Errorf("shard %d needs a shared cpu.max pool before finite quota", id)
			}
		}
	}
	c.mu.Lock()
	for id := range c.workers {
		c.appendLocked(id, message{kind: configMsg, config: cfg})
	}
	c.mu.Unlock()
	return nil
}
