package memcache

import (
	"sync"
	"net"
	"log"
	"time"
	"syscall"
)

type ServerStatusType int

const (
	ServerStatusAlive ServerStatusType = 0
	ServerStatusDead ServerStatusType  = 1

	dialTimeout time.Duration = 250 * time.Millisecond
	maxServerFailure int      = 4
)

type healthCheckerRegistry struct {
	servers         []net.Addr
	cb func(net.Addr, ServerStatusType)
	serverStatus    []ServerStatusType
	serverDeadCount []int
}

// HealthChecker is responsible to check the server's status
// and call the callbacks registered by client and selector when server's status changed
type HealthChecker interface {
	Start()
	Stop()
	Register(sub interface{}, servers []net.Addr, cb func(net.Addr, ServerStatusType))
	Unregister(sub interface{})
}


type DefaultHealthChecker struct {
	lk       sync.Mutex
	started  bool
	registries map[interface{}]*healthCheckerRegistry
	stopChan chan bool
}

var hc *DefaultHealthChecker

func GetDefaultHealthChecker() *DefaultHealthChecker {
	return hc
}

func (hc *DefaultHealthChecker) Start() {
	if !hc.started {
		hc.lk.Lock()
		defer hc.lk.Unlock()
		if hc.started {
			return
		}
		go func() {
			for {
				select {
				case <-time.After(time.Second):
					hc.check()
				case <-hc.stopChan:
					log.Printf("stop health checker")
					return
				}
			}
		}()
		hc.started = true
	}
}
func (hc *DefaultHealthChecker) Stop() {
	if hc.started {
		hc.lk.Lock()
		defer hc.lk.Unlock()
		if !hc.started {
			return
		}
		hc.stopChan <- true
		hc.started = false
	}
}

func (hc *DefaultHealthChecker) Register(sub interface{}, servers []net.Addr, cb func(net.Addr, ServerStatusType)) {
	hc.lk.Lock()
	defer hc.lk.Unlock()
	hc.registries[sub] = &healthCheckerRegistry{
		servers: servers,
		cb: cb,
		serverStatus: make([]ServerStatusType, len(servers)),
		serverDeadCount: make([]int, len(servers)),
	}
}

func (hc *DefaultHealthChecker) Unregister(sub interface{}) {
	hc.lk.Lock()
	defer hc.lk.Unlock()
	delete(hc.registries, sub)
}

func (hc *DefaultHealthChecker) check() {
	hc.lk.Lock()
	defer hc.lk.Unlock()
	for _, registry := range hc.registries {
		for i, addr := range registry.servers {
			log.Printf("check %s", addr)
			markDead := false
			nc, err := net.DialTimeout(addr.Network(), addr.String(), dialTimeout)
			if nc != nil {
				nc.Close()
			}

			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					log.Print(err)
					markDead = true
				} else {
					switch t := err.(type) {
					case *net.OpError:
						if t.Op == "dial" || t.Op == "read" {
							markDead = true
							log.Printf(t.Op)
						}
					case syscall.Errno:
						if t == syscall.ECONNREFUSED {
							markDead = true
							log.Print(t)
						}
					}
				}
			}
			if markDead {
				registry.serverDeadCount[i] += 1
			}
			if originStatus := registry.serverStatus[i]; markDead && originStatus == ServerStatusAlive && registry.serverDeadCount[i] > maxServerFailure {
				registry.serverStatus[i] = ServerStatusDead
				registry.cb(addr, registry.serverStatus[i])
			} else if !markDead && originStatus == ServerStatusDead {
				registry.serverStatus[i] = ServerStatusAlive
				registry.serverDeadCount[i] = 0
				registry.cb(addr, registry.serverStatus[i])
			}
		}
	}
}

func init() {
	hc = &DefaultHealthChecker{
		registries: make(map[interface{}]*healthCheckerRegistry),
		stopChan: make(chan bool),
	}
}
