/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memcache

import (
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"strings"
	"sync"
)

const (
	serverSelectorDefaultRetryLimit int = 3
)

// ServerSelector is the interface that selects a memcache server
// as a function of the item's key.
//
// All ServerSelector implementations must be threadsafe.
type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(key string) (net.Addr, error)
	Each(func(net.Addr) error) error
}

// ServerList is a simple ServerSelector. Its zero value is usable.
type ServerList struct {
	lk    sync.RWMutex
	addrs []net.Addr
}

// SetServers changes a ServerList's set of servers at runtime and is
// threadsafe.
//
// Each server is given equal weight. A server is given more weight
// if it's listed multiple times.
//
// SetServers returns an error if any of the server names fail to
// resolve. No attempt is made to connect to the server. If any error
// is returned, no changes are made to the ServerList.
func (ss *ServerList) SetServers(servers ...string) error {
	naddr := make([]net.Addr, len(servers))
	for i, server := range servers {
		log.Println(server)
		if strings.Contains(server, "/") {
			addr, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				return err
			}
			naddr[i] = addr
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				return err
			}
			naddr[i] = tcpaddr
		}
	}

	ss.lk.Lock()
	defer ss.lk.Unlock()
	ss.addrs = naddr
	return nil
}

// Each iterates over each server calling the given function
func (ss *ServerList) Each(f func(net.Addr) error) error {
	ss.lk.RLock()
	defer ss.lk.RUnlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

func (ss *ServerList) PickServer(key string) (net.Addr, error) {
	ss.lk.RLock()
	defer ss.lk.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}
	// TODO-GO: remove this copy
	cs := crc32.ChecksumIEEE([]byte(key))
	return ss.addrs[cs%uint32(len(ss.addrs))], nil
}

// A dynamic server selector behaves like a general server selector except that
// it will kick the dead server and re-add it when the server become alive again
type DynamicServerSelector interface {
ServerSelector
	OnServerStatusChanged(net.Addr, ServerStatusType)
}

type DefaultDynamicServerSelector struct {
	ServerList
	statusList []ServerStatusType
}

func (ss *DefaultDynamicServerSelector) SetServers(servers ...string) error {
	ss.ServerList.SetServers(servers...)
	ss.lk.Lock()
	defer ss.lk.Unlock()
	ss.statusList = make([]ServerStatusType, len(servers))
	return nil
}

func (ss *DefaultDynamicServerSelector) hash(key string) uint32 {
	sum := crc32.ChecksumIEEE([]byte(key))
	sum = ((sum&0xffffffff)>>16)&0x7fff
	if sum == 0 { return 1 }; return sum
}
func (ss *DefaultDynamicServerSelector) PickServer(key string) (net.Addr, error) {
	ss.lk.RLock()
	defer ss.lk.RUnlock()
	if len(ss.addrs) == 0 {
		return nil, ErrNoServers
	}

	for i := 0; i < serverSelectorDefaultRetryLimit; i++ {
		sum := ss.hash(key)
		if idx := sum % uint32(len(ss.addrs)); ss.statusList[idx] != ServerStatusAlive {
			key = fmt.Sprintf("%v%v", sum, i)
			continue
		} else {
			return ss.addrs[idx], nil
		}
	}
	return nil, ErrRetryLimitReached
}

func (ss *DefaultDynamicServerSelector) OnServerStatusChanged(addr net.Addr, status ServerStatusType) {
	log.Printf("server: %v status changed %v", addr, status)
	ss.lk.RLock()
	defer ss.lk.RLock()
	for idx, addr_ := range ss.addrs {
		if addr_ == addr {
			ss.statusList[idx] = status
			break
		}
	}
}
