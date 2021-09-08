// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
	filters		 []filter.Filter
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.filters = []filter.Filter{
		filter.NewHealthFilter(s.GetName()),
		filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// check whether the region movement is valuable
func checkRegionMoveValuable(source *core.StoreInfo, target *core.StoreInfo, approximateSize int64) bool {
	return source.GetRegionSize() - target.GetRegionSize() >= 2 * approximateSize
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	sources := filter.SelectSourceStores(stores, s.filters, cluster)
	targets := filter.SelectTargetStores(stores, s.filters, cluster)
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})
	var selectedRegion *core.RegionInfo = nil
	var source *core.StoreInfo = nil
	//Notice: why we need check MaxReplicas
	for _, source = range sources {
		cb := func(rc core.RegionsContainer) {
			selectedRegion = rc.RandomRegion([]byte(""), []byte(""))
		}
		cluster.GetPendingRegionsWithLock(source.GetID(), cb)
		if selectedRegion == nil || len(selectedRegion.GetPeers()) < cluster.GetMaxReplicas() {
			cluster.GetFollowersWithLock(source.GetID(), cb)
		}
		if selectedRegion == nil || len(selectedRegion.GetPeers()) < cluster.GetMaxReplicas() {
			cluster.GetLeadersWithLock(source.GetID(), cb)
		}
		if selectedRegion != nil && len(selectedRegion.GetPeers()) >= cluster.GetMaxReplicas() {
			break
		}
	}

	log.Infof("select region=%+v source=%+v", selectedRegion, source)
	if selectedRegion == nil {
		log.Infof("select region empty")
		return nil
	}
	selectedRegion.GetApproximateSize()
	var target *core.StoreInfo = nil
	for _, t := range targets {
		if selectedRegion.GetStorePeer(t.GetID()) != nil {
			continue
		}
		if checkRegionMoveValuable(source, t, selectedRegion.GetApproximateSize()) {
			target = t
			break
		}
	}
	log.Infof("select target=%+v", target)
	if target == nil {
		log.Infof("select target empty")
		return nil
	}
	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		log.Warn("allocPeer error", err)
		return nil
	}
	op, err := operator.CreateMovePeerOperator(s.GetName(), cluster, selectedRegion,
	 operator.OpBalance, source.GetID(), target.GetID(), newPeer.GetId())
	if err != nil {
		log.Warn("alloc operator error", err)
	}
	return op
}
