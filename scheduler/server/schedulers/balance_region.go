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

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
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

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	suitableStores := []*core.StoreInfo{}
	for _, store := range stores {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	//排序
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})

	for _, sourceStore := range suitableStores {
		for i := 0; i < balanceRegionRetryLimit; i++ {
			sourceStoreID := sourceStore.GetID()
			var region *core.RegionInfo
			cluster.GetPendingRegionsWithLock(sourceStoreID, func(rc core.RegionsContainer) {
				region = rc.RandomRegion(nil, nil)
			})
			if region == nil {
				cluster.GetFollowersWithLock(sourceStoreID, func(rc core.RegionsContainer) {
					region = rc.RandomRegion(nil, nil)
				})
			}
			if region == nil {
				cluster.GetLeadersWithLock(sourceStoreID, func(rc core.RegionsContainer) {
					region = rc.RandomRegion(nil, nil)
				})
			}
			if region == nil {
				continue
			}
			// We don't schedule region with abnormal number of replicas.
			if len(region.GetPeers()) != cluster.GetMaxReplicas() {
				continue
			}

			oldPeer := region.GetStorePeer(sourceStoreID)
			op := s.movePeer(cluster, suitableStores, sourceStore, region, oldPeer)
			if op != nil {
				return op
			}
		}
	}
	return nil
}

func (s *balanceRegionScheduler) movePeer(cluster opt.Cluster, suitableStores []*core.StoreInfo, sourceStore *core.StoreInfo, region *core.RegionInfo, oldPeer *metapb.Peer) *operator.Operator {
	// select target store
	var targetStore *core.StoreInfo
	for i := len(suitableStores) - 1; i >= 0; i-- {
		if int64(sourceStore.GetRegionSize()-suitableStores[i].GetRegionSize()) < 2*region.GetApproximateSize() {
			break
		}

		_, ok := region.GetStoreIds()[suitableStores[i].GetID()]
		if !ok {
			targetStore = suitableStores[i]
			break
		}
	}
	if targetStore == nil {
		return nil
	}

	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}

	op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, oldPeer.GetStoreId(), newPeer.GetStoreId(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return op
}
