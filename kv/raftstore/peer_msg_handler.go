package raftstore

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	ready := d.RaftGroup.Ready()
	result, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		panic(err)
	}
	if result != nil {
		d.insertRegionToMeta(result.Region)
	}

	d.Send(d.ctx.trans, ready.Messages)
	for _, ent := range ready.CommittedEntries {
		switch ent.EntryType {
		case eraftpb.EntryType_EntryNormal:
			reqs := new(raft_cmdpb.RaftCmdRequest)
			reqs.Unmarshal(ent.Data)
			cb := d.getCallback(ent.Index, ent.Term)

			// check regionEpoch is staled
			if err := util.CheckRegionEpoch(reqs, d.Region(), true); err != nil {
				if cb != nil {
					cb.Done(ErrResp(err))
				}
				continue
			}

			if reqs.AdminRequest == nil {
				//log.Infof("%d state=%d apply index=%d term=%d\n", d.RaftGroup.Raft.GetID(), d.RaftGroup.Raft.State, ent.Index, ent.Term)
				tag := fmt.Sprintf("id %v store %v index %v",
					d.PeerId(), d.storeID(), ent.Index)
				resps := d.execute(reqs.Requests, cb, tag)
				if cb != nil && resps != nil {
					log.Infof("cb resp")
					cb.Done(&raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}, Responses: resps})
				}
			} else {
				switch reqs.AdminRequest.CmdType {
				case raft_cmdpb.AdminCmdType_CompactLog:
					d.doCompactLog(reqs.AdminRequest.CompactLog)
				case raft_cmdpb.AdminCmdType_Split:
					d.doSplitRegion(reqs.AdminRequest.Split, cb)
				}
			}
		case eraftpb.EntryType_EntryConfChange:
			d.doConfChange(ent.Data, d.getCallback(ent.Index, ent.Term))
		}
	}
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) insertRegionToMeta(region *metapb.Region) {
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()
	storeMeta.regions[region.Id] = region
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region})
}

func (d *peerMsgHandler) doCompactLog(req *raft_cmdpb.CompactLogRequest) {
	//don't forget to write DB after change val of tuancatedState!!
	wb := engine_util.WriteBatch{}
	// notice!!!
	if req.CompactIndex < d.peerStorage.truncatedIndex() {
		return
	}
	d.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
		Index: req.CompactIndex,
		Term:  req.CompactTerm,
	}
	log.Infof("%s compactLog to index %d term %d", d.Tag, req.CompactIndex, req.CompactTerm)
	wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	wb.WriteToDB(d.ctx.engine.Kv)
	d.ScheduleCompactLog(req.CompactIndex)
}

func (d *peerMsgHandler) doConfChange(Data []byte, cb *message.Callback) {
	cc := eraftpb.ConfChange{}
	cc.Unmarshal(Data)
	newRegion := new(metapb.Region)
	util.CloneMsg(d.Region(), newRegion)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		for _, peer := range newRegion.Peers {
			if peer.Id == cc.NodeId {
				return
			}
		}
		newPeer := d.getPeerFromCache(cc.NodeId)
		if newPeer == nil {
			newPeer = new(metapb.Peer)
			err := newPeer.Unmarshal(cc.Context)
			if err != nil {
				panic(err)
			}
			d.insertPeerCache(newPeer)
		}
		newRegion.Peers = append(newRegion.Peers, newPeer)
		newRegion.RegionEpoch.ConfVer++
	case eraftpb.ConfChangeType_RemoveNode:
		if d.stopped || d.getPeerFromCache(cc.NodeId) == nil {
			return
		}
		if d.PeerId() == cc.NodeId {
			d.destroyPeer()
			return
		}
		newPeers := make([]*metapb.Peer, 0)
		for _, peer := range newRegion.Peers {
			if peer.Id != cc.NodeId {
				newPeers = append(newPeers, peer)
			}
		}
		d.removePeerCache(cc.NodeId)
		newRegion.Peers = newPeers
		newRegion.RegionEpoch.ConfVer++
	}
	d.RaftGroup.ApplyConfChange(cc)
	Wb := new(engine_util.WriteBatch)
	meta.WriteRegionState(Wb, newRegion, rspb.PeerState_Normal)
	err := d.ctx.engine.WriteKV(Wb)
	if err != nil {
		log.Panicf("doConfChange: write kv err %v", err)
	}
	d.SetRegion(newRegion)
	//storeMeta := d.ctx.storeMeta
	// storeMeta.Lock()
	// defer storeMeta.Unlock()
	// storeMeta.regions[d.regionId] = d.peerStorage.region
	d.insertRegionToMeta(newRegion)
	// maybe useless
	defer func() {
		if cb == nil {
			return
		}
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.Region(),
				},
			},
		})
	}()
}

func (d *peerMsgHandler) createRegion(id uint64, startKey, endKey []byte,
	peerId []uint64) *metapb.Region {
	newPeers := make([]*metapb.Peer, len(peerId))
	stores := make([]uint64, len(peerId))
	for idx, peer := range d.Region().Peers {
		stores[idx] = peer.StoreId
	}
	sort.Slice(peerId, func(i int, j int) bool { return peerId[i] < peerId[j] })
	sort.Slice(stores, func(i int, j int) bool { return stores[i] < stores[j] })
	for i, id := range peerId {
		newPeers[i] = &metapb.Peer{Id: id, StoreId: stores[i]}
	}
	return &metapb.Region{
		Id:       id,
		StartKey: startKey,
		EndKey:   endKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: InitEpochConfVer,
			Version: InitEpochVer,
		},
		Peers: newPeers,
	}
}

func (d *peerMsgHandler) doSplitRegion(req *raft_cmdpb.SplitRequest, cb *message.Callback) {
	var err error = nil

	defer func() {
		if cb == nil {
			return
		}
		if err != nil {
			cb.Done(ErrResp(err))
		}
		cb.Done(newCmdResp())
	}()

	region := &metapb.Region{}
	util.CloneMsg(d.Region(), region)
	log.Infof("%d splitRegion req=%+v", d.PeerId(), req)
	if bytes.Compare(req.SplitKey, region.StartKey) < 0 ||
		engine_util.ExceedEndKey(req.SplitKey, region.EndKey) {
		// TODO ?
		log.Info("region splitKey not in range[%v:%v] splitKey=%v ", region.StartKey,
			region.EndKey, req.SplitKey)
		return
	}

	region.RegionEpoch.Version++
	newRegion := d.createRegion(req.NewRegionId, req.SplitKey, region.EndKey, req.NewPeerIds)
	region.EndKey = req.SplitKey
	peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Errorf("%d doSplitRegion createPeer fail=%v", d.PeerId(), err)
		return
	}
	raftWB := new(engine_util.WriteBatch)
	kvWB := new(engine_util.WriteBatch)
	//d.peerStorage.clearExtraData(region)

	writeInitialApplyState(kvWB, newRegion.Id)
	writeInitialRaftState(raftWB, newRegion.Id)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	d.insertRegionToMeta(region)
	d.insertRegionToMeta(newRegion)
	err = d.ctx.engine.WriteKV(kvWB)
	if err != nil {
		log.Panicf("doSplitRegion: writeKv err=%v", err)
	}
	err = d.ctx.engine.WriteRaft(raftWB)
	if err != nil {
		log.Panicf("doSplitRegion: writeRaft err=%v", err)
	}

	d.SetRegion(region)
	d.ctx.router.register(peer)
	d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

	log.Infof("%d region split origin=%+v\nnewRegion=%+v\n", d.PeerId(), region, newRegion)

}

func (d *peerMsgHandler) lookStore() {
	store := d.storeID()
	func() {
		s := ""
		iter := d.ctx.engine.Kv.NewTransaction(true).NewIterator(badger.DefaultIteratorOptions)
		s += fmt.Sprintf("scan store %d Region %v\n\n", store, d.Region())
		cnt := 0
		iter.Seek([]byte{})
		for iter.Valid() {
			v, _ := iter.Item().Value()
			s += fmt.Sprintf("kv %s %s\n", iter.Item().Key(), v)
			iter.Next()
			cnt++
		}
		s += fmt.Sprintf("key count=%v", cnt)
		log.Info(s)
	}()
}

func (d *peerMsgHandler) getCallback(index, term uint64) *message.Callback {
	newProposals := make([]*proposal, 0)
	var ret *message.Callback = nil
	for i := range d.proposals {
		if d.proposals[i].index == index {
			if d.proposals[i].term == term {
				ret = d.proposals[i].cb
			} else {
				d.proposals[i].cb.Done(ErrRespStaleCommand(d.RaftGroup.Raft.Term))
			}
		} else {
			newProposals = append(newProposals, d.proposals[i])
		}
	}
	d.proposals = newProposals
	return ret
}

func (d *peerMsgHandler) execute(reqs []*raft_cmdpb.Request, cb *message.Callback, tag string) []*raft_cmdpb.Response {
	resps := make([]*raft_cmdpb.Response, 0)
	kvWB := new(engine_util.WriteBatch)
	//defer d.lookStore()
	checkKeyExist := func(key []byte) bool {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			log.Infof("key %s not in peer=%v region=%v", key, d.PeerId(), d.Region())
			if cb != nil {
				cb.Done(ErrResp(err))
			}
			return false
		}
		return true
	}

	for _, req := range reqs {
		log.Infof("%d store %d apply cmd=%+v\n", d.RaftGroup.Raft.GetID(), d.storeID(), req)
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			get := req.GetGet()
			if !checkKeyExist(get.GetKey()) {
				return nil
			}
			txn := d.ctx.engine.Kv.NewTransaction(false)
			val, err := engine_util.GetCFFromTxn(txn, get.GetCf(), get.GetKey())
			if err != nil {
				log.Infof("Get key %s not found", req.Get.Key)
				if cb != nil {
					cb.Done(ErrResp(&util.ErrKeyNotInRegion{
						Key:    get.GetKey(),
						Region: d.Region(),
					}))
				}
				return nil
			}

			log.Infof("Get key %s value %s", req.Get.GetKey(), val)
			resps = append(resps, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{
					Value: val,
				},
			})
		case raft_cmdpb.CmdType_Put:
			put := req.GetPut()
			if !checkKeyExist(put.GetKey()) {
				return nil
			}
			log.Infof("%s put key %s val %s", tag, put.Key, put.Value)
			kvWB.SetCF(put.GetCf(), put.GetKey(), put.GetValue())
			resps = append(resps, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})
		case raft_cmdpb.CmdType_Delete:
			Del := req.GetDelete()
			if !checkKeyExist(Del.GetKey()) {
				return nil
			}
			log.Infof("%s delete key %s", tag, Del.Key)
			kvWB.DeleteCF(Del.GetCf(), Del.GetKey())
			resps = append(resps, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		case raft_cmdpb.CmdType_Snap:
			resps = append(resps, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: d.Region(),
				},
			})
			if cb != nil {
				cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				d.lookStore()
			}
		}
	}
	err := d.peerStorage.Engines.WriteKV(kvWB)
	if err != nil {
		log.Panicf("execute: writeKv err=%v", err)
	}
	return resps
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	log.Infof("%s get request=%+v", d.Tag, msg)
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		log.Infof("%s sumbit req fail: %v", d.Tag, err)
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).

	if adminReq := msg.AdminRequest; adminReq != nil {
		switch adminReq.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(adminReq.TransferLeader.Peer.Id)
			cb.Done(&raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				}})
			return
		case raft_cmdpb.AdminCmdType_ChangePeer:
			// TODO: when remove leader itself, transfer leader to other
			ctx, err := adminReq.ChangePeer.Peer.Marshal()
			if err == nil {
				err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
					ChangeType: adminReq.ChangePeer.ChangeType,
					NodeId:     adminReq.ChangePeer.Peer.Id,
					Context:    ctx,
				})
				if adminReq.ChangePeer.ChangeType == eraftpb.ConfChangeType_AddNode {
					d.peer.insertPeerCache(adminReq.ChangePeer.Peer)
				}
				log.Infof("region=%+v", d.Region())
			}
		default:
			data, _ := msg.Marshal()
			err = d.RaftGroup.Propose(data)
		}
	} else {
		data, _ := msg.Marshal()
		err = d.RaftGroup.Propose(data)
	}

	if err != nil && cb != nil {
		cb.Done(ErrResp(err))
		return
	}
	index := d.RaftGroup.Raft.RaftLog.LastIndex()
	term := d.RaftGroup.Raft.GetTerm()
	if cb != nil {
		d.proposals = append(d.proposals, &proposal{
			index: index,
			term:  term,
			cb:    cb,
		})
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func MakeScan(store uint64, kv *badger.DB) func() {
	return func() {
		s := ""
		iter := kv.NewTransaction(true).NewIterator(badger.DefaultIteratorOptions)
		s += fmt.Sprintf("scan store %d\n", store)
		cnt := 0
		iter.Seek([]byte{})
		for iter.Valid() {
			v, _ := iter.Item().Value()
			s += fmt.Sprintf("kv %s %s\n", iter.Item().Key(), v)
			iter.Next()
			cnt++
		}
		s += fmt.Sprintf("key count=%v", cnt)
		log.Info(s)
	}
}
