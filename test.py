import os

test = [
    #'TestOneSnapshot2C',
    # 'TestSnapshotRecover2C',
    # 'TestSnapshotRecoverManyClients2C',
    # 'TestSnapshotUnreliable2C',
    # 'TestSnapshotUnreliableRecover2C',
    # 'TestSnapshotUnreliableRecoverConcurrentPartition2C'

    # 'TestTransferLeader3B',
    # 'TestBasicConfChange3B',
    # 'TestConfChangeRecover3B',
    # 'TestConfChangeRecoverManyClients3B',
    # 'TestConfChangeUnreliable3B',
    # 'TestConfChangeUnreliableRecover3B',
    # 'TestConfChangeSnapshotUnreliableRecover3B',
    # 'TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B',

    # 'TestOneSplit3B',
    # 'TestSplitRecover3B',
    # 'TestSplitRecoverManyClients3B',
    # 'TestSplitUnreliable3B',
    # 'TestSplitUnreliableRecover3B',
    # 'TestSplitConfChangeSnapshotUnreliableRecover3B',
    'TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B'
]
max = 100
i = 0
errFound = True
while True:
    os.system('rm -rf /tmp/test-raftstore*')
    if max != None and i >= max:
        break
    i += 1
    print(i)
    for t in test:
        print(t)
        log_file = 'logs/' + t + '_' + str(i) + ".log"
        ret = os.system('GOMAXPROCS=8 GO111MODULE=on go test -v -timeout 1h  --count=1 --parallel=1 -p=1 ./raft ./kv/test_raftstore -run ' + t + ' > ' + log_file)
        if ret != 0:
            print('fail')
            if errFound:
                exit(0)
        else:
            os.system('rm ' + log_file)