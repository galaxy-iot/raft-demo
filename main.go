package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/wh8199/log"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type logEntryData struct {
	Key   string
	Value string
}

type RAFTFSM struct {
	dbStore  *DBStore
	readOnly bool
}

func NewRAFTFsm(dataDir, nodeID string) (*RAFTFSM, error) {
	dbStore, err := NewDBStore(dataDir + "/" + nodeID)
	if err != nil {
		return nil, err
	}

	return &RAFTFSM{
		dbStore:  dbStore,
		readOnly: false,
	}, nil
}

func (r *RAFTFSM) Apply(logEntry *raft.Log) interface{} {
	e := logEntryData{}
	if err := json.Unmarshal(logEntry.Data, &e); err != nil {
		panic("Failed unmarshaling Raft log entry. This is a bug.")
	}

	log.Info(e)

	return r.dbStore.Set(e.Key, e.Value)
}

func (r *RAFTFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

func (r *RAFTFSM) Restore(snapshot io.ReadCloser) error {
	return nil
}

func newRaftTransport(tcpAddress string) (*raft.NetworkTransport, error) {
	address, err := net.ResolveTCPAddr("tcp", tcpAddress)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(address.String(),
		address, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	return transport, nil
}

var (
	dataDir     = "data"
	nodeID      = ""
	address     = ""
	httpAddress = ""
)

func init() {
	flag.StringVar(&nodeID, "node-id", "", "")
	flag.StringVar(&address, "address", "", "")
	flag.StringVar(&httpAddress, "http-address", "", "")

	flag.Parse()
}

type RaftServer struct {
	leaderNotifyCh chan bool
	raft           *raft.Raft
	mux            *http.ServeMux
	fsm            *RAFTFSM
}

func (r *RaftServer) buildRaft() error {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(nodeID)
	r.leaderNotifyCh = make(chan bool, 1)
	raftConfig.NotifyCh = r.leaderNotifyCh

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir,
		"raft-log.bolt"))
	if err != nil {
		log.Fatal(err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir,
		"raft-stable.bolt"))
	if err != nil {
		log.Fatal(err)
	}

	snapshotStore := raft.NewDiscardSnapshotStore()

	fsm, err := NewRAFTFsm(dataDir, nodeID)
	if err != nil {
		log.Fatal(err)
	}

	r.fsm = fsm

	transport, err := newRaftTransport(address)
	if err != nil {
		log.Fatal(err)
	}

	rf, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		log.Fatal(err)
	}

	r.raft = rf

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raftConfig.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}

	f := rf.BootstrapCluster(configuration)
	log.Info(f.Error())

	return nil
}

func (r *RaftServer) start() {
	r.mux = http.NewServeMux()

	r.mux.HandleFunc("/join", r.doJoin)
	r.mux.HandleFunc("/set", r.doSet)
	r.mux.HandleFunc("/get", r.doGet)

	l, err := net.Listen("tcp", httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	if err := http.Serve(l, r.mux); err != nil {
		log.Fatal(err)
	}
}

func (r *RaftServer) doJoin(w http.ResponseWriter, req *http.Request) {
	vars := req.URL.Query()

	peerAddress := vars.Get("peerAddress")
	peerNodeID := vars.Get("peernode")
	if peerAddress == "" {
		fmt.Fprint(w, "invalid peerAddress\n")
		return
	}

	addPeerFuture := r.raft.AddVoter(raft.ServerID(peerNodeID), raft.ServerAddress(peerAddress), 0, 0)
	if err := addPeerFuture.Error(); err != nil {
		fmt.Fprint(w, "internal error\n")
		return
	}

	fmt.Fprint(w, "ok")
}

func (r *RaftServer) doSet(w http.ResponseWriter, req *http.Request) {
	bytes, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprint(w, "internal error\n")
		return
	}

	applyResult := r.raft.Apply(bytes, 5*time.Second)
	if applyResult.Error() != nil {
		fmt.Fprint(w, "internal error\n")
		return
	}

	fmt.Fprint(w, "ok")
}

func (r *RaftServer) doGet(w http.ResponseWriter, req *http.Request) {
	vars := req.URL.Query()
	key := vars.Get("key")

	value, err := r.fsm.dbStore.Get(key)
	if err != nil {
		fmt.Fprint(w, "internal error\n")
		return
	}

	fmt.Fprint(w, value)
}

func main() {
	dataDir = dataDir + "/" + nodeID
	if err := os.Mkdir(dataDir, 0777); err != nil {
		log.Error(err)
	}

	log.Info("create a raft node")

	r := RaftServer{}
	go r.start()

	if err := r.buildRaft(); err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case leader := <-r.leaderNotifyCh:
			if leader {
				log.Info(r.raft.Stats())
				log.Info("become leader, enable write api")
			} else {
				log.Info("become follower, close write api")
			}
		case <-ticker.C:
			log.Info(r.raft.Leader())
		}
	}
}
