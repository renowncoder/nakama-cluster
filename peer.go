package nakamacluster

import (
	"context"
	"strconv"
	"sync"

	"github.com/doublemo/nakama-cluster/api"
	"github.com/serialx/hashring"
	"github.com/shimingyah/pool"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

type Peer interface {
	Get(id string) (*Meta, bool)
	GetByName(name string) []*Meta
	All() []*Meta
	AllToMap() map[string]*Meta
	Size() int
	SizeByName(name string) int
	Send(ctx context.Context, node *Meta, in *api.Envelope) (*api.Envelope, error)
	SendStream(ctx context.Context, clientId string, node *Meta, in *api.Envelope, md metadata.MD) (created bool, ch chan *api.Envelope, err error)
	GetWithHashRing(name, k string) (*Meta, bool)
	Sync(nodes ...*Meta)
	Update(id string, status MetaStatus)
	Delete(id string)
	Reset()
}

type PeerOptions struct {
	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// MaxConcurrentStreams limit on the number of concurrent streams to each single connection
	MaxConcurrentStreams int

	// If Reuse is true and the pool is at the MaxActive limit, then Get() reuse
	// the connection to return, If Reuse is false and the pool is at the MaxActive limit,
	// create a one-time connection to return.
	Reuse bool

	MessageQueueSize int
}

type streamContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type LocalPeer struct {
	ctx                context.Context
	ctxCancelFn        context.CancelFunc
	nodes              map[string]*Meta
	nodesByName        map[string]int
	rings              map[string]*hashring.HashRing
	grpcPool           sync.Map
	grpcStreams        sync.Map
	grpcStreamCancelFn sync.Map
	options            *PeerOptions
	logger             *zap.Logger
	sync.RWMutex
}

func (peer *LocalPeer) Get(id string) (*Meta, bool) {
	peer.RLock()
	defer peer.RUnlock()
	node, ok := peer.nodes[id]
	if !ok {
		return nil, false
	}
	return node.Clone(), true
}

func (peer *LocalPeer) GetByName(name string) []*Meta {
	nodes := make([]*Meta, 0)
	size := peer.SizeByName(name)
	if size < 1 {
		return nodes
	}
	var n string
	peer.RLock()
	for _, node := range peer.nodes {
		n = node.Name
		peer.RUnlock()
		if n == name {
			nodes = append(nodes, node.Clone())
		}
		peer.RLock()
	}
	peer.RUnlock()
	return nodes
}

func (peer *LocalPeer) All() []*Meta {
	len := peer.Size()
	nodes := make([]*Meta, len)

	i := 0
	peer.RLock()
	for _, v := range peer.nodes {
		peer.RUnlock()
		nodes[i] = v.Clone()
		i++
		peer.RLock()
	}
	peer.RUnlock()
	return nodes
}

func (peer *LocalPeer) AllToMap() map[string]*Meta {
	nodes := make(map[string]*Meta)
	peer.RLock()
	for k, v := range peer.nodes {
		peer.RUnlock()
		nodes[k] = v.Clone()
		peer.RLock()
	}
	peer.RUnlock()
	return nodes
}

func (peer *LocalPeer) Size() int {
	peer.RLock()
	defer peer.RUnlock()
	return len(peer.nodes)
}

func (peer *LocalPeer) SizeByName(name string) int {
	peer.RLock()
	defer peer.RUnlock()
	return peer.nodesByName[name]
}

func (peer *LocalPeer) Send(ctx context.Context, node *Meta, in *api.Envelope) (*api.Envelope, error) {
	p, err := peer.makeGrpcPool(node.Id, node.Addr)
	if err != nil {
		return nil, err
	}

	conn, err := p.Get()
	if err != nil {
		return nil, err
	}

	defer conn.Close()
	client := api.NewApiServerClient(conn.Value())
	return client.Call(ctx, in)
}

func (peer *LocalPeer) SendStream(ctx context.Context, clientId string, node *Meta, in *api.Envelope, md metadata.MD) (created bool, ch chan *api.Envelope, err error) {
	stream, ok := peer.grpcStreams.Load(clientId)
	if ok && stream != nil {
		err = stream.(api.ApiServer_StreamClient).Send(in)
		return
	}

	p, err := peer.makeGrpcPool(node.Id, node.Addr)
	if err != nil {
		return false, nil, err
	}

	conn, err := p.Get()
	if err != nil {
		return false, nil, err
	}

	defer conn.Close()

	client := api.NewApiServerClient(conn.Value())
	ctxStream, ok := peer.grpcStreamCancelFn.Load(node.Id)
	if ok {
		ctxS := ctxStream.(*streamContext)
		ctx = ctxS.ctx
	} else {
		ctxM, cancel := context.WithCancel(ctx)
		ctx = ctxM
		peer.grpcStreamCancelFn.Store(node.Id, &streamContext{ctx: ctxM, cancel: cancel})
	}

	ctx = metadata.NewOutgoingContext(ctx, md)
	s, err := client.Stream(ctx)
	if err != nil {
		return false, nil, err
	}

	ch = make(chan *api.Envelope, peer.options.MessageQueueSize)
	go func() {
		defer func() {
			close(ch)
			peer.grpcStreams.Delete(clientId)
		}()

		out, err := s.Recv()
		if err != nil {
			peer.logger.Warn("recv message error", zap.Error(err))
			return
		}

		select {
		case ch <- out:
		case <-ctx.Done():
			s.CloseSend()
			return
		default:
		}
	}()

	// store the client
	peer.grpcStreams.Store(clientId, s)
	return true, ch, s.Send(in)
}

func (peer *LocalPeer) GetWithHashRing(name, k string) (*Meta, bool) {
	peer.RLock()
	defer peer.RUnlock()
	ring, ok := peer.rings[name]
	if !ok {
		return nil, false
	}

	id, ok := ring.GetNode(k)
	if !ok {
		return nil, false
	}
	node, ok := peer.nodes[id]
	if !ok {
		return nil, false
	}

	return node, true
}

func (peer *LocalPeer) Sync(nodes ...*Meta) {
	nodeMap := make(map[string]bool, len(nodes))
	newNodes := make(map[string]*Meta, len(nodes))
	newRings := make(map[string]*hashring.HashRing)
	newNodesByName := make(map[string]int)
	var weight int
	for _, node := range nodes {
		newNodes[node.Id] = node
		nodeMap[node.Id] = true
		weight = 1
		if v, ok := node.Vars["weight"]; ok {
			weight, _ = strconv.Atoi(v)
			if weight < 1 {
				weight = 1
			}
		}

		if _, ok := newRings[node.Name]; !ok {
			newRings[node.Name] = hashring.NewWithWeights(map[string]int{node.Id: weight})
		} else {
			newRings[node.Name] = newRings[node.Name].AddWeightedNode(node.Id, weight)
		}
		newNodesByName[node.Name]++
	}

	peer.Lock()
	for k := range peer.nodes {
		peer.Unlock()
		if !nodeMap[k] {
			if m, ok := peer.grpcPool.Load(k); ok && m != nil {
				m.(pool.Pool).Close()
				peer.grpcPool.Delete(k)
			}

			if m, ok := peer.grpcStreamCancelFn.Load(k); ok && m != nil {
				m.(*streamContext).cancel()
				peer.grpcStreamCancelFn.Delete(k)
			}
		}
		peer.Lock()
	}
	peer.nodes = newNodes
	peer.rings = newRings
	peer.nodesByName = newNodesByName
	peer.Unlock()
}

func (peer *LocalPeer) Reset() {
	peer.Lock()
	peer.rings = make(map[string]*hashring.HashRing)
	peer.nodes = make(map[string]*Meta)
	peer.nodesByName = make(map[string]int)
	peer.Unlock()
	peer.grpcPool.Range(func(key, value any) bool {
		if v, ok := peer.grpcPool.LoadAndDelete(key); ok && v != nil {
			v.(pool.Pool).Close()
		}
		return true
	})

	peer.grpcStreamCancelFn.Range(func(key, value any) bool {
		if v, ok := peer.grpcStreamCancelFn.LoadAndDelete(key); ok && v != nil {
			v.(*streamContext).cancel()
		}
		return true
	})
}

func (peer *LocalPeer) Delete(id string) {
	peer.Lock()
	if m, ok := peer.nodes[id]; ok {
		peer.nodesByName[m.Name]--
		if _, ok := peer.rings[m.Name]; ok {
			peer.rings[m.Name] = peer.rings[m.Name].RemoveNode(m.Id)
		}

		if peer.nodesByName[m.Name] < 1 {
			delete(peer.nodesByName, m.Name)
			delete(peer.rings, m.Name)
		}
		delete(peer.nodes, m.Id)
	}
	peer.Unlock()

	if m, ok := peer.grpcPool.Load(id); ok {
		m.(pool.Pool).Close()
		peer.grpcPool.Delete(id)
	}

	if m, ok := peer.grpcStreamCancelFn.Load(id); ok {
		m.(*streamContext).cancel()
		peer.grpcStreamCancelFn.Delete(id)
	}
}

func (peer *LocalPeer) Update(id string, status MetaStatus) {
	peer.Lock()
	defer peer.Unlock()
	node, ok := peer.nodes[id]
	if !ok {
		return
	}

	newNode := node.Clone()
	newNode.Status = status
	peer.nodes[id] = newNode
}

func (peer *LocalPeer) makeGrpcPool(id, addr string) (pool.Pool, error) {
	p, ok := peer.grpcPool.Load(id)
	if ok {
		return p.(pool.Pool), nil
	}

	pool, err := pool.New(addr, pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              peer.options.MaxIdle,
		MaxActive:            peer.options.MaxActive,
		MaxConcurrentStreams: peer.options.MaxConcurrentStreams,
		Reuse:                peer.options.Reuse,
	})

	if err != nil {
		return nil, err
	}

	peer.grpcPool.Store(id, pool)
	return pool, nil
}

func NewPeer(ctx context.Context, logger *zap.Logger, options PeerOptions) *LocalPeer {
	ctx, cancel := context.WithCancel(ctx)
	s := &LocalPeer{
		ctx:         ctx,
		ctxCancelFn: cancel,
		nodes:       make(map[string]*Meta),
		nodesByName: make(map[string]int),
		rings:       make(map[string]*hashring.HashRing),
		logger:      logger,
		options:     &options,
	}
	return s
}
