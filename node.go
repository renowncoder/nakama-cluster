package nakamacluster

// node type
type NodeType int

const (
	NODE_TYPE_NAKAMA        NodeType = iota + 1 // nakama main service
	NODE_TYPE_MICROSERVICES                     // microservice
)
