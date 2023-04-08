package nakamacluster

import (
	"encoding/json"
	"fmt"
	"net"

	sockaddr "github.com/hashicorp/go-sockaddr"
)

// MetaStatus state
// Will be used to describe the state of the microservice
type MetaStatus int

const (
	META_STATUS_WAIT_READY MetaStatus = iota // waiting for ready
	META_STATUS_READYED                      // node ready
	META_STATUS_STOPED                       // node down
)

// NodeMeta Node parameters
type Meta struct {
	Id     string            `json:"id"`
	Name   string            `json:"name"`
	Addr   string            `json:"addr"`
	Type   NodeType          `json:"type"`
	Status MetaStatus        `json:"status"`
	Vars   map[string]string `json:"vars"`
}

// Marshal create JSON
func (n *Meta) Marshal() ([]byte, error) {
	return json.Marshal(n)
}

// Clone copy
func (n Meta) Clone() *Meta {
	return &n
}

// NewNodeMetaFromJSON Created via json stream NodeMeta
func NewNodeMetaFromJSON(b []byte) *Meta {
	var m Meta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return &m
}

// NewNodeMeta Create node meta information
func NewNodeMeta(id, name, addr string, nodeType NodeType, vars map[string]string) *Meta {
	return &Meta{
		Id:     id,
		Name:   name,
		Addr:   addr,
		Type:   nodeType,
		Vars:   vars,
		Status: META_STATUS_WAIT_READY,
	}
}

// NewNodeMetaFromConfig Create node meta through configuration file
func NewNodeMetaFromConfig(id, name string, t NodeType, vars map[string]string, c Config) *Meta {
	addr := ""
	ip, err := net.ResolveIPAddr("ip", c.Addr)
	if err == nil && c.Addr != "" || c.Addr != "0.0.0.0" {
		addr = ip.String()
	} else {
		addr, err = sockaddr.GetPrivateIP()
		if err != nil {
			panic(err)
		}
	}

	vars["domain"] = c.Domain
	return NewNodeMeta(id, name, fmt.Sprintf("%s:%d", addr, c.Port), t, vars)
}
