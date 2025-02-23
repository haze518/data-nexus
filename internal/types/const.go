package types

type ServerState int8

const (
	ServerStateActive ServerState = iota
	ServerStateStopped
)
