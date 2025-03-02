package types

type ServerState int8

const (
	ServerStateActive ServerState = iota
	ServerStateInactive
)

func (s ServerState) String() string {
	switch s {
	case ServerStateActive:
		return "active"
	case ServerStateInactive:
		return "inactive"
	default:
		return "unknown"
	}
}

func ParseServerState(stateStr string) ServerState {
	switch stateStr {
	case "active":
		return ServerStateActive
	case "inactive":
		return ServerStateInactive
	default:
		return -1
	}
}
