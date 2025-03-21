package types

// ServerState represents the current availability state of a server instance.
type ServerState int8

const (
	// ServerStateActive indicates that the server is currently alive and processing metrics.
	ServerStateActive ServerState = iota

	// ServerStateInactive indicates that the server is considered offline or unresponsive.
	ServerStateInactive
)

// String returns the string representation of a ServerState.
// If the state is unknown, it returns "unknown".
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

// ParseServerState parses a string and returns the corresponding ServerState.
// If the string is unrecognized, it returns -1 (an invalid ServerState).
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
