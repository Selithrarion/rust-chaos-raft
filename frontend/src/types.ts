export enum ConnectionStatus {
	DISCONNECTED = 'DISCONNECTED',
	CONNECTING = 'CONNECTING',
	CONNECTED = 'CONNECTED',
	ERROR = 'ERROR',
}

export interface ClusterStatus {
	leader_id: number
}

export enum NodeStatus {
	ALIVE = 'alive',
	DEAD = 'dead',
}

export interface ClusterNode {
	id: number
	isLeader: boolean
	status: NodeStatus
}

export enum ChaosTestStatus {
	IDLE = 'idle',
	RUNNING = 'running',
	FINISHED = 'finished',
}

export interface PingResult {
	alive_nodes: number[]
}
