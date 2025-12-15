import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { listen, type Event } from '@tauri-apps/api/event'
import { ConnectionStatus, type ClusterStatus, type ClusterNode, type PingResult, NodeStatus, ChaosTestStatus } from '@/types'
import { invoke } from '@tauri-apps/api/core'
import { useIntervalFn } from '@vueuse/core'

export const useAppStore = defineStore('app', () => {
	const connectionStatus = ref<ConnectionStatus>(ConnectionStatus.DISCONNECTED)
	const lastError = ref<string | null>(null)
	const roundtripLatency = ref(0)
	const nodes = ref<ClusterNode[]>([
		{ id: 1, isLeader: false, status: NodeStatus.DEAD },
		{ id: 2, isLeader: false, status: NodeStatus.DEAD },
		{ id: 3, isLeader: false, status: NodeStatus.DEAD },
	])
	const commandHistory = ref<string[]>([])
	const chaosTestResult = ref<{ downtime: number | null; status: ChaosTestStatus }>({ downtime: null, status: ChaosTestStatus.IDLE })

	listen<string>('error', (event: Event<string>) => {
		lastError.value = event.payload
		connectionStatus.value = ConnectionStatus.ERROR
	}).catch(console.error)

	async function measureLatency() {
		const start = performance.now()
		await invoke('ping')
		const end = performance.now()
		roundtripLatency.value = Math.round(end - start)
	}

	async function initializeConnection() {
		connectionStatus.value = ConnectionStatus.CONNECTING
		try {
			connectionStatus.value = await invoke<ConnectionStatus>('get_connection_status')
		} catch (e) {
			console.error('Failed to initialize connection:', e)
			connectionStatus.value = ConnectionStatus.ERROR
			lastError.value = e as string
		}
	}

	async function fetchClusterStatus() {
		try {
			const pingResult = await invoke<PingResult>('ping_nodes')
			nodes.value.forEach((node) => {
				node.status = pingResult.alive_nodes.includes(node.id) ? NodeStatus.ALIVE : NodeStatus.DEAD
			})

			if (pingResult.alive_nodes.length > 0) {
				const status = await invoke<ClusterStatus>('get_cluster_status')
				nodes.value.forEach((node) => {
					node.isLeader = node.id === status.leader_id
				})
			}
		} catch (e) {
			console.error('Failed to fetch cluster status:', e as string)
			nodes.value.forEach((node) => (node.isLeader = false))
		}
	}

	async function sendCommand(command: string) {
		lastError.value = null
		try {
			await invoke('client_request', { command })
			if (!commandHistory.value.includes(command)) {
				commandHistory.value.unshift(command)
			}
		} catch (e) {
			console.error('Failed to send command:', e)
			lastError.value = e as string
		}
	}

	async function runChaosTestAndMeasure() {
		chaosTestResult.value = { downtime: null, status: ChaosTestStatus.RUNNING }
		lastError.value = null

		try {
			if (aliveNodesCount.value <= 2) {
				throw new Error('Cannot run test with less than 3 nodes. Reset the cluster')
			}

			const downtime = await invoke<number>('measure_recovery_time')
			chaosTestResult.value = { downtime, status: ChaosTestStatus.FINISHED }
		} catch (e) {
			lastError.value = e as string
			chaosTestResult.value.status = ChaosTestStatus.IDLE
		} finally {
			await fetchClusterStatus()
		}
	}

	const aliveNodesCount = computed(() => nodes.value.filter((n) => n.status === NodeStatus.ALIVE).length)

	// TODO: maybe use tauri events
	useIntervalFn(fetchClusterStatus, 1000)
	useIntervalFn(measureLatency, 2000)

	async function init() {
		await initializeConnection()
		await fetchClusterStatus()
	}
	init()

	return {
		connectionStatus,
		lastError,
		roundtripLatency,
		nodes,
		commandHistory,
		chaosTestResult,
		aliveNodesCount,
		measureLatency,
		initializeConnection,
		fetchClusterStatus,
		sendCommand,
		runChaosTest: runChaosTestAndMeasure,
	}
})
