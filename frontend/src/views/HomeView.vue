<template>
	<div class="mx-auto max-w-7xl p-4 lg:p-8">
		<div class="flex items-end justify-between">
			<div class="flex-grow">
				<h1 class="text-3xl font-bold text-white">Raft Dashboard</h1>
				<!--				<p class="text-graphite mt-1">-->
				<!--					Status: <span :class="statusColor">{{ store.connectionStatus }}</span>-->
				<!--					<span class="ml-4">-->
				<!--						UI-Backend Latency: <span class="font-semibold text-white">{{ store.roundtripLatency }}ms</span>-->
				<!--					</span>-->
				<!--				</p>-->
			</div>
			<div class="flex w-1/3 flex-col">
				<div class="flex items-center gap-4">
					<UiInput
						v-model="command"
						placeholder="SET key=value"
						@keyup.enter="handleSendCommand"
						@keyup.up="navigateHistory(1)"
						@keyup.down="navigateHistory(-1)"
					/>
					<UiButton @click="handleSendCommand" variant="primary" :loading="isLoading"> Send </UiButton>
				</div>
				<p v-if="store.lastError" class="text-bearish-red mt-2 text-sm">{{ store.lastError }}</p>
			</div>
		</div>

		<div v-if="store.commandHistory.length > 0" class="mt-4 text-right">
			<p class="text-graphite text-sm">History (use ↑/↓):</p>
			<div class="flex flex-col items-end">
				<button
					v-for="cmd in store.commandHistory.slice(0, 3)"
					:key="cmd"
					@click="reuseCommand(cmd)"
					class="text-accent-blue/70 hover:text-accent-blue cursor-pointer text-sm"
				>
					{{ cmd }}
				</button>
			</div>
		</div>

		<div class="mt-12 border-t border-white/10 pt-8">
			<div class="flex items-center justify-between">
				<div>
					<h2 class="text-2xl font-bold text-white">Chaos</h2>
					<p class="text-graphite mt-1">Kill the leader node</p>
				</div>
				<div class="flex items-start gap-4">
					<UiButton
						@click="store.runChaosTest"
						variant="secondary"
						:loading="store.chaosTestResult.status === ChaosTestStatus.RUNNING"
						:disabled="store.aliveNodesCount <= 2"
						class-name="bg-bearish-red/80 hover:bg-bearish-red disabled:bg-bearish-red/20"
					>
						Kill Leader & Measure Downtime
					</UiButton>
				</div>
			</div>
			<div
				v-if="store.chaosTestResult.status === ChaosTestStatus.FINISHED"
				class="bg-background-light mt-4 rounded-lg p-4 text-center"
			>
				<p class="text-lg">
					Cluster recovered! New leader elected in
					<span class="text-bullish-green font-bold">{{ store.chaosTestResult.downtime }}ms</span>. No data was lost.
				</p>
			</div>
		</div>

		<div class="mt-12 flex items-center justify-center gap-16">
			<div v-for="node in store.nodes" :key="node.id" class="flex flex-col items-center gap-4">
				<div
					class="flex-center relative h-32 w-32 rounded-full border-4 transition-all"
					:class="{
						'border-accent-blue shadow-accent-blue/40 shadow-[0_0_30px_0px]': node.isLeader,
						'border-graphite': !node.isLeader && node.status === NodeStatus.ALIVE,
						'border-graphite/30': node.status === NodeStatus.DEAD,
					}"
				>
					<span
						class="text-4xl font-bold"
						:class="{
							'text-white': node.isLeader,
							'text-graphite': !node.isLeader && node.status === NodeStatus.ALIVE,
							'text-graphite/30': node.status === NodeStatus.DEAD,
						}"
					>
						{{ node.id }}
					</span>
					<svg
						v-if="node.status === NodeStatus.DEAD"
						class="text-bearish-red/50 absolute h-16 w-16"
						fill="none"
						viewBox="0 0 24 24"
						stroke="currentColor"
					>
						<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
					</svg>
				</div>
				<span
					class="text-lg font-semibold"
					:class="{
						'text-accent-blue': node.isLeader,
						'text-graphite': !node.isLeader && node.status === NodeStatus.ALIVE,
						'text-graphite/50': node.status === NodeStatus.DEAD,
					}"
				>
					{{ node.isLeader ? 'Leader' : node.status === NodeStatus.ALIVE ? 'Follower' : 'Dead' }}</span
				>
			</div>
		</div>
	</div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { useAppStore } from '@/stores/app'
import { ConnectionStatus, NodeStatus, ChaosTestStatus } from '@/types'
import UiInput from '@/components/ui/UiInput.vue'
import UiButton from '@/components/ui/UiButton.vue'

const store = useAppStore()

const command = ref('')
const isLoading = ref(false)
const historyIndex = ref(-1)

async function handleSendCommand() {
	if (!command.value) return
	isLoading.value = true
	try {
		await store.sendCommand(command.value)
		historyIndex.value = -1
		if (!store.lastError) {
			command.value = ''
		}
	} finally {
		isLoading.value = false
	}
}

function navigateHistory(direction: 1 | -1) {
	const newIndex = historyIndex.value + direction
	if (newIndex >= 0 && newIndex < store.commandHistory.length) {
		historyIndex.value = newIndex
		command.value = store.commandHistory[newIndex]!
	}
}

function reuseCommand(cmd: string) {
	command.value = cmd
}

const statusColor = computed(() => {
	switch (store.connectionStatus) {
		case ConnectionStatus.CONNECTED:
			return 'text-bullish-green'
		case ConnectionStatus.CONNECTING:
			return 'text-accent-blue'
		case ConnectionStatus.ERROR:
			return 'text-bearish-red'
		default:
			return 'text-graphite'
	}
})
</script>
