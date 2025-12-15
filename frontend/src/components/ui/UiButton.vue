<template>
	<button
		:disabled="disabled || loading"
		class="flex-center relative transform cursor-pointer rounded-md px-5 py-2.5 font-bold text-white transition-all duration-150 ease-in-out hover:scale-102 active:scale-98 disabled:cursor-not-allowed disabled:bg-gray-600 disabled:opacity-50 disabled:hover:scale-100"
		:class="{
			'bg-accent-blue/80 hover:bg-accent-blue': variant === 'primary',
			'bg-graphite/80 hover:bg-graphite': variant === 'secondary',
		}"
	>
		<span :class="{ 'opacity-0': loading }"><slot /></span>
		<div v-if="loading" class="absolute inset-0 flex items-center justify-center">
			<svg class="h-5 w-5 animate-spin text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
				<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
				<path
					class="opacity-75"
					fill="currentColor"
					d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
				></path>
			</svg>
		</div>
	</button>
</template>

<script setup lang="ts">
withDefaults(defineProps<{ disabled?: boolean; loading?: boolean; variant?: 'primary' | 'secondary' }>(), {
	variant: 'primary',
})
</script>
