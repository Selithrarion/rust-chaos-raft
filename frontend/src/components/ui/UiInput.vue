<template>
	<div class="flex-1">
		<label v-if="label" :for="id" class="mb-2 block text-sm font-medium text-gray-300">{{ label }}</label>
		<div class="flex flex-1 items-center gap-3">
			<input
				:id="id"
				:type="type"
				:value="modelValue"
				:placeholder="placeholder"
				@input="$emit('update:modelValue', ($event.target as HTMLInputElement).value)"
				class="bg-background-light focus:border-accent-blue block w-full flex-grow rounded-md border-2 border-transparent p-2.5 text-white transition-colors outline-none"
			/>
			<slot name="append" />
		</div>
		<p v-if="hint" class="mt-2 text-xs text-gray-400">{{ hint }}</p>
	</div>
</template>

<script setup lang="ts">
withDefaults(
	defineProps<{
		modelValue: string | number | null
		id?: string
		label?: string
		hint?: string
		placeholder?: string
		type?: string
	}>(),
	{ type: 'text' },
)
defineEmits(['update:modelValue'])
</script>

<style scoped>
input[type='number']::-webkit-inner-spin-button,
input[type='number']::-webkit-outer-spin-button {
	-webkit-appearance: none;
	margin: 0;
}
input[type='number'] {
	-moz-appearance: textfield;
}
</style>
