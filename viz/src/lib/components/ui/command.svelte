<script lang="ts">
	import { cn } from '$lib/utils';

	interface Props {
		class?: string;
	}

	let { class: className = '' }: Props = $props();
	let searchValue = $state('');
</script>

<div
	class={cn(
		'bg-popover text-popover-foreground flex h-full w-full flex-col overflow-hidden rounded-md',
		className
	)}
>
	<slot name="input" {searchValue} />
	<slot name="list" {searchValue} />
</div>
