<script lang="ts">
	import type { TradeData } from '$lib/duckdb';
	import { cn, formatCurrency } from '$lib/utils';

	interface Props {
		data: TradeData[];
		mode: 'Commodity' | 'Country' | 'Port';
		tradeMode: 'All' | 'Import' | 'Export';
		selectedItem?: string;
		onSelect?: (item: string) => void;
	}

	let { data, mode, tradeMode, selectedItem, onSelect }: Props = $props();

	const columnHeader = $derived(() => {
		if (tradeMode === 'All') {
			return 'Total Trade';
		} else if (tradeMode === 'Import') {
			return 'Total Imports';
		} else {
			return 'Total Exports';
		}
	});

	function handleClick(item: string) {
		if (onSelect) {
			onSelect(item);
		}
	}
</script>

<div class="w-full overflow-x-auto">
	<table class="w-full border-collapse text-sm sm:text-base">
		<thead>
			<tr class="border-b">
				<th class="p-2 text-left sm:p-3">
					{mode === 'Commodity' ? 'Commodity' : mode === 'Country' ? 'Country' : 'Port'}
				</th>
				<th class="w-32 p-2 text-right sm:w-40 sm:p-3">{columnHeader()}</th>
			</tr>
		</thead>
		<tbody>
			{#each data as row, index (JSON.stringify(row))}
				<tr
					class={cn(
						'cursor-pointer border-b hover:bg-accent',
						selectedItem === row[mode] ? 'bg-accent' : ''
					)}
					onclick={() => handleClick(row[mode])}
				>
					<td class="max-w-0 truncate p-2 font-medium sm:p-3">
						<div class="truncate" title={row[mode] || 'N/A'}>
							{row[mode] || 'N/A'}
						</div>
					</td>
					<td class="w-32 p-2 text-right font-semibold whitespace-nowrap sm:w-40 sm:p-3">
						{formatCurrency(row['Total USD Value'])}
					</td>
				</tr>
			{/each}
		</tbody>
	</table>
</div>
