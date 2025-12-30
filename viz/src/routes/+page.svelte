<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import {
		initDuckDB,
		queryData,
		queryTableData,
		isDuckDBLoaded,
		isDuckDBInitializing,
		requiresDuckDB,
		type TradeData
	} from '$lib/duckdb';
	import TreeMap from '$lib/components/TreeMap.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import Button from '$lib/components/ui/button.svelte';
	import Card from '$lib/components/ui/card.svelte';
	import Popover from '$lib/components/ui/popover.svelte';
	import Command from '$lib/components/ui/command.svelte';
	import { ChevronsUpDown } from 'lucide-svelte';
	import { cn } from '$lib/utils';

	type VizMode = 'Commodity' | 'Country' | 'Port';
	type TradeMode = 'All' | 'Import' | 'Export';

	// Loading state management
	const loadingState = $state({
		initial: true,
		table: false,
		duckDB: false
	});

	let error = $state<string | null>(null);
	let treemapData = $state<TradeData[]>([]);
	let tableData = $state<TradeData[]>([]);
	let mode = $state<VizMode>('Country');
	let tradeMode = $state<TradeMode>('All');
	let year = $state<number>(2025);
	let selectedItem = $state<string | undefined>(undefined);
	let filter = $state<{ commodity?: string; country?: string; port?: string }>({});
	const tablePageSize = 50;
	let allDataShown = $state(false);

	// Popover states
	let tradePopoverOpen = $state(false);
	let commodityPopoverOpen = $state(false);
	let yearPopoverOpen = $state(false);

	// Function to scroll to top of page
	function scrollToTop() {
		window.scrollTo({ top: 0, behavior: 'smooth' });
	}

	// Helper: Clear all data
	function clearData() {
		treemapData = [];
		tableData = [];
	}

	// Helper: Ensure DuckDB is ready (initialized and loaded)
	async function ensureDuckDBReady(): Promise<void> {
		if (isDuckDBLoaded()) return;

		if (!isDuckDBInitializing()) {
			await initDuckDB();
		} else {
			// Wait for existing initialization to complete
			while (isDuckDBInitializing() && !isDuckDBLoaded()) {
				await new Promise((resolve) => setTimeout(resolve, 100));
			}
		}
	}

	// Helper: Reset loading states
	function resetLoadingStates() {
		loadingState.table = false;
		loadingState.duckDB = false;
	}

	// Function to update URL with current state
	function updateURL() {
		// Don't update URL if we're restoring from URL to avoid loops
		if (isRestoringFromURL) return;

		const params = new URLSearchParams();
		params.set('mode', mode);
		params.set('tradeMode', tradeMode);
		params.set('year', year.toString());
		if (filter.commodity) params.set('commodity', filter.commodity);
		if (filter.country) params.set('country', filter.country);
		if (filter.port) params.set('port', filter.port);

		const newUrl = params.toString() ? `?${params.toString()}` : '';
		// Use pushState to create history entries for back button navigation
		goto(newUrl, { replaceState: false, noScroll: true });
	}

	// Function to read URL params and restore state
	function restoreStateFromURL() {
		const params = $page.url.searchParams;
		if (params.has('mode')) {
			const urlMode = params.get('mode') as VizMode;
			if (['Commodity', 'Country', 'Port'].includes(urlMode)) {
				mode = urlMode;
			}
		}
		if (params.has('tradeMode')) {
			const urlTradeMode = params.get('tradeMode') as TradeMode;
			if (['All', 'Import', 'Export'].includes(urlTradeMode)) {
				tradeMode = urlTradeMode;
			}
		}
		if (params.has('year')) {
			const urlYear = parseInt(params.get('year') || '2025', 10);
			if (!isNaN(urlYear) && urlYear >= 2003 && urlYear <= 2025) {
				year = urlYear;
			}
		}
		// Clear all filters first, then set only what's in URL
		filter = {};
		if (params.has('commodity')) {
			filter.commodity = params.get('commodity') || undefined;
		}
		if (params.has('country')) {
			filter.country = params.get('country') || undefined;
		}
		if (params.has('port')) {
			filter.port = params.get('port') || undefined;
		}
		// Clear selectedItem when restoring from URL
		selectedItem = undefined;
	}

	// Track if we're updating from URL to avoid infinite loops
	let isRestoringFromURL = $state(false);
	let previousURL = $state<string>('');

	// Watch for URL changes (browser back/forward navigation)
	$effect(() => {
		const currentURL = $page.url.search;
		// Only restore if URL actually changed and we're not in initial loading
		if (!loadingState.initial && currentURL !== previousURL) {
			const params = $page.url.searchParams;
			const urlMode = params.get('mode') || 'Country';
			const urlTradeMode = params.get('tradeMode') || 'All';
			const urlYear = params.get('year') ? parseInt(params.get('year') || '2025', 10) : 2025;
			const urlCommodity = params.get('commodity') || undefined;
			const urlCountry = params.get('country') || undefined;
			const urlPort = params.get('port') || undefined;

			// Only restore if state differs from URL
			if (
				mode !== urlMode ||
				tradeMode !== urlTradeMode ||
				year !== urlYear ||
				filter.commodity !== urlCommodity ||
				filter.country !== urlCountry ||
				filter.port !== urlPort
			) {
				isRestoringFromURL = true;
				previousURL = currentURL; // Update before restoring to avoid re-triggering
				restoreStateFromURL();
				loadData();
				scrollToTop();
				isRestoringFromURL = false;
			} else {
				previousURL = currentURL;
			}
		} else if (currentURL !== previousURL) {
			previousURL = currentURL;
		}
	});

	onMount(async () => {
		try {
			// Initialize previousURL to current URL
			previousURL = $page.url.search;
			// Restore state from URL first
			restoreStateFromURL();
			// Start loading data immediately (JSON files don't need DuckDB)
			// Initialize DuckDB in background (non-blocking)
			initDuckDB().catch((e) => console.warn('DuckDB initialization failed:', e));
			// Load initial data with optimized files (will use JSON if available)
			await loadData(true);
			loadingState.initial = false;
		} catch (e) {
			console.error('Error loading data:', e);
			error = e instanceof Error ? e.message : 'Failed to load data';
			loadingState.initial = false;
		}
	});

	async function loadData(initialLoad: boolean = false) {
		try {
			const hasFilter = filter && Object.keys(filter).length > 0;
			const needsDuckDB = requiresDuckDB(filter, !hasFilter, year);

			// Set up loading state for DuckDB-dependent queries
			if (needsDuckDB) {
				loadingState.duckDB = true;
				clearData();
				await ensureDuckDBReady();
			} else {
				loadingState.duckDB = false;
			}

			// Load treemap data
			const treemapLimit = initialLoad ? 100 : undefined;
			treemapData = await queryData(mode, tradeMode, filter, treemapLimit, !hasFilter, year);

			// Load table data
			if (!initialLoad) {
				loadingState.table = true;
			}
			tableData = await queryTableData(mode, tradeMode, filter, tablePageSize, 0, !hasFilter, year);

			allDataShown = false;
			resetLoadingStates();
		} catch (e) {
			console.error('Error in loadData:', e);
			error = e instanceof Error ? e.message : 'Failed to query data';
			resetLoadingStates();
		}
	}

	async function showAllTableData() {
		if (loadingState.table || allDataShown) return;

		try {
			loadingState.table = true;
			const hasFilter = filter && Object.keys(filter).length > 0;
			// Load all data at once (no limit, no offset)
			const allData = await queryTableData(mode, tradeMode, filter, undefined, 0, !hasFilter, year);
			tableData = allData;
			allDataShown = true;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load all data';
		} finally {
			loadingState.table = false;
		}
	}

	function handleModeChange(newMode: VizMode) {
		mode = newMode;
		selectedItem = undefined;
		filter = {};
		updateURL();
		scrollToTop();
		loadData();
	}

	function handleTradeModeChange(newTradeMode: TradeMode) {
		tradeMode = newTradeMode;
		updateURL();
		scrollToTop();
		loadData();
	}

	function handleYearChange(newYear: number) {
		year = newYear;
		updateURL();
		scrollToTop();
		loadData();
	}

	function handleSelect(item: string) {
		selectedItem = item;

		// Switch mode based on current mode
		if (mode === 'Commodity') {
			// When commodity is selected, switch to Country mode filtered by that commodity
			mode = 'Country';
			filter = { commodity: item };
		} else if (mode === 'Country') {
			// When country is selected, switch to Commodity mode filtered by that country
			mode = 'Commodity';
			filter = { country: item };
		} else if (mode === 'Port') {
			// When port is selected, switch to Commodity mode filtered by that port
			mode = 'Commodity';
			filter = { port: item };
		}

		updateURL();
		scrollToTop();
		loadData();
	}

	function resetView() {
		mode = 'Commodity';
		tradeMode = 'All';
		year = 2025;
		selectedItem = undefined;
		filter = {};
		updateURL();
		scrollToTop();
		loadData();
	}

	const title = $derived(() => {
		const modeLabel =
			mode === 'Commodity' ? 'Commodities' : mode === 'Country' ? 'Countries' : 'Ports';
		const tradeLabel =
			tradeMode === 'All' ? 'All Trade' : tradeMode === 'Import' ? 'Imports' : 'Exports';
		return `${modeLabel} - ${tradeLabel}`;
	});

	const headerTitle = $derived(() => {
		if (tradeMode === 'All') {
			return 'Foreign Trade';
		} else if (tradeMode === 'Import') {
			return 'Foreign Import';
		} else {
			return 'Foreign Export';
		}
	});

	const selectedItemLabel = $derived(() => {
		if (filter.commodity) {
			return filter.commodity;
		}
		if (filter.country) {
			return filter.country;
		}
		if (filter.port) {
			return filter.port;
		}
		return null;
	});

	const tradeOptions = [
		{ value: 'All', label: 'Import + Export' },
		{ value: 'Import', label: 'Import' },
		{ value: 'Export', label: 'Export' }
	];

	const commodityOptions = [
		{ value: 'Country', label: 'Country' },
		{ value: 'Commodity', label: 'Commodity' },
		{ value: 'Port', label: 'Port' }
	];

	// Generate year options (2003 to 2025)
	const yearOptions = Array.from({ length: 2025 - 2003 + 1 }, (_, i) => 2003 + i)
		.reverse()
		.map((y) => ({ value: y, label: y.toString() }));

	function handleTradeSelect(value: string) {
		tradeMode = value as TradeMode;
		tradePopoverOpen = false;
		updateURL();
		scrollToTop();
		loadData();
	}

	function handleCommoditySelect(value: string) {
		handleModeChange(value as VizMode);
		commodityPopoverOpen = false;
	}

	function handleYearSelect(value: number | string) {
		const yearValue = typeof value === 'number' ? value : parseInt(value, 10);
		if (!isNaN(yearValue)) {
			handleYearChange(yearValue);
			yearPopoverOpen = false;
		}
	}
</script>

<svelte:head>
	<title>{title()}</title>
</svelte:head>

<div class="min-h-screen bg-background p-3 sm:p-4 md:p-6 lg:p-8">
	<div class="mx-auto space-y-4 sm:space-y-6">
		<!-- Header -->
		<div class="flex flex-col gap-2">
			<div class="flex flex-wrap items-center gap-2">
				<h1 class="text-2xl font-bold sm:text-3xl">
					<Popover bind:open={tradePopoverOpen}>
						<span slot="trigger">
							<Button
								variant="ghost"
								class="h-auto p-0 text-2xl font-bold text-foreground underline decoration-dotted underline-offset-4 hover:bg-transparent hover:decoration-solid sm:text-3xl"
								role="combobox"
							>
								{headerTitle()}
								<ChevronsUpDown class="ml-2 h-4 w-4 opacity-50 sm:h-5 sm:w-5" />
							</Button>
						</span>
						<span slot="content">
							<Command>
								<div slot="list" let:searchValue>
									<div class="max-h-[300px] overflow-x-hidden overflow-y-auto">
										<div class="overflow-hidden p-2 text-foreground">
											{#each tradeOptions as option}
												{@const isSelected = tradeMode === option.value}
												<button
													type="button"
													role="option"
													aria-selected={isSelected}
													tabindex="0"
													class="relative flex w-full cursor-default items-center rounded-md px-4 py-3 text-left text-base font-medium transition-colors outline-none select-none hover:bg-accent"
													onclick={() => handleTradeSelect(option.value)}
													onkeydown={(e) => {
														if (e.key === 'Enter' || e.key === ' ') {
															e.preventDefault();
															handleTradeSelect(option.value);
														}
													}}
												>
													<span class={cn(isSelected ? 'font-bold' : '')}>{option.label}</span>
												</button>
											{/each}
										</div>
									</div>
								</div>
							</Command>
						</span>
					</Popover>
					<span class="text-2xl font-bold sm:text-3xl"> by </span>
					<Popover bind:open={commodityPopoverOpen}>
						<span slot="trigger">
							<Button
								variant="ghost"
								class="h-auto p-0 text-2xl font-bold text-foreground underline decoration-dotted underline-offset-4 hover:bg-transparent hover:decoration-solid sm:text-3xl"
								role="combobox"
							>
								{mode}
								<ChevronsUpDown class="ml-2 h-4 w-4 opacity-50 sm:h-5 sm:w-5" />
							</Button>
						</span>
						<span slot="content">
							<Command>
								<div slot="list" let:searchValue>
									<div class="max-h-[300px] overflow-x-hidden overflow-y-auto">
										<div class="overflow-hidden p-2 text-foreground">
											{#each commodityOptions as option}
												{@const isSelected = mode === option.value}
												<button
													type="button"
													role="option"
													aria-selected={isSelected}
													tabindex="0"
													class="relative flex w-full cursor-default items-center rounded-md px-4 py-3 text-left text-base font-medium transition-colors outline-none select-none hover:bg-accent"
													onclick={() => handleCommoditySelect(option.value)}
													onkeydown={(e) => {
														if (e.key === 'Enter' || e.key === ' ') {
															e.preventDefault();
															handleCommoditySelect(option.value);
														}
													}}
												>
													<span class={cn(isSelected ? 'font-bold' : '')}>{option.label}</span>
												</button>
											{/each}
										</div>
									</div>
								</div>
							</Command>
						</span>
					</Popover>
					<span class="text-2xl font-bold sm:text-3xl"> in </span>
					<Popover bind:open={yearPopoverOpen}>
						<span slot="trigger">
							<Button
								variant="ghost"
								class="h-auto p-0 text-2xl font-bold text-foreground underline decoration-dotted underline-offset-4 hover:bg-transparent hover:decoration-solid sm:text-3xl"
								role="combobox"
							>
								{year}
								<ChevronsUpDown class="ml-2 h-4 w-4 opacity-50 sm:h-5 sm:w-5" />
							</Button>
						</span>
						<span slot="content">
							<Command>
								<div slot="list" let:searchValue>
									<div class="max-h-[300px] overflow-x-hidden overflow-y-auto">
										<div class="overflow-hidden p-2 text-foreground">
											{#each yearOptions as option}
												{@const isSelected = year === option.value}
												<button
													type="button"
													role="option"
													aria-selected={isSelected}
													tabindex="0"
													class="relative flex w-full cursor-default items-center rounded-md px-4 py-3 text-left text-base font-medium transition-colors outline-none select-none hover:bg-accent"
													onclick={() => handleYearSelect(option.value)}
													onkeydown={(e) => {
														if (e.key === 'Enter' || e.key === ' ') {
															e.preventDefault();
															handleYearSelect(option.value);
														}
													}}
												>
													<span class={cn(isSelected ? 'font-bold' : '')}>{option.label}</span>
												</button>
											{/each}
										</div>
									</div>
								</div>
							</Command>
						</span>
					</Popover>
				</h1>
			</div>
			{#if selectedItemLabel()}
				<p class="text-sm text-muted-foreground sm:text-base">{selectedItemLabel()}</p>
			{/if}
		</div>

		<!-- Loading State -->
		{#if loadingState.initial}
			<Card class="border-none shadow-none">
				<div class="text-center text-muted-foreground">Loading data...</div>
			</Card>
		{/if}

		<!-- Error State -->
		{#if error}
			<Card class="border-none shadow-none">
				<div class="text-center text-red-500">Error: {error}</div>
			</Card>
		{/if}

		<!-- Tree Map -->
		{#if !loadingState.initial && !error}
			<Card class="border-none shadow-none">
				<div class="-mx-3 overflow-x-auto px-3 sm:-mx-4 sm:px-4">
					{#if loadingState.duckDB}
						<div class="flex h-96 items-center justify-center text-muted-foreground">
							Loading data...
						</div>
					{:else if treemapData.length > 0}
						<TreeMap data={treemapData} {mode} onSelect={handleSelect} />
					{:else}
						<div class="flex h-96 items-center justify-center text-muted-foreground">
							No data available for the selected filters
						</div>
					{/if}
				</div>
			</Card>

			<!-- Data Table -->
			<Card class="border-none shadow-none">
				{#if loadingState.duckDB}
					<div class="flex h-48 items-center justify-center text-muted-foreground">
						Loading data...
					</div>
				{:else if tableData.length > 0}
					<DataTable data={tableData} {mode} {tradeMode} {selectedItem} onSelect={handleSelect} />
				{:else}
					<div class="flex h-48 items-center justify-center text-muted-foreground">
						No data available for the selected filters
					</div>
				{/if}
				{#if tableData.length > 0 && !allDataShown && !loadingState.duckDB}
					<div class="mt-4 flex justify-center">
						<Button
							variant="outline"
							onclick={showAllTableData}
							disabled={loadingState.table}
							class="w-full sm:w-auto"
						>
							{loadingState.table ? 'Loading...' : 'Show All'}
						</Button>
					</div>
				{/if}
			</Card>
		{/if}

		<!-- Footer -->
		{#if !loadingState.initial && !error}
			<Card class="border-none shadow-none">
				<div class="text-center text-sm text-muted-foreground">
					<p>
						Made using <a href="https://kit.svelte.dev" target="_blank" class="underline"
							>SvelteKit</a
						>, with assistance from
						<a href="https://anthropic.com/claude" target="_blank" class="underline">Claude</a>.
						Data and code available on
						<a
							href="https://github.com/Vonter/india-export-import"
							target="_blank	"
							class="underline">GitHub</a
						>.
					</p>
				</div>
			</Card>
		{/if}
	</div>
</div>
