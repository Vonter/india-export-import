<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { hierarchy, treemap } from 'd3-hierarchy';
	import { Plot, Rect, Text } from 'svelteplot';
	import type { TradeData } from '$lib/duckdb';
	import { formatCurrency } from '$lib/utils';

	interface Props {
		data: TradeData[];
		mode: 'Commodity' | 'Country' | 'Port';
		onSelect?: (item: string) => void;
	}

	let { data, mode, onSelect }: Props = $props();
	let containerEl: HTMLElement | null = $state(null);
	let containerWidth = $state(1200);
	let containerHeight = $state(600);
	let activeNode: any | null = $state(null);
	let isPinned = $state(false);
	let isMobile = $state(false);

	// Generate random light colors for each item (consistent per item)
	const generateColor = (seed: string): string => {
		// Simple hash function for consistent colors
		let hash = 0;
		for (let i = 0; i < seed.length; i++) {
			hash = seed.charCodeAt(i) + ((hash << 5) - hash);
		}
		// Generate HSL color with light background for good contrast with black text
		const hue = Math.abs(hash) % 360;
		const saturation = 30 + (Math.abs(hash) % 30); // 30-60%
		const lightness = 75 + (Math.abs(hash) % 20); // 75-95% (light colors)
		return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
	};

	// Update container dimensions on resize
	onMount(() => {
		// Detect mobile on mount
		const checkMobile = () => {
			isMobile = window.innerWidth < 768;
		};
		checkMobile();
		window.addEventListener('resize', checkMobile);

		if (!containerEl) return;
		const observer = new ResizeObserver(([entry]) => {
			containerWidth = entry.contentRect.width;
			containerHeight = entry.contentRect.height || 600;
		});
		observer.observe(containerEl);
		return () => {
			observer.disconnect();
			window.removeEventListener('resize', checkMobile);
		};
	});

	// Prepare data for treemap
	const treemapData = $derived.by(() => {
		if (!data || data.length === 0) return [];
		const limited = data.slice(0, 100);
		return limited
			.filter((d) => {
				const name = d[mode];
				const value = d['Total USD Value'];
				return name && name.trim() !== '' && value != null && value > 0;
			})
			.map((d) => ({
				name: String(d[mode]),
				value: Number(d['Total USD Value']),
				color: generateColor(String(d[mode]))
			}));
	});

	// Calculate treemap layout using d3-hierarchy
	const treemapLayout = $derived.by(() => {
		const currentData = treemapData;
		if (!currentData || currentData.length === 0) {
			return { data: [], x1: 'x1', y1: 'y1', x2: 'x2', y2: 'y2', maxCellSize: 0 };
		}

		const root = hierarchy({ children: currentData }).sum((d: any) => d.value || 0);
		const totalValue = root.value || 1;

		const layout = treemap().size([containerWidth, containerHeight]).padding(2);
		layout(root);

		const layoutData = root.leaves().map((d: any) => {
			const fillColor = d.data.color;
			const percentage = (d.data.value / totalValue) * 100;
			const width = d.x1 - d.x0;
			const height = d.y1 - d.y0;
			const cellSize = Math.min(width, height); // Use minimum dimension as cell size

			return {
				x1: d.x0,
				y1: d.y0,
				x2: d.x1,
				y2: d.y1,
				name: d.data.name,
				value: d.data.value,
				percentage,
				fillColor,
				cellSize
			};
		});

		// Find the maximum cell size
		const maxCellSize =
			layoutData.length > 0 ? Math.max(...layoutData.map((d: any) => d.cellSize)) : 0;

		return {
			data: layoutData,
			x1: 'x1',
			y1: 'y1',
			x2: 'x2',
			y2: 'y2',
			maxCellSize
		};
	});

	// Hit testing for interactions
	function hitTest(x: number, y: number) {
		const layoutData = treemapLayout.data;
		if (layoutData.length === 0) return null;

		// The plot's Y-axis is inverted relative to the browser's clientY
		const plotY = containerHeight - y;
		return (
			layoutData.find((n: any) => x >= n.x1 && x <= n.x2 && plotY >= n.y1 && plotY <= n.y2) || null
		);
	}

	function handlePointerMove(event: PointerEvent) {
		if (isPinned || !containerEl) return;
		const rect = containerEl.getBoundingClientRect();
		activeNode = hitTest(event.clientX - rect.left, event.clientY - rect.top);
	}

	function handleClick(event: MouseEvent) {
		if (!containerEl) return;
		const rect = containerEl.getBoundingClientRect();
		const hit = hitTest(event.clientX - rect.left, event.clientY - rect.top);

		if (isPinned && activeNode && hit && activeNode.name === hit.name) {
			isPinned = false;
			activeNode = null;
		} else if (hit) {
			isPinned = true;
			activeNode = hit;
			if (onSelect) {
				onSelect(hit.name);
			}
		} else {
			isPinned = false;
			activeNode = null;
		}
	}
</script>

<div class="w-full overflow-auto" style="height: 70vh;" bind:this={containerEl}>
	{#if data && data.length > 0 && treemapData.length > 0}
		<div class="relative min-w-0">
			<Plot
				width={containerWidth}
				height={containerHeight}
				x={{ domain: [0, containerWidth] }}
				y={{ domain: [0, containerHeight] }}
				marginLeft={0}
				marginRight={0}
				marginBottom={0}
				marginTop={0}
			>
				<Rect
					data={treemapLayout.data}
					x1={treemapLayout.x1}
					y1={treemapLayout.y1}
					x2={treemapLayout.x2}
					y2={treemapLayout.y2}
					fill={(d: any) => d.fillColor}
					stroke="#ffffff"
					strokeWidth={0.5}
				/>

				<Text
					data={treemapLayout.data}
					x={(d: any) => (d.x1 + d.x2) / 2}
					y={(d: any) => (d.y1 + d.y2) / 2}
					text={(d: any) => {
						const width = d.x2 - d.x1;
						const height = d.y2 - d.y1;
						const name = d.name;
						const value = d.value;
						const padding = isMobile ? 6 : 12;
						const availableWidth = Math.max(0, width - padding * 2);
						const availableHeight = Math.max(0, height - padding * 2);

						if (availableWidth <= 0 || availableHeight <= 0) {
							return '';
						}

						const valueText = formatCurrency(value);
						const charWidthRatio = 0.6;
						const lineHeightRatio = 1.2;
						const minFontSize = isMobile ? 3 : 6;
						const maxFontSize = isMobile ? 16 : 20;

						// Calculate cell size and scale font size relative to largest cell
						const cellSize = Math.min(width, height);
						const maxCellSize = treemapLayout.maxCellSize;

						// Scale font size relative to largest cell
						let fontSize: number;
						if (maxCellSize > 0) {
							const sizeRatio = cellSize / maxCellSize;
							fontSize = minFontSize + (maxFontSize - minFontSize) * sizeRatio;
						} else {
							fontSize = maxFontSize;
						}

						// Calculate maximum font size that fits both name and value
						// We need to fit two lines: name (possibly truncated) and value
						const valueWidthEstimate = valueText.length * charWidthRatio;
						const maxNameWidth = availableWidth;

						// Calculate font size based on height constraint (2 lines)
						const fontSizeByHeight = availableHeight / (2 * lineHeightRatio);

						// Calculate font size based on width constraint
						// We need to fit the wider of name or value
						let fontSizeByWidth: number;
						if (name.length > valueText.length) {
							// Name is longer, calculate based on name width
							fontSizeByWidth = availableWidth / (name.length * charWidthRatio);
						} else {
							// Value is longer, calculate based on value width
							fontSizeByWidth = availableWidth / (valueText.length * charWidthRatio);
						}

						// Use the smaller of the two constraints
						let maxFittingFontSize = Math.min(fontSizeByWidth, fontSizeByHeight);

						// Use the smaller of the scaled size and the fitting size
						fontSize = Math.min(fontSize, maxFittingFontSize);
						fontSize = Math.max(minFontSize, Math.min(maxFontSize, fontSize));

						// Check if both name and value fit at this size
						const charWidth = fontSize * charWidthRatio;
						const lineHeight = fontSize * lineHeightRatio;
						const nameWidth = name.length * charWidth;
						const valueWidth = valueText.length * charWidth;

						// Always try to show both name and value
						// If full name doesn't fit, truncate it
						if (
							Math.max(nameWidth, valueWidth) <= availableWidth &&
							lineHeight * 2 <= availableHeight
						) {
							return `${name}\n${valueText}`;
						}

						// Truncate name to fit with value
						// Calculate how many characters of name we can fit
						const maxCharsForName = Math.floor(availableWidth / charWidth) - 1;

						if (
							maxCharsForName >= 2 &&
							valueWidth <= availableWidth &&
							lineHeight * 2 <= availableHeight
						) {
							// We can fit at least 2 characters of name plus value
							let truncatedName: string;
							if (name.length > maxCharsForName) {
								// Truncate and add ellipsis
								const truncateTo = Math.max(1, maxCharsForName - 3); // Leave room for ellipsis
								truncatedName = name.substring(0, truncateTo) + '...';
							} else {
								truncatedName = name;
							}

							const truncatedWidth = truncatedName.length * charWidth;
							if (Math.max(truncatedWidth, valueWidth) <= availableWidth) {
								return `${truncatedName}\n${valueText}`;
							}
						}

						// If still doesn't fit, scale down font size more aggressively to fit both
						// Recalculate with smaller font size
						const requiredWidth = Math.max(name.length, valueText.length) * charWidthRatio;
						const requiredHeight = 2 * lineHeightRatio;
						const fontSizeForBoth = Math.min(
							availableWidth / requiredWidth,
							availableHeight / requiredHeight
						);

						if (fontSizeForBoth >= minFontSize) {
							// Use this smaller font size
							const smallerCharWidth = fontSizeForBoth * charWidthRatio;
							const smallerMaxChars = Math.floor(availableWidth / smallerCharWidth) - 1;

							if (smallerMaxChars >= 2) {
								let truncatedName: string;
								if (name.length > smallerMaxChars) {
									const truncateTo = Math.max(1, smallerMaxChars - 3);
									truncatedName = name.substring(0, truncateTo) + '...';
								} else {
									truncatedName = name;
								}

								// Verify value fits
								const valueWidthSmall = valueText.length * smallerCharWidth;
								const truncatedWidthSmall = truncatedName.length * smallerCharWidth;
								if (Math.max(truncatedWidthSmall, valueWidthSmall) <= availableWidth) {
									return `${truncatedName}\n${valueText}`;
								}
							}
						}

						// Absolute last resort: show abbreviated name and value
						// Only if cell is extremely small
						if (availableWidth > 0 && availableHeight > 0) {
							const minCharsForName =
								Math.floor(availableWidth / (minFontSize * charWidthRatio)) - 1;
							if (minCharsForName >= 1) {
								const abbrevName =
									name.length > minCharsForName
										? name.substring(0, Math.max(1, minCharsForName - 1)) + '...'
										: name;
								return `${abbrevName}\n${valueText}`;
							}
						}

						// Only show just value if absolutely nothing else fits
						return valueText;
					}}
					fontSize={(d: any) => {
						const width = d.x2 - d.x1;
						const height = d.y2 - d.y1;
						const padding = isMobile ? 6 : 12;
						const availableWidth = Math.max(0, width - padding * 2);
						const availableHeight = Math.max(0, height - padding * 2);

						if (availableWidth <= 0 || availableHeight <= 0) {
							return isMobile ? 3 : 6;
						}

						const name = d.name;
						const valueText = formatCurrency(d.value);
						const charWidthRatio = 0.6;
						const lineHeightRatio = 1.2;
						const minFontSize = isMobile ? 3 : 6;
						const maxFontSize = isMobile ? 16 : 20;

						// Calculate cell size (minimum dimension)
						const cellSize = Math.min(width, height);
						const maxCellSize = treemapLayout.maxCellSize;

						// Scale font size relative to largest cell
						// Largest cell gets maxFontSize, smaller cells scale down proportionally
						let fontSize: number;
						if (maxCellSize > 0) {
							const sizeRatio = cellSize / maxCellSize;
							fontSize = minFontSize + (maxFontSize - minFontSize) * sizeRatio;
						} else {
							fontSize = maxFontSize;
						}

						// Prioritize fitting both name and value
						// Calculate font size based on height constraint (2 lines)
						const fontSizeByHeight = availableHeight / (2 * lineHeightRatio);

						// Calculate font size based on width constraint
						// We need to fit the wider of name or value (name may be truncated)
						const maxTextLength = Math.max(name.length, valueText.length);
						const fontSizeByWidth = availableWidth / (maxTextLength * charWidthRatio);

						// Use the smaller of the two constraints to ensure both fit
						let maxFittingFontSize = Math.min(fontSizeByWidth, fontSizeByHeight);

						// Use the smaller of the scaled size and the fitting size
						fontSize = Math.min(fontSize, maxFittingFontSize);
						fontSize = Math.max(minFontSize, Math.min(maxFontSize, fontSize));

						// Verify both name and value fit at this size
						const charWidth = fontSize * charWidthRatio;
						const lineHeight = fontSize * lineHeightRatio;
						const nameWidth = name.length * charWidth;
						const valueWidth = valueText.length * charWidth;

						// If both don't fit, scale down more aggressively
						if (
							Math.max(nameWidth, valueWidth) > availableWidth ||
							lineHeight * 2 > availableHeight
						) {
							// Recalculate with more aggressive scaling to fit both
							const requiredWidth = Math.max(name.length, valueText.length) * charWidthRatio;
							const requiredHeight = 2 * lineHeightRatio;
							const fontSizeForBoth = Math.min(
								availableWidth / requiredWidth,
								availableHeight / requiredHeight
							);
							fontSize = Math.max(minFontSize, Math.min(maxFontSize, fontSizeForBoth));
						}

						return fontSize;
					}}
					fontWeight="bold"
					strokeWidth={2}
					strokeLinejoin="round"
					stroke="rgba(255,255,255,0.8)"
					fill="#000000"
					filter={(d: any) => {
						// Show text for all cells that have any space
						const width = d.x2 - d.x1;
						const height = d.y2 - d.y1;
						const padding = isMobile ? 6 : 12;
						const availableWidth = width - padding * 2;
						const availableHeight = height - padding * 2;

						// Only filter out cells that are truly too small to show anything
						return availableWidth > 0 && availableHeight > 0;
					}}
				/>
			</Plot>

			<!-- Invisible overlay for interactions -->
			<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
			<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
			<div
				class="absolute inset-0 cursor-pointer touch-manipulation"
				onpointermove={handlePointerMove}
				onpointerleave={() => !isPinned && (activeNode = null)}
				onclick={handleClick}
				onkeydown={(e) => {
					if (e.key === 'Escape') {
						isPinned = false;
						activeNode = null;
					}
				}}
				role="figure"
				tabindex="0"
			></div>
		</div>
	{:else}
		<div class="flex h-96 items-center justify-center text-muted-foreground">No data available</div>
	{/if}
</div>
