import * as duckdb from '@duckdb/duckdb-wasm';

let db: duckdb.AsyncDuckDB | null = null;
let connection: Awaited<ReturnType<typeof duckdb.AsyncDuckDB.prototype.connect>> | null = null;

export interface TradeData {
	Country: string;
	Commodity: string;
	Port: string;
	Type: string;
	'Total USD Value': number;
}

export async function initDuckDB(): Promise<void> {
	if (db && connection) {
		duckDBInitialized = true;
		return;
	}

	if (duckDBInitializing) {
		// Wait for existing initialization to complete
		while (duckDBInitializing && !duckDBInitialized) {
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
		return;
	}

	duckDBInitializing = true;

	try {
		// Get bundles from jsDelivr CDN
		const bundles = duckdb.getJsDelivrBundles();
		const bundle = await duckdb.selectBundle(bundles);

		// Fetch the worker file to avoid CORS issues
		// Create a blob URL from the fetched content
		const workerResponse = await fetch(bundle.mainWorker!);
		if (!workerResponse.ok) {
			throw new Error(`Failed to fetch worker: ${workerResponse.statusText}`);
		}
		const workerBlob = await workerResponse.blob();
		const workerUrl = URL.createObjectURL(workerBlob);

		const worker = new Worker(workerUrl, {
			type: 'module'
		});

		// Fetch the main module and pthread worker as well
		const mainModuleResponse = await fetch(bundle.mainModule!);
		if (!mainModuleResponse.ok) {
			throw new Error(`Failed to fetch WASM module: ${mainModuleResponse.statusText}`);
		}
		const mainModuleBlob = await mainModuleResponse.blob();
		const mainModuleUrl = URL.createObjectURL(mainModuleBlob);

		let pthreadWorkerUrl: string | undefined;
		if (bundle.pthreadWorker) {
			const pthreadWorkerResponse = await fetch(bundle.pthreadWorker);
			if (!pthreadWorkerResponse.ok) {
				throw new Error(`Failed to fetch pthread worker: ${pthreadWorkerResponse.statusText}`);
			}
			const pthreadWorkerBlob = await pthreadWorkerResponse.blob();
			pthreadWorkerUrl = URL.createObjectURL(pthreadWorkerBlob);
		}

		db = new duckdb.AsyncDuckDB(console, worker);
		await db.instantiate(mainModuleUrl, pthreadWorkerUrl);

		connection = await db.connect();
		duckDBInitialized = true;

		// Start loading base Parquet in background (non-blocking)
		loadBaseParquetInBackground();
	} finally {
		duckDBInitializing = false;
	}
}

const loadedTables = new Set<string>();
let baseParquetLoaded = false;
let duckDBInitializing = false;
let duckDBInitialized = false;

// Check if DuckDB is loaded
export function isDuckDBLoaded(): boolean {
	return duckDBInitialized && db !== null && connection !== null;
}

// Check if DuckDB is currently initializing
export function isDuckDBInitializing(): boolean {
	return duckDBInitializing;
}

// Check if DuckDB is required for a query
export function requiresDuckDB(
	filter?: { commodity?: string; country?: string; port?: string },
	useOptimizedFile: boolean = true,
	year?: number
): boolean {
	const hasFilter = filter && Object.keys(filter).length > 0;
	// DuckDB is required if:
	// 1. There are filters (can't use JSON files)
	// 2. Year is not provided (can't use JSON files)
	// 3. Optimized file is disabled
	return hasFilter || year === undefined || !useOptimizedFile;
}

// Load base Parquet file in background for filtered queries (non-blocking)
export function loadBaseParquetInBackground(): void {
	if (baseParquetLoaded) return;

	// Load in background without blocking - don't await
	initDuckDB()
		.then(async () => {
			if (!connection) return;

			if (loadedTables.has('trade_data')) {
				baseParquetLoaded = true;
				return;
			}

			const baseUrl = new URL('/data/export-import-aggregated.parquet', window.location.origin)
				.href;
			const escapedUrl = baseUrl.replace(/'/g, "''");
			try {
				await connection.query(`
				CREATE OR REPLACE TABLE trade_data AS 
				SELECT * FROM read_parquet('${escapedUrl}')
			`);
				loadedTables.add('trade_data');
				baseParquetLoaded = true;
			} catch (e) {
				console.warn('Failed to load base Parquet in background:', e);
			}
		})
		.catch((e) => {
			// Silently fail - DuckDB will be initialized when needed
			console.warn('DuckDB initialization in background failed:', e);
		});
}

export async function loadParquetFile(
	url: string,
	tableName: string = 'trade_data'
): Promise<void> {
	if (!connection) {
		await initDuckDB();
		if (!connection) throw new Error('Failed to initialize DuckDB connection');
	}

	// Only load if not already loaded
	if (loadedTables.has(tableName)) {
		return;
	}

	// Register the Parquet file as a table
	// Note: URL is from static assets, so it's safe
	const escapedUrl = url.replace(/'/g, "''");
	await connection.query(`
		CREATE OR REPLACE TABLE ${tableName} AS 
		SELECT * FROM read_parquet('${escapedUrl}')
	`);

	loadedTables.add(tableName);
}

export function getJsonFileForMode(mode: 'Commodity' | 'Country' | 'Port', year: number): string {
	return `/data/${mode.toLowerCase()}-aggregated-${year}.json`;
}

interface JsonRow {
	Commodity?: string;
	Country?: string;
	Port?: string;
	Type: string;
	'Total USD Value': number;
}

export async function queryData(
	mode: 'Commodity' | 'Country' | 'Port',
	tradeMode: 'All' | 'Import' | 'Export',
	filter?: { commodity?: string; country?: string; port?: string },
	limit?: number,
	useOptimizedFile: boolean = true,
	year?: number
): Promise<TradeData[]> {
	// Check if there are any active filters
	const hasFilter = filter && Object.keys(filter).length > 0;

	// Load base Parquet in background for future filtered queries
	loadBaseParquetInBackground();

	// Use JSON files when no filters and year is provided (fastest path)
	if (useOptimizedFile && !hasFilter && year !== undefined) {
		try {
			const jsonUrl = getJsonFileForMode(mode, year);
			const response = await fetch(jsonUrl);
			if (!response.ok) {
				throw new Error(`Failed to load JSON file: ${jsonUrl}`);
			}
			const jsonData: JsonRow[] = await response.json();

			// Filter by trade mode
			let filtered = jsonData;
			if (tradeMode !== 'All') {
				filtered = jsonData.filter((row) => row.Type === tradeMode);
			}

			// Group by mode column and sum values when tradeMode is 'All'
			const groupByColumn =
				mode === 'Commodity' ? 'Commodity' : mode === 'Country' ? 'Country' : 'Port';
			const grouped = new Map<string, number>();

			for (const row of filtered) {
				const key = row[groupByColumn] || '';
				const value = row['Total USD Value'] || 0;
				grouped.set(key, (grouped.get(key) || 0) + value);
			}

			// Convert to array and sort
			let result = Array.from(grouped.entries())
				.map(([label, value]) => {
					const item: TradeData = {
						Country: '',
						Commodity: '',
						Port: '',
						Type: tradeMode === 'All' ? '' : tradeMode,
						'Total USD Value': value
					};
					item[mode] = label;
					return item;
				})
				.sort((a, b) => b['Total USD Value'] - a['Total USD Value']);

			// Apply limit if specified
			if (limit !== undefined) {
				result = result.slice(0, limit);
			}

			return result;
		} catch (e) {
			console.warn('Failed to load JSON, falling back to Parquet:', e);
			// Fall through to Parquet loading
		}
	}

	// Use Parquet for filtered queries or when JSON fails
	if (!connection) {
		await initDuckDB();
		if (!connection) throw new Error('Failed to initialize DuckDB connection');
	}

	let tableName = 'trade_data';
	if (!hasFilter && year === undefined) {
		// This shouldn't happen in normal flow, but handle it
		throw new Error('Year must be provided for optimized queries');
	} else {
		// Use base aggregated file for filtered queries
		const baseUrl = new URL('/data/export-import-aggregated.parquet', window.location.origin).href;
		await loadParquetFile(baseUrl, 'trade_data');
		tableName = 'trade_data';
	}

	const conditions: string[] = [];

	// Add year condition if using base file
	if (year !== undefined) {
		conditions.push(`Year = ${year}`);
	}

	if (tradeMode !== 'All') {
		const typeValue = tradeMode === 'Import' ? 'Import' : 'Export';
		conditions.push(`Type = '${typeValue.replace(/'/g, "''")}'`);
	}

	if (filter) {
		if (filter.commodity) {
			conditions.push(`Commodity = '${filter.commodity.replace(/'/g, "''")}'`);
		}
		if (filter.country) {
			conditions.push(`Country = '${filter.country.replace(/'/g, "''")}'`);
		}
		if (filter.port) {
			conditions.push(`Port = '${filter.port.replace(/'/g, "''")}'`);
		}
	}

	const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
	const limitClause = limit ? `LIMIT ${limit}` : '';

	// Safe column name - mode is controlled and can only be one of the three values
	const groupByColumn =
		mode === 'Commodity' ? 'Commodity' : mode === 'Country' ? 'Country' : 'Port';

	// Aggregate from base file with GROUP BY
	const query = `
		SELECT 
			${groupByColumn} as label,
			SUM("Total USD Value") as value
		FROM ${tableName}
		${whereClause}
		GROUP BY ${groupByColumn}
		ORDER BY value DESC
		${limitClause}
	`;

	const result = await connection.query(query);
	const rows = result.toArray();
	const data: TradeData[] = rows.map((row: any) => {
		const item: TradeData = {
			Country: '',
			Commodity: '',
			Port: '',
			Type: tradeMode === 'All' ? '' : tradeMode,
			'Total USD Value': Number(row.value)
		};
		item[mode] = String(row.label);
		return item;
	});

	return data;
}

export async function queryTableData(
	mode: 'Commodity' | 'Country' | 'Port',
	tradeMode: 'All' | 'Import' | 'Export',
	filter?: { commodity?: string; country?: string; port?: string },
	limit?: number,
	offset: number = 0,
	useOptimizedFile: boolean = true,
	year?: number
): Promise<TradeData[]> {
	// Check if there are any active filters
	const hasFilter = filter && Object.keys(filter).length > 0;

	// Load base Parquet in background for future filtered queries
	loadBaseParquetInBackground();

	// Use JSON files when no filters and year is provided (fastest path)
	if (useOptimizedFile && !hasFilter && year !== undefined) {
		try {
			const jsonUrl = getJsonFileForMode(mode, year);
			const response = await fetch(jsonUrl);
			if (!response.ok) {
				throw new Error(`Failed to load JSON file: ${jsonUrl}`);
			}
			const jsonData: JsonRow[] = await response.json();

			// Filter by trade mode
			let filtered = jsonData;
			if (tradeMode !== 'All') {
				filtered = jsonData.filter((row) => row.Type === tradeMode);
			}

			// Group by mode column and sum values when tradeMode is 'All'
			const groupByColumn =
				mode === 'Commodity' ? 'Commodity' : mode === 'Country' ? 'Country' : 'Port';
			const grouped = new Map<string, number>();

			for (const row of filtered) {
				const key = row[groupByColumn] || '';
				const value = row['Total USD Value'] || 0;
				grouped.set(key, (grouped.get(key) || 0) + value);
			}

			// Convert to array and sort
			let result = Array.from(grouped.entries())
				.map(([label, value]) => {
					const item: TradeData = {
						Country: '',
						Commodity: '',
						Port: '',
						Type: tradeMode === 'All' ? '' : tradeMode,
						'Total USD Value': value
					};
					item[mode] = label;
					return item;
				})
				.sort((a, b) => b['Total USD Value'] - a['Total USD Value']);

			// Apply pagination (offset and limit)
			if (offset > 0 || limit !== undefined) {
				const start = offset;
				const end = limit !== undefined ? start + limit : undefined;
				result = result.slice(start, end);
			}

			return result;
		} catch (e) {
			console.warn('Failed to load JSON, falling back to Parquet:', e);
			// Fall through to Parquet loading
		}
	}

	// Use Parquet for filtered queries or when JSON fails
	if (!connection) {
		await initDuckDB();
		if (!connection) throw new Error('Failed to initialize DuckDB connection');
	}

	let tableName = 'trade_data';
	if (!hasFilter && year === undefined) {
		// This shouldn't happen in normal flow, but handle it
		throw new Error('Year must be provided for optimized queries');
	} else {
		// Use base aggregated file for filtered queries
		const baseUrl = new URL('/data/export-import-aggregated.parquet', window.location.origin).href;
		await loadParquetFile(baseUrl, 'trade_data');
		tableName = 'trade_data';
	}

	const conditions: string[] = [];

	// Add year condition if using base file
	if (year !== undefined) {
		conditions.push(`Year = ${year}`);
	}

	if (tradeMode !== 'All') {
		const typeValue = tradeMode === 'Import' ? 'Import' : 'Export';
		conditions.push(`Type = '${typeValue.replace(/'/g, "''")}'`);
	}

	if (filter) {
		if (filter.commodity) {
			conditions.push(`Commodity = '${filter.commodity.replace(/'/g, "''")}'`);
		}
		if (filter.country) {
			conditions.push(`Country = '${filter.country.replace(/'/g, "''")}'`);
		}
		if (filter.port) {
			conditions.push(`Port = '${filter.port.replace(/'/g, "''")}'`);
		}
	}

	const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
	const limitClause = limit ? `LIMIT ${limit} OFFSET ${offset}` : '';

	// Safe column name - mode is controlled and can only be one of the three values
	const groupByColumn =
		mode === 'Commodity' ? 'Commodity' : mode === 'Country' ? 'Country' : 'Port';

	// Aggregate from base file with GROUP BY
	const query = `
		SELECT 
			${groupByColumn} as label,
			SUM("Total USD Value") as value
		FROM ${tableName}
		${whereClause}
		GROUP BY ${groupByColumn}
		ORDER BY value DESC
		${limitClause}
	`;

	const result = await connection.query(query);
	const rows = result.toArray();
	const data: TradeData[] = rows.map((row: any) => {
		const item: TradeData = {
			Country: '',
			Commodity: '',
			Port: '',
			Type: tradeMode === 'All' ? '' : tradeMode,
			'Total USD Value': Number(row.value)
		};
		item[mode] = String(row.label);
		return item;
	});

	return data;
}
