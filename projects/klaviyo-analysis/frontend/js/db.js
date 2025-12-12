import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

let db = null;
let conn = null;

export async function initDB() {
    if (db) return;
    
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
    
    const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    conn = await db.connect();
    
    console.log('DuckDB initialized');
}

export async function loadParquet(name, path) {
    const fullPath = new URL(path, window.location.href).href;
    await db.registerFileURL(name, fullPath, duckdb.DuckDBDataProtocol.HTTP, false);
    await conn.query(`CREATE TABLE IF NOT EXISTS ${name} AS SELECT * FROM parquet_scan('${name}')`);
    console.log(`Loaded ${name}`);
}

export async function query(sql) {
    const result = await conn.query(sql);
    return result.toArray().map(row => row.toJSON());
}