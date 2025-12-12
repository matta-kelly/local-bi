import { initDB, loadParquet } from './db.js';
import { initRouter } from './router.js';

async function init() {
    console.log('Initializing app...');
    
    // Init DuckDB
    await initDB();
    
    // Load data
    await loadParquet('campaigns', 'data/campaigns.parquet');
    await loadParquet('f_segments', 'data/f_segments.parquet');
    
    // Init router
    await initRouter();
    
    console.log('App ready');
}

init().catch(console.error);