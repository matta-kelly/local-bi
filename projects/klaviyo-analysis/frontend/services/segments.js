import { query } from '../js/db.js';

let currentSort = { column: 'total_customers', dir: 'DESC' };
let minSends = 0;

export async function init() {
    await loadSegments();
    setupFilters();
}

async function loadSegments() {
    const segments = await query(`
        SELECT 
            segment_name,
            -- Campaign stats
            total_campaigns,
            total_sends,
            avg_open_rate,
            avg_click_rate,
            total_campaign_revenue,
            total_unsubscribes,
            avg_spam_rate,
            avg_bounce_rate,
            -- Purchase stats
            total_customers,
            total_orders,
            total_order_revenue,
            avg_order_value,
            -- Send time distribution (holiday)
            holiday_pct_morning,
            holiday_pct_afternoon,
            holiday_pct_evening,
            holiday_pct_night,
            -- Send time distribution (workday)
            workday_pct_morning,
            workday_pct_afternoon,
            workday_pct_evening,
            workday_pct_night
        FROM f_segments
        WHERE total_sends >= ${minSends}
        ORDER BY ${currentSort.column} ${currentSort.dir}
    `);
    
    renderTable(segments);
}

function renderTable(data) {
    const container = document.getElementById('segments-table');
    
    if (!data.length) {
        container.innerHTML = '<p>No segments found</p>';
        return;
    }
    
    const headers = Object.keys(data[0]);
    
    const html = `
        <table>
            <thead>
                <tr>
                    ${headers.map(h => `
                        <th class="sortable" data-column="${h}">
                            ${formatHeader(h)}
                            ${currentSort.column === h ? (currentSort.dir === 'ASC' ? ' ▲' : ' ▼') : ''}
                        </th>
                    `).join('')}
                </tr>
            </thead>
            <tbody>
                ${data.map(row => `
                    <tr>
                        ${headers.map(h => `<td>${formatCell(h, row[h])}</td>`).join('')}
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;
    
    container.innerHTML = html;
    
    container.querySelectorAll('.sortable').forEach(th => {
        th.addEventListener('click', () => handleSort(th.dataset.column));
    });
}

async function handleSort(column) {
    if (currentSort.column === column) {
        currentSort.dir = currentSort.dir === 'ASC' ? 'DESC' : 'ASC';
    } else {
        currentSort.column = column;
        currentSort.dir = 'DESC';
    }
    await loadSegments();
}

function formatHeader(key) {
    return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function formatCell(key, value) {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'bigint') value = Number(value);
    if (typeof value === 'number') {
        // Rates and percentages
        if (key.includes('rate') || key.includes('pct_')) {
            return (value * 100).toFixed(1) + '%';
        }
        // Revenue/money
        if (key.includes('revenue') || key === 'avg_order_value') {
            return '$' + value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
        // Counts
        if (key.includes('total_') || key.includes('_sends') || key.includes('_orders') || key.includes('_customers') || key.includes('_unsubscribes')) {
            return Math.round(value).toLocaleString();
        }
        return value.toLocaleString();
    }
    return value;
}

function setupFilters() {
    document.getElementById('min-sends')?.addEventListener('change', (e) => {
        minSends = parseInt(e.target.value) || 0;
        loadSegments();
    });
}