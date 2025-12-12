import { query } from '../js/db.js';

let currentSort = { column: 'send_time', dir: 'DESC' };
let minSends = 0;

export async function init() {
    await populateSegments();
    await loadCampaigns();
    setupFilters();
}

async function populateSegments() {
    const segments = await query(`SELECT DISTINCT list FROM campaigns ORDER BY list`);
    const select = document.getElementById('segment-filter');
    
    segments.forEach(s => {
        const option = document.createElement('option');
        option.value = s.list;
        option.textContent = s.list;
        select.appendChild(option);
    });
}

async function loadCampaigns() {
    const startDate = document.getElementById('date-start')?.value;
    const endDate = document.getElementById('date-end')?.value;
    const segment = document.getElementById('segment-filter')?.value;
    
    let where = [];
    if (startDate) where.push(`send_time >= '${startDate}'`);
    if (endDate) where.push(`send_time <= '${endDate}'`);
    if (segment) where.push(`list = '${segment}'`);
    if (minSends > 0) where.push(`total_recipients >= ${minSends}`);
    
    const whereClause = where.length ? `WHERE ${where.join(' AND ')}` : '';
    
    const campaigns = await query(`
        SELECT 
            campaign_name,
            subject,
            list,
            send_time,
            total_recipients,
            unique_opens,
            open_rate,
            unique_clicks,
            click_rate,
            revenue,
            unsubscribes,
            spam_complaints_rate
        FROM campaigns
        ${whereClause}
        ORDER BY ${currentSort.column} ${currentSort.dir}
        LIMIT 50
    `);
    
    renderTable(campaigns);
}

function renderTable(data) {
    const container = document.getElementById('campaigns-table');
    
    if (!data.length) {
        container.innerHTML = '<p>No campaigns found</p>';
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
    await loadCampaigns();
}

function formatHeader(key) {
    return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function formatCell(key, value) {
    if (value === null || value === undefined) return '—';
    if (key.includes('rate')) return (value * 100).toFixed(2) + '%';
    if (key === 'revenue') return '$' + value.toFixed(2);
    if (key === 'send_time') return new Date(value).toLocaleDateString();
    return value;
}

function setupFilters() {
    document.getElementById('apply-filters')?.addEventListener('click', loadCampaigns);
    document.getElementById('min-sends')?.addEventListener('change', (e) => {
        minSends = parseInt(e.target.value) || 0;
        loadCampaigns();
    });
}