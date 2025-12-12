const routes = {
    '/campaigns': { page: 'pages/campaigns.html', service: 'campaigns' },
    '/segments': { page: 'pages/segments.html', service: 'segments' },
    '/health': { page: 'pages/health.html', service: 'health' },
};

export async function initRouter() {
    window.addEventListener('hashchange', handleRoute);
    
    // Default route
    if (!window.location.hash) {
        window.location.hash = '#/campaigns';
    } else {
        handleRoute();
    }
}

async function handleRoute() {
    const hash = window.location.hash.slice(1) || '/campaigns';
    const route = routes[hash];
    
    if (!route) {
        document.getElementById('page-content').innerHTML = '<p>Page not found</p>';
        return;
    }
    
    // Update active nav
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.getAttribute('href') === `#${hash}`);
    });
    
    // Load page HTML
    const response = await fetch(route.page);
    const html = await response.text();
    document.getElementById('page-content').innerHTML = html;
    
    // Load and init service
    const service = await import(`../services/${route.service}.js`);
    if (service.init) {
        await service.init();
    }
}