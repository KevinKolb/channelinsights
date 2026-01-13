let currentView = 'states';
let currentData = null;

const entityTypes = ['distributors', 'buying_groups', 'dealers', 'pos_providers', 'incentive_platforms', 'sales_agencies', 'integrators', 'service_providers', 'ecommerce_platforms'];

const entityDefinitions = {
    'distributors': 'Wholesale companies that purchase appliances from manufacturers and sell to dealers and retailers.',
    'buying_groups': 'Cooperative organizations that pool purchasing power to negotiate better terms with suppliers.',
    'dealers': 'Retail stores and outlets that sell appliances directly to consumers.',
    'pos_providers': 'Point-of-Sale system providers that offer transaction and inventory management solutions.',
    'incentive_platforms': 'Software platforms that manage rebates, rewards, and promotional incentive programs.',
    'sales_agencies': 'Independent sales representatives and agencies that represent manufacturers to retailers on commission.',
    'integrators': 'System integrators and solution providers that design, implement, and manage complex technology solutions.',
    'service_providers': 'Field service organizations that install, maintain, and repair appliances and equipment.',
    'ecommerce_platforms': 'Online marketplace and eCommerce platforms that enable digital sales channels for products.'
};

async function loadData(view) {
    const filename = `${view}.json`;

    try {
        const response = await fetch(`data/${filename}`);
        const data = await response.json();
        currentData = data;
        displayMetadata(data.metadata);
        applyFilters();
    } catch (error) {
        document.getElementById('content').innerHTML =
            '<div class="empty-state">Error loading data</div>';
        console.error('Error loading data:', error);
    }
}

function displayMetadata(metadata) {
    const metadataDiv = document.getElementById('metadata');
    const datePulled = new Date(metadata.date_pulled).toLocaleDateString();

    if (currentView === 'states') {
        const source = metadata.source === 'hardcoded' ? 'Hardcoded Fallback' : metadata.source;
        metadataDiv.innerHTML = `Data Source: ${source} | Last Updated: ${datePulled}`;
    } else if (currentView === 'manufacturers') {
        const source = metadata.source === 'hardcoded' ? 'Hardcoded Fallback' : metadata.source;
        metadataDiv.innerHTML = `Data Source: ${source} | Last Updated: ${datePulled} | Total: ${metadata.total_manufacturers}`;
    } else if (entityTypes.includes(currentView)) {
        const source = metadata.source === 'hardcoded' ? 'Hardcoded Fallback' : metadata.source;
        let metadataText = `Data Source: ${source} | Last Updated: ${datePulled} | Total: ${metadata.total_count}`;

        if (entityDefinitions[currentView]) {
            metadataText += `<br><span class="entity-definition">${entityDefinitions[currentView]}</span>`;
        }

        metadataDiv.innerHTML = metadataText;
    }
}

function switchView(view) {
    currentView = view;

    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[data-view="${view}"]`).classList.add('active');

    loadData(view);
}

function applyFilters() {
    if (!currentData) return;

    const searchTerm = document.getElementById('search').value.toLowerCase();
    const countryFilter = document.getElementById('country-filter').value;
    const sortOption = document.getElementById('sort').value;

    if (currentView === 'states') {
        // States view keeps country sectioning
        let filteredData = JSON.parse(JSON.stringify(currentData['North America']));

        if (countryFilter !== 'all') {
            const filtered = {};
            filtered[countryFilter] = filteredData[countryFilter];
            filteredData = filtered;
        }

        if (searchTerm) {
            Object.keys(filteredData).forEach(country => {
                const countryData = filteredData[country];

                if (countryData.states) {
                    countryData.states = countryData.states.filter(state =>
                        state.toLowerCase().includes(searchTerm)
                    );
                }
                if (countryData.provinces) {
                    countryData.provinces = countryData.provinces.filter(province =>
                        province.toLowerCase().includes(searchTerm)
                    );
                }
                if (countryData.territories) {
                    countryData.territories = countryData.territories.filter(territory =>
                        territory.toLowerCase().includes(searchTerm)
                    );
                }
            });
        }

        sortData(filteredData, sortOption);
        render(filteredData);
    } else if (currentView === 'manufacturers') {
        renderFlatList(searchTerm, countryFilter, sortOption, 'manufacturers');
    } else if (entityTypes.includes(currentView)) {
        renderFlatList(searchTerm, countryFilter, sortOption, 'entities');
    }
}

function sortData(data, sortOption) {
    Object.keys(data).forEach(country => {
        const countryData = data[country];

        if (currentView === 'states') {
            ['states', 'provinces', 'territories'].forEach(key => {
                if (countryData[key]) {
                    countryData[key] = applySortOption(countryData[key], sortOption);
                }
            });
        } else {
            if (countryData.manufacturers) {
                countryData.manufacturers = applySortOption(
                    countryData.manufacturers,
                    sortOption,
                    item => item.name
                );
            }
        }
    });
}

function applySortOption(array, sortOption, keyFn = x => x) {
    const sorted = [...array];

    switch(sortOption) {
        case 'alphabetical':
            return sorted.sort((a, b) => keyFn(a).localeCompare(keyFn(b)));
        case 'reverse':
            return sorted.sort((a, b) => keyFn(b).localeCompare(keyFn(a)));
        case 'length':
            return sorted.sort((a, b) => keyFn(a).length - keyFn(b).length);
        default:
            return sorted;
    }
}

function render(data) {
    const content = document.getElementById('content');

    if (Object.keys(data).length === 0) {
        content.innerHTML = '<div class="empty-state">No results found</div>';
        return;
    }

    let html = '';

    Object.entries(data).forEach(([country, countryData]) => {
        if (currentView === 'states') {
            const hasStates = countryData.states && countryData.states.length > 0;
            const hasProvinces = countryData.provinces && countryData.provinces.length > 0;
            const hasTerritories = countryData.territories && countryData.territories.length > 0;

            if (!hasStates && !hasProvinces && !hasTerritories) return;

            const totalCount = (countryData.states?.length || 0) +
                             (countryData.provinces?.length || 0) +
                             (countryData.territories?.length || 0);

            html += `
                <div class="country-section">
                    <div class="country-header">
                        <h2 class="country-name">${country}</h2>
                        <span class="count">${totalCount}</span>
                    </div>`;

            if (hasStates) {
                html += `
                    <div class="division-type">States</div>
                    <div class="states-grid">
                        ${countryData.states.map(state =>
                            `<div class="state-item">${state}</div>`
                        ).join('')}
                    </div>`;
            }

            if (hasProvinces) {
                html += `
                    <div class="division-type">Provinces</div>
                    <div class="states-grid">
                        ${countryData.provinces.map(province =>
                            `<div class="state-item">${province}</div>`
                        ).join('')}
                    </div>`;
            }

            if (hasTerritories) {
                html += `
                    <div class="division-type">Territories</div>
                    <div class="states-grid">
                        ${countryData.territories.map(territory =>
                            `<div class="state-item">${territory}</div>`
                        ).join('')}
                    </div>`;
            }

            html += `</div>`;

        } else {
            const hasManufacturers = countryData.manufacturers && countryData.manufacturers.length > 0;

            if (!hasManufacturers) return;

            html += `
                <div class="country-section">
                    <div class="country-header">
                        <h2 class="country-name">${country}</h2>
                        <span class="count">${countryData.manufacturers.length}</span>
                    </div>
                    <div class="states-grid">
                        ${countryData.manufacturers.map(mfr => `
                            <div class="state-item manufacturer-item">
                                ${mfr.website ?
                                    `<a href="${mfr.website}" target="_blank" class="manufacturer-link">${mfr.name}</a>` :
                                    `<span class="manufacturer-link">${mfr.name}</span>`
                                }
                            </div>
                        `).join('')}
                    </div>
                </div>`;
        }
    });

    content.innerHTML = html || '<div class="empty-state">No results found</div>';
}

function renderFlatList(searchTerm, countryFilter, sortOption, dataType) {
    const content = document.getElementById('content');
    const northAmerica = currentData['North America'];

    let countries = countryFilter === 'all'
        ? Object.keys(northAmerica)
        : [countryFilter];

    // Collect all items across selected countries
    let allItems = [];

    countries.forEach(country => {
        let items = [];

        if (dataType === 'manufacturers') {
            items = northAmerica[country]?.manufacturers || [];
        } else if (dataType === 'entities') {
            items = northAmerica[country]?.entities || [];
        }

        // Add country info to each item for filtering/display
        items.forEach(item => {
            allItems.push({...item, country: country});
        });
    });

    // Filter by search term
    if (searchTerm) {
        allItems = allItems.filter(item =>
            item.name.toLowerCase().includes(searchTerm)
        );
    }

    // Sort
    allItems = applySortOption(allItems, sortOption, item => item.name);

    // Render flat grid
    if (allItems.length === 0) {
        content.innerHTML = '<div class="empty-state">No results found</div>';
        return;
    }

    const html = `
        <div class="states-grid">
            ${allItems.map(item => `
                <div class="state-item manufacturer-item">
                    ${item.website ?
                        `<a href="${item.website}" target="_blank" class="manufacturer-link">${item.name}</a>` :
                        `<span class="manufacturer-link">${item.name}</span>`
                    }
                </div>
            `).join('')}
        </div>`;

    content.innerHTML = html;
}

function getCurrentFilteredData() {
    if (!currentData) return [];

    const searchTerm = document.getElementById('search').value.toLowerCase();
    const countryFilter = document.getElementById('country-filter').value;
    const sortOption = document.getElementById('sort').value;

    let rows = [];

    if (currentView === 'states') {
        const northAmerica = currentData['North America'];
        const countries = countryFilter === 'all' ? Object.keys(northAmerica) : [countryFilter];

        countries.forEach(country => {
            const countryData = northAmerica[country];

            if (countryData.states) {
                countryData.states.forEach(state => {
                    if (!searchTerm || state.toLowerCase().includes(searchTerm)) {
                        rows.push({ Country: country, Type: 'State', Name: state });
                    }
                });
            }
            if (countryData.provinces) {
                countryData.provinces.forEach(province => {
                    if (!searchTerm || province.toLowerCase().includes(searchTerm)) {
                        rows.push({ Country: country, Type: 'Province', Name: province });
                    }
                });
            }
            if (countryData.territories) {
                countryData.territories.forEach(territory => {
                    if (!searchTerm || territory.toLowerCase().includes(searchTerm)) {
                        rows.push({ Country: country, Type: 'Territory', Name: territory });
                    }
                });
            }
        });
    } else if (currentView === 'manufacturers') {
        const northAmerica = currentData['North America'];
        const countries = countryFilter === 'all' ? Object.keys(northAmerica) : [countryFilter];

        countries.forEach(country => {
            const manufacturers = northAmerica[country]?.manufacturers || [];
            manufacturers.forEach(mfr => {
                if (!searchTerm || mfr.name.toLowerCase().includes(searchTerm)) {
                    rows.push({
                        Country: country,
                        Name: mfr.name,
                        Website: mfr.website || ''
                    });
                }
            });
        });
    } else if (entityTypes.includes(currentView)) {
        const northAmerica = currentData['North America'];
        const countries = countryFilter === 'all' ? Object.keys(northAmerica) : [countryFilter];

        countries.forEach(country => {
            const countryEntities = northAmerica[country]?.entities || [];
            countryEntities.forEach(entity => {
                if (!searchTerm || entity.name.toLowerCase().includes(searchTerm)) {
                    rows.push({
                        Country: country,
                        Name: entity.name,
                        Website: entity.website || ''
                    });
                }
            });
        });
    }

    return rows;
}

function downloadCSV() {
    const data = getCurrentFilteredData();
    if (data.length === 0) {
        alert('No data to download');
        return;
    }

    const headers = Object.keys(data[0]);
    const csv = [
        headers.join(','),
        ...data.map(row => headers.map(h => {
            const value = row[h] || '';
            return `"${value.toString().replace(/"/g, '""')}"`;
        }).join(','))
    ].join('\n');

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `channel_insights_${currentView}_${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
}

function downloadJSON() {
    const data = getCurrentFilteredData();
    if (data.length === 0) {
        alert('No data to download');
        return;
    }

    const json = JSON.stringify(data, null, 2);
    const blob = new Blob([json], { type: 'application/json;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `channel_insights_${currentView}_${new Date().toISOString().split('T')[0]}.json`;
    link.click();
}

function downloadExcel() {
    const data = getCurrentFilteredData();
    if (data.length === 0) {
        alert('No data to download');
        return;
    }

    const ws = XLSX.utils.json_to_sheet(data);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, currentView.charAt(0).toUpperCase() + currentView.slice(1));
    XLSX.writeFile(wb, `channel_insights_${currentView}_${new Date().toISOString().split('T')[0]}.xlsx`);
}

document.querySelectorAll('.tab-button').forEach(button => {
    button.addEventListener('click', () => {
        switchView(button.dataset.view);
    });
});

document.getElementById('search').addEventListener('input', applyFilters);
document.getElementById('country-filter').addEventListener('change', applyFilters);
document.getElementById('sort').addEventListener('change', applyFilters);

document.querySelectorAll('.download-btn').forEach(button => {
    button.addEventListener('click', () => {
        const format = button.dataset.format;
        if (format === 'csv') {
            downloadCSV();
        } else if (format === 'json') {
            downloadJSON();
        } else if (format === 'excel') {
            downloadExcel();
        }
    });
});

loadData('states');
