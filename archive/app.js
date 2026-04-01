let currentCategory = 'all';
let currentData = null;
let posData = null;
let currentPosSheet = null;

const categoryDescriptions = {
    'all': 'All channel partners across the appliance industry value chain.',
    'Makers': 'Manufacturers who own brands and/or produce appliances - vertically integrated, brand owners, OEM, ODM.',
    'Middle': 'Intermediaries in the supply chain - distributors, buying groups, rep firms, logistics providers, incentive platforms.',
    'Sellers': 'Organizations that sell to end consumers - retailers, dealers, franchisees.',
    'Projects': 'Project-based partners - builders, contractors, installers, service providers.',
    'People': 'Individual contributors - sales associates, influencers, affiliates.'
};

async function loadData() {
    try {
        const response = await fetch('data/partners.json');
        const data = await response.json();
        currentData = data;
        displayMetadata();
        applyFilters();
    } catch (error) {
        document.getElementById('content').innerHTML =
            '<div class="empty-state">Error loading data</div>';
        console.error('Error loading data:', error);
    }
}

function displayMetadata() {
    const metadataDiv = document.getElementById('metadata');
    if (!currentData) return;

    const searchTerm = document.getElementById('search').value.toLowerCase();
    const countryFilter = document.getElementById('country-filter').value;

    // Count filtered items
    const northAmerica = currentData['North America'];
    let count = 0;

    const countries = countryFilter === 'all' ? Object.keys(northAmerica) : [countryFilter];

    countries.forEach(country => {
        const items = northAmerica[country]?.manufacturers || [];
        items.forEach(item => {
            const matchesCategory = currentCategory === 'all' || item.category === currentCategory;
            const matchesSearch = !searchTerm || item.name.toLowerCase().includes(searchTerm);
            if (matchesCategory && matchesSearch) count++;
        });
    });

    const categoryText = currentCategory === 'all' ? 'All Partners' : currentCategory;
    metadataDiv.innerHTML = `<strong>${categoryText}</strong>: ${count} | <em>${categoryDescriptions[currentCategory]}</em>`;
}

function switchCategory(category) {
    currentCategory = category;

    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[data-category="${category}"]`).classList.add('active');

    if (category === 'pos') {
        loadPosData();
    } else {
        document.getElementById('pos-sheet-bar').style.display = 'none';
        applyFilters();
    }
}

async function loadPosData() {
    if (posData) {
        renderPosView();
        return;
    }
    document.getElementById('content').innerHTML = '<div class="loading">Loading POS data...</div>';
    try {
        const response = await fetch('360clientssendingpos.json');
        if (!response.ok) {
            throw new Error(`HTTP ${response.status} — ${response.statusText}`);
        }
        const text = await response.text();
        try {
            posData = JSON.parse(text);
        } catch (parseErr) {
            throw new Error(`JSON parse error: ${parseErr.message}`);
        }
        currentPosSheet = Object.keys(posData)[0];
        renderPosView();
    } catch (error) {
        document.getElementById('content').innerHTML =
            `<div class="empty-state">Error loading 360clientssendingpos.json<br><small>${error.message}</small></div>`;
        console.error('Error loading POS data:', error);
    }
}

function renderPosView() {
    const sheetBar = document.getElementById('pos-sheet-bar');
    const sheetNames = Object.keys(posData);

    // Render sheet selector
    sheetBar.style.display = 'flex';
    sheetBar.innerHTML = sheetNames.map(name =>
        `<button class="pos-sheet-btn${name === currentPosSheet ? ' active' : ''}" data-sheet="${name}">${name}</button>`
    ).join('');
    sheetBar.querySelectorAll('.pos-sheet-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            currentPosSheet = btn.dataset.sheet;
            renderPosView();
        });
    });

    const searchTerm = document.getElementById('search').value.toLowerCase();
    const rows = posData[currentPosSheet] || [];
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

    const filtered = searchTerm
        ? rows.filter(row => columns.some(col => {
            const v = row[col];
            return v != null && String(v).toLowerCase().includes(searchTerm);
          }))
        : rows;

    // Update metadata
    document.getElementById('metadata').innerHTML =
        `<strong>${currentPosSheet}</strong>: ${filtered.length} of ${rows.length} rows`;

    const content = document.getElementById('content');
    if (filtered.length === 0) {
        content.innerHTML = '<div class="empty-state">No results found</div>';
        return;
    }

    const headerHtml = columns.map(c => `<th>${c}</th>`).join('');
    const bodyHtml = filtered.map(row =>
        `<tr>${columns.map(c => `<td>${row[c] ?? ''}</td>`).join('')}</tr>`
    ).join('');

    content.innerHTML = `
        <div class="pos-table-wrap">
            <table class="pos-table">
                <thead><tr>${headerHtml}</tr></thead>
                <tbody>${bodyHtml}</tbody>
            </table>
        </div>`;
}

function applyFilters() {
    if (!currentData) return;

    const searchTerm = document.getElementById('search').value.toLowerCase();
    const countryFilter = document.getElementById('country-filter').value;
    const sortOption = document.getElementById('sort').value;

    renderFlatList(searchTerm, countryFilter, sortOption);
    displayMetadata();
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

function renderFlatList(searchTerm, countryFilter, sortOption) {
    const content = document.getElementById('content');
    const northAmerica = currentData['North America'];

    let countries = countryFilter === 'all'
        ? Object.keys(northAmerica)
        : [countryFilter];

    // Collect all items across selected countries
    let allItems = [];

    countries.forEach(country => {
        const items = northAmerica[country]?.manufacturers || [];

        items.forEach(item => {
            // Filter by category
            if (currentCategory !== 'all' && item.category !== currentCategory) {
                return;
            }
            allItems.push({...item, country: country});
        });
    });

    // Filter by search term
    if (searchTerm) {
        allItems = allItems.filter(item =>
            item.name.toLowerCase().includes(searchTerm) ||
            (item.sub_type && item.sub_type.some(st => st.toLowerCase().includes(searchTerm))) ||
            (item.notes && item.notes.toLowerCase().includes(searchTerm))
        );
    }

    // Sort
    allItems = applySortOption(allItems, sortOption, item => item.name);

    // Render
    if (allItems.length === 0) {
        content.innerHTML = '<div class="empty-state">No results found</div>';
        return;
    }

    const html = `
        <div class="manufacturer-list">
            ${allItems.map(item => {
                const details = [];
                if (item.headquarters) details.push(item.headquarters);
                if (item.country) details.push(item.country);

                const detailsText = details.length > 0 ? ` <span class="manufacturer-details">(${details.join(', ')})</span>` : '';

                // Determine which URL to use
                const url = item.website || item.wikipedia_url;
                const urlDisplay = item.website ?
                    new URL(item.website).hostname.replace('www.', '') :
                    (item.wikipedia_url ? 'Wikipedia' : '');

                // Build category tag
                const categoryTag = item.category
                    ? `<span class="category-tag category-${item.category.toLowerCase()}">${item.category}</span>`
                    : '';

                // Build sub-type tags
                const subTypeTags = (item.sub_type && item.sub_type.length > 0)
                    ? item.sub_type.map(st => `<span class="sub-type-tag">${st}</span>`).join('')
                    : '';

                return `
                    <div class="manufacturer-list-item">
                        <div class="manufacturer-info">
                            <span class="manufacturer-name">${item.name}</span>${detailsText}${url ? ` <a href="${url}" target="_blank" class="manufacturer-url">${urlDisplay}</a>` : ''}
                        </div>
                        <div class="tags-row">
                            ${categoryTag}${subTypeTags}
                        </div>
                    </div>
                `;
            }).join('')}
        </div>`;
    content.innerHTML = html;
}

function getCurrentFilteredData() {
    if (!currentData) return [];

    const searchTerm = document.getElementById('search').value.toLowerCase();
    const countryFilter = document.getElementById('country-filter').value;

    let rows = [];
    const northAmerica = currentData['North America'];
    const countries = countryFilter === 'all' ? Object.keys(northAmerica) : [countryFilter];

    countries.forEach(country => {
        const items = northAmerica[country]?.manufacturers || [];
        items.forEach(item => {
            const matchesCategory = currentCategory === 'all' || item.category === currentCategory;
            const matchesSearch = !searchTerm || item.name.toLowerCase().includes(searchTerm);

            if (matchesCategory && matchesSearch) {
                rows.push({
                    Country: country,
                    Category: item.category || '',
                    SubType: (item.sub_type || []).join('; '),
                    Name: item.name,
                    Headquarters: item.headquarters || '',
                    Website: item.website || '',
                    Notes: item.notes || ''
                });
            }
        });
    });

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
    link.download = `channel_insights_${currentCategory}_${new Date().toISOString().split('T')[0]}.csv`;
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
    link.download = `channel_insights_${currentCategory}_${new Date().toISOString().split('T')[0]}.json`;
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
    XLSX.utils.book_append_sheet(wb, ws, currentCategory === 'all' ? 'All' : currentCategory);
    XLSX.writeFile(wb, `channel_insights_${currentCategory}_${new Date().toISOString().split('T')[0]}.xlsx`);
}

// Event listeners
document.querySelectorAll('.tab-button').forEach(button => {
    button.addEventListener('click', () => {
        switchCategory(button.dataset.category);
    });
});

document.getElementById('search').addEventListener('input', () => {
    if (currentCategory === 'pos') renderPosView();
    else applyFilters();
});
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

// Initialize
loadData();
