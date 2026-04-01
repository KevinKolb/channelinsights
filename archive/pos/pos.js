const state = {
    metadata: null,
    providers: [],
};

const defaultRunButtonLabel = "Update Data *";
const runButton = document.getElementById("run-pos");
const runStatus = document.getElementById("run-status");

function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function formatDate(value) {
    if (!value) return "Unknown";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
    });
}

function uniqueValues(values) {
    return [...new Set(values.filter(Boolean))];
}

function getHostname(url) {
    try {
        return new URL(url).hostname.replace("www.", "");
    } catch {
        return url;
    }
}

function getSearchBlob(provider) {
    return [
        provider.name,
        provider.country,
        provider.website,
        provider.vendor,
        provider.notes,
        ...(provider.aliases || []),
        ...(provider.products || []),
        ...(provider.verticals || []),
        ...(provider.tags || []),
    ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
}

function buildCountryOptions(providers) {
    const select = document.getElementById("country-filter");
    const countries = uniqueValues(providers.map((provider) => provider.country)).sort();
    select.innerHTML = '<option value="all">All countries</option>' +
        countries.map((country) => `<option value="${country}">${country}</option>`).join("");
}

function buildVerticalOptions(providers) {
    const select = document.getElementById("vertical-filter");
    const verticals = uniqueValues(providers.flatMap((provider) => provider.verticals || [])).sort();
    select.innerHTML = '<option value="all">All verticals</option>' +
        verticals.map((vertical) => `<option value="${vertical}">${vertical}</option>`).join("");
}

function renderHeroMetrics() {
    const providers = state.providers;
    const countries = uniqueValues(providers.map((provider) => provider.country));
    const products = uniqueValues(providers.flatMap((provider) => provider.products || []));
    const verticals = uniqueValues(providers.flatMap((provider) => provider.verticals || []));
    const erpLinked = providers.filter((provider) =>
        (provider.products || []).some((product) => product.toLowerCase().includes("erp"))
    );

    document.getElementById("metric-total").textContent = String(providers.length).padStart(2, "0");
    document.getElementById("metric-countries").textContent = String(countries.length).padStart(2, "0");
    document.getElementById("metric-products").textContent = String(products.length).padStart(2, "0");
    document.getElementById("metric-verticals").textContent = String(verticals.length).padStart(2, "0");
    document.getElementById("metric-erp").textContent = String(erpLinked.length).padStart(2, "0");
    document.getElementById("metric-updated").textContent = formatDate(state.metadata?.updated_at);
}

function renderProductStrip(providers) {
    const strip = document.getElementById("segment-strip");
    const counts = new Map();

    providers.forEach((provider) => {
        (provider.products || []).forEach((product) => {
            counts.set(product, (counts.get(product) || 0) + 1);
        });
    });

    const ranked = [...counts.entries()]
        .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
        .slice(0, 5);

    if (!ranked.length) {
        strip.innerHTML = "";
        return;
    }

    strip.innerHTML = ranked
        .map(([product, count]) => `<span class="chip">${escapeHtml(product)} / ${count}</span>`)
        .join("");
}

function renderCard(provider) {
    const vendor = provider.vendor
        ? `<div class="meta-block"><strong>Vendor</strong><div>${escapeHtml(provider.vendor)}</div></div>`
        : "";

    const aliases = (provider.aliases || []).length
        ? `<div class="meta-block">
                <strong>Aliases</strong>
                <div class="pill-list">
                    ${(provider.aliases || []).map((alias) => `<span class="pill subtle">${escapeHtml(alias)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const products = (provider.products || []).length
        ? `<div class="meta-block">
                <strong>Products</strong>
                <div class="pill-list">
                    ${(provider.products || []).map((product) => `<span class="pill">${escapeHtml(product)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const verticals = (provider.verticals || []).length
        ? `<div class="meta-block">
                <strong>Verticals</strong>
                <div class="pill-list">
                    ${(provider.verticals || []).map((vertical) => `<span class="pill subtle">${escapeHtml(vertical)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const tags = (provider.tags || []).length
        ? `<div class="meta-block">
                <strong>Tags</strong>
                <div class="pill-list">
                    ${(provider.tags || []).map((tag) => `<span class="pill subtle">${escapeHtml(tag)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const overline = provider.vendor ? "Vendor-backed platform" : "Retail system";

    return `
        <article class="provider-card">
            <span class="card-overline">${overline}</span>
            <div class="card-header">
                <div>
                    <h2 class="card-title">${escapeHtml(provider.name)}</h2>
                    ${provider.website ? `<a class="card-link" href="${escapeHtml(provider.website)}" target="_blank" rel="noreferrer">${escapeHtml(getHostname(provider.website))}</a>` : ""}
                </div>
                <span class="card-country">${escapeHtml(provider.country || "Unknown")}</span>
            </div>
            <p class="card-copy">${escapeHtml(provider.notes || "No notes available.")}</p>
            <div class="card-meta">
                ${vendor}
                ${aliases}
                ${products}
                ${verticals}
                ${tags}
            </div>
            <div class="card-footer">
                <span>ID ${escapeHtml(provider.id || "N/A")}</span>
                <span>Seen ${formatDate(provider.last_seen || provider.first_seen)}</span>
            </div>
        </article>
    `;
}

function setRunStatus(message, tone = "info") {
    if (!runStatus) return;

    if (!message) {
        runStatus.hidden = true;
        runStatus.textContent = "";
        runStatus.className = "run-status";
        return;
    }

    runStatus.hidden = false;
    runStatus.textContent = message;
    runStatus.className = `run-status ${tone}`.trim();
}

function applyFilters() {
    const searchTerm = document.getElementById("search").value.trim().toLowerCase();
    const country = document.getElementById("country-filter").value;
    const vertical = document.getElementById("vertical-filter").value;
    const sort = document.getElementById("sort").value;

    let providers = state.providers.filter((provider) => {
        const matchesSearch = !searchTerm || getSearchBlob(provider).includes(searchTerm);
        const matchesCountry = country === "all" || provider.country === country;
        const matchesVertical = vertical === "all" || (provider.verticals || []).includes(vertical);
        return matchesSearch && matchesCountry && matchesVertical;
    });

    if (sort === "name-asc") {
        providers.sort((a, b) => a.name.localeCompare(b.name));
    } else if (sort === "name-desc") {
        providers.sort((a, b) => b.name.localeCompare(a.name));
    } else if (sort === "country") {
        providers.sort((a, b) => {
            const countryOrder = a.country.localeCompare(b.country);
            return countryOrder || a.name.localeCompare(b.name);
        });
    }

    document.getElementById("result-count").textContent =
        `${providers.length} of ${state.providers.length} POS systems shown`;

    const grid = document.getElementById("card-grid");
    if (!providers.length) {
        grid.innerHTML = '<article class="empty-state">No POS systems match these filters.</article>';
        renderProductStrip([]);
        return;
    }

    renderProductStrip(providers);
    grid.innerHTML = providers.map(renderCard).join("");
}

async function postRunRequest() {
    const endpoints = ["run", "http://127.0.0.1:8766/run", "http://localhost:8766/run"];
    let lastError = new Error("Run endpoint unavailable");

    for (const endpoint of endpoints) {
        try {
            const response = await fetch(endpoint, {
                method: "POST",
                cache: "no-store",
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok || !payload.ok) {
                throw new Error(payload.error || `HTTP ${response.status}`);
            }
            return payload;
        } catch (error) {
            lastError = error;
        }
    }

    throw lastError;
}

async function runGenerator() {
    if (!runButton) return;

    runButton.disabled = true;
    runButton.textContent = "Updating...";
    setRunStatus("Refreshing pos.json via pos.py...", "info");

    try {
        const payload = await postRunRequest();
        await loadProviders();
        const stats = payload.stats || {};
        const totalCount = payload.metadata?.total_count ?? payload.total_count ?? state.providers.length;
        setRunStatus(
            `Refreshed ${totalCount} systems. ${stats.added ?? 0} added, ${stats.updated ?? 0} updated, ${stats.unchanged ?? 0} unchanged.`,
            "success"
        );
    } catch (error) {
        setRunStatus(
            `Could not run pos.py from the browser. Start a local runner with "python pos\\pos.py --serve". ${error.message}`,
            "error"
        );
    } finally {
        runButton.disabled = false;
        runButton.textContent = defaultRunButtonLabel;
    }
}

async function loadProviders() {
    try {
        const cacheBustUrl = `pos.json?ts=${Date.now()}`;
        const response = await fetch(cacheBustUrl, { cache: "no-store" });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const payload = await response.json();
        state.metadata = payload.metadata || {};
        state.providers = (payload.providers || []).slice();

        buildCountryOptions(state.providers);
        buildVerticalOptions(state.providers);
        renderHeroMetrics();
        applyFilters();
    } catch (error) {
        document.getElementById("card-grid").innerHTML =
            `<article class="empty-state">Could not load pos.json<br>${error.message}</article>`;
        document.getElementById("result-count").textContent = "Data unavailable";
        console.error("Error loading POS systems:", error);
    }
}

document.getElementById("search").addEventListener("input", applyFilters);
document.getElementById("country-filter").addEventListener("change", applyFilters);
document.getElementById("vertical-filter").addEventListener("change", applyFilters);
document.getElementById("sort").addEventListener("change", applyFilters);
if (runButton) {
    runButton.addEventListener("click", runGenerator);
}

loadProviders();
