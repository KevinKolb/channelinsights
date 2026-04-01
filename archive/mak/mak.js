const state = {
    metadata: null,
    makers: [],
};

const defaultRunButtonLabel = "Update Data *";
const runButton = document.getElementById("run-mak");
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

function getSearchBlob(maker) {
    return [
        maker.name,
        maker.country,
        maker.website,
        maker.headquarters,
        maker.manufacturer_type,
        maker.source,
        maker.notes,
        ...(maker.subtypes || []),
        ...(maker.brands || []),
    ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
}

function buildCountryOptions(makers) {
    const select = document.getElementById("country-filter");
    const countries = uniqueValues(makers.map((maker) => maker.country)).sort();
    select.innerHTML = '<option value="all">All countries</option>' +
        countries.map((country) => `<option value="${country}">${country}</option>`).join("");
}

function buildSubtypeOptions(makers) {
    const select = document.getElementById("subtype-filter");
    const subtypes = uniqueValues(makers.flatMap((maker) => maker.subtypes || [])).sort();
    select.innerHTML = '<option value="all">All maker subtypes</option>' +
        subtypes.map((subtype) => `<option value="${subtype}">${subtype}</option>`).join("");
}

function renderHeroMetrics() {
    const makers = state.makers;
    const countries = uniqueValues(makers.map((maker) => maker.country));
    const brandEntries = uniqueValues(makers.flatMap((maker) => maker.brands || []));
    const types = uniqueValues(makers.map((maker) => maker.manufacturer_type));
    const portfolios = makers.filter((maker) => (maker.brand_count || 0) > 0);

    document.getElementById("metric-total").textContent = String(makers.length).padStart(2, "0");
    document.getElementById("metric-countries").textContent = String(countries.length).padStart(2, "0");
    document.getElementById("metric-brands").textContent = String(brandEntries.length).padStart(2, "0");
    document.getElementById("metric-types").textContent = String(types.length).padStart(2, "0");
    document.getElementById("metric-portfolios").textContent = String(portfolios.length).padStart(2, "0");
    document.getElementById("metric-updated").textContent = formatDate(
        state.metadata?.generated_at || state.metadata?.updated_at
    );
}

function renderSubtypeStrip(makers) {
    const strip = document.getElementById("segment-strip");
    const counts = new Map();

    makers.forEach((maker) => {
        (maker.subtypes || []).forEach((subtype) => {
            counts.set(subtype, (counts.get(subtype) || 0) + 1);
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
        .map(([subtype, count]) => `<span class="chip">${escapeHtml(subtype)} / ${count}</span>`)
        .join("");
}

function formatRevenue(value) {
    if (!value || Number.isNaN(Number(value))) {
        return "";
    }
    return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
        notation: "compact",
        maximumFractionDigits: 1,
    }).format(value);
}

function renderCard(maker) {
    const headquarters = maker.headquarters
        ? `<div class="meta-block"><strong>Headquarters</strong><div>${escapeHtml(maker.headquarters)}</div></div>`
        : "";

    const typeBlock = maker.manufacturer_type
        ? `<div class="meta-block"><strong>Type</strong><div>${escapeHtml(maker.manufacturer_type)}</div></div>`
        : "";

    const subtypes = (maker.subtypes || []).length
        ? `<div class="meta-block">
                <strong>Subtypes</strong>
                <div class="pill-list">
                    ${(maker.subtypes || []).map((subtype) => `<span class="pill subtle">${escapeHtml(subtype)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const brands = (maker.brands || []).length
        ? `<div class="meta-block span-2">
                <strong>Brands</strong>
                <div class="pill-list">
                    ${(maker.brands || []).map((brand) => `<span class="pill">${escapeHtml(brand)}</span>`).join("")}
                </div>
           </div>`
        : `<div class="meta-block span-2">
                <strong>Brands</strong>
                <p class="empty-note">No distinct brand portfolio mapped from the current local brand file.</p>
           </div>`;

    const sourceBlock = maker.source
        ? `<div class="meta-block"><strong>Source</strong><div>${escapeHtml(maker.source)}</div></div>`
        : "";

    const revenueBlock = maker.revenue_usd
        ? `<div class="meta-block"><strong>Revenue</strong><div>${escapeHtml(formatRevenue(maker.revenue_usd))}</div></div>`
        : "";

    const overline = (maker.brand_count || 0) > 0 ? "Brand owner / portfolio" : "Maker / single-brand";

    return `
        <article class="maker-card">
            <span class="card-overline">${overline}</span>
            <div class="card-header">
                <div>
                    <h2 class="card-title">${escapeHtml(maker.name)}</h2>
                    ${maker.website ? `<a class="card-link" href="${escapeHtml(maker.website)}" target="_blank" rel="noreferrer">${escapeHtml(getHostname(maker.website))}</a>` : ""}
                </div>
                <span class="card-country">${escapeHtml(maker.country || "Unknown")}</span>
            </div>
            <p class="card-copy">${escapeHtml(maker.notes || "No notes available.")}</p>
            <div class="card-meta">
                ${headquarters}
                ${typeBlock}
                ${subtypes}
                ${sourceBlock}
                ${revenueBlock}
                ${brands}
            </div>
            <div class="card-footer">
                <span>ID ${escapeHtml(maker.id || "N/A")}</span>
                <span>Brands ${escapeHtml(String(maker.brand_count || 0))}</span>
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
    const subtype = document.getElementById("subtype-filter").value;
    const sort = document.getElementById("sort").value;

    let makers = state.makers.filter((maker) => {
        const matchesSearch = !searchTerm || getSearchBlob(maker).includes(searchTerm);
        const matchesCountry = country === "all" || maker.country === country;
        const matchesSubtype = subtype === "all" || (maker.subtypes || []).includes(subtype);
        return matchesSearch && matchesCountry && matchesSubtype;
    });

    if (sort === "name-asc") {
        makers.sort((a, b) => a.name.localeCompare(b.name));
    } else if (sort === "name-desc") {
        makers.sort((a, b) => b.name.localeCompare(a.name));
    } else if (sort === "brand-count") {
        makers.sort((a, b) => (b.brand_count || 0) - (a.brand_count || 0) || a.name.localeCompare(b.name));
    } else if (sort === "country") {
        makers.sort((a, b) => {
            const countryOrder = a.country.localeCompare(b.country);
            return countryOrder || a.name.localeCompare(b.name);
        });
    }

    document.getElementById("result-count").textContent =
        `${makers.length} of ${state.makers.length} makers shown`;

    const grid = document.getElementById("card-grid");
    if (!makers.length) {
        grid.innerHTML = '<article class="empty-state">No makers match these filters.</article>';
        renderSubtypeStrip([]);
        return;
    }

    renderSubtypeStrip(makers);
    grid.innerHTML = makers.map(renderCard).join("");
}

async function postRunRequest() {
    const endpoints = ["run", "http://127.0.0.1:8767/run", "http://localhost:8767/run"];
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
    setRunStatus("Refreshing mak.json via mak.py...", "info");

    try {
        const payload = await postRunRequest();
        await loadMakers();
        const totalCount = payload.metadata?.total_count ?? payload.total_count ?? state.makers.length;
        const brandCount = payload.metadata?.mapped_brand_entries ?? 0;
        setRunStatus(
            `Refreshed ${totalCount} makers with ${brandCount} mapped brand entries.`,
            "success"
        );
    } catch (error) {
        setRunStatus(
            `Could not run mak.py from the browser. Start a local runner with "python mak\\mak.py --serve". ${error.message}`,
            "error"
        );
    } finally {
        runButton.disabled = false;
        runButton.textContent = defaultRunButtonLabel;
    }
}

async function loadMakers() {
    try {
        const cacheBustUrl = `mak.json?ts=${Date.now()}`;
        const response = await fetch(cacheBustUrl, { cache: "no-store" });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const payload = await response.json();
        state.metadata = payload.metadata || {};
        state.makers = (payload.makers || []).slice();

        buildCountryOptions(state.makers);
        buildSubtypeOptions(state.makers);
        renderHeroMetrics();
        applyFilters();
    } catch (error) {
        document.getElementById("card-grid").innerHTML =
            `<article class="empty-state">Could not load mak.json<br>${error.message}</article>`;
        document.getElementById("result-count").textContent = "Data unavailable";
        console.error("Error loading makers:", error);
    }
}

document.getElementById("search").addEventListener("input", applyFilters);
document.getElementById("country-filter").addEventListener("change", applyFilters);
document.getElementById("subtype-filter").addEventListener("change", applyFilters);
document.getElementById("sort").addEventListener("change", applyFilters);
if (runButton) {
    runButton.addEventListener("click", runGenerator);
}

loadMakers();
