const state = {
    metadata: null,
    groups: [],
};

const defaultRunButtonLabel = "Update Data *";
const runButton = document.getElementById("run-bg");
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

function humanizeToken(value) {
    return String(value ?? "")
        .split("_")
        .filter(Boolean)
        .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
        .join(" ");
}

function renderSourceLinks(urls) {
    if (!(urls || []).length) {
        return "";
    }

    return `<div class="source-strip">
                ${(urls || []).map((url) => (
                    `<a class="source-link" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${escapeHtml(getHostname(url))}</a>`
                )).join("")}
           </div>`;
}

function getSearchBlob(group) {
    return [
        group.name,
        group.country,
        group.website,
        group.parent_company,
        group.notes,
        group.partner_company_note,
        group.preferred_pos_note,
        group.preferred_pos_status,
        ...(group.aliases || []),
        ...(group.member_segments || []),
        ...(group.partner_companies || []),
        ...(group.partner_company_sources || []),
        ...(group.preferred_pos || []),
        ...(group.preferred_pos_sources || []),
        ...(group.tags || []),
    ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
}

function buildCountryOptions(groups) {
    const select = document.getElementById("country-filter");
    const countries = uniqueValues(groups.map((group) => group.country)).sort();
    select.innerHTML = '<option value="all">All countries</option>' +
        countries.map((country) => `<option value="${country}">${country}</option>`).join("");
}

function buildSegmentOptions(groups) {
    const select = document.getElementById("segment-filter");
    const segments = uniqueValues(groups.flatMap((group) => group.member_segments || [])).sort();
    select.innerHTML = '<option value="all">All segments</option>' +
        segments.map((segment) => `<option value="${segment}">${segment}</option>`).join("");
}

function renderHeroMetrics() {
    const groups = state.groups;
    const countries = uniqueValues(groups.map((group) => group.country));
    const segments = uniqueValues(groups.flatMap((group) => group.member_segments || []));
    const groupsWithPosSignals = groups.filter((group) => (group.preferred_pos || []).length > 0);
    const posPlatforms = uniqueValues(groups.flatMap((group) => group.preferred_pos || []));

    document.getElementById("metric-total").textContent = String(groups.length).padStart(2, "0");
    document.getElementById("metric-countries").textContent = String(countries.length).padStart(2, "0");
    document.getElementById("metric-segments").textContent = String(segments.length).padStart(2, "0");
    document.getElementById("metric-pos-groups").textContent = String(groupsWithPosSignals.length).padStart(2, "0");
    document.getElementById("metric-pos-platforms").textContent = String(posPlatforms.length).padStart(2, "0");
    document.getElementById("metric-updated").textContent = formatDate(state.metadata?.updated_at);

    const posSummary = document.getElementById("pos-summary");
    if (posSummary) {
        posSummary.textContent =
            `${groupsWithPosSignals.length} groups have public POS signals across ${posPlatforms.length} named platforms. ` +
            "Confidence is stored per group because many groups publish an approved ecosystem rather than one exclusive POS.";
    }
}

function renderSegmentStrip(groups) {
    const strip = document.getElementById("segment-strip");
    const counts = new Map();

    groups.forEach((group) => {
        (group.member_segments || []).forEach((segment) => {
            counts.set(segment, (counts.get(segment) || 0) + 1);
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
        .map(([segment, count]) => `<span class="chip">${segment} / ${count}</span>`)
        .join("");
}

function renderCard(group) {
    const aliases = (group.aliases || []).length
        ? `<div class="meta-block">
                <strong>Aliases</strong>
                <div class="pill-list">
                    ${(group.aliases || []).map((alias) => `<span class="pill subtle">${escapeHtml(alias)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const segments = (group.member_segments || []).length
        ? `<div class="meta-block">
                <strong>Member segments</strong>
                <div class="pill-list">
                    ${(group.member_segments || []).map((segment) => `<span class="pill">${escapeHtml(segment)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const tags = (group.tags || []).length
        ? `<div class="meta-block">
                <strong>Tags</strong>
                <div class="pill-list">
                    ${(group.tags || []).map((tag) => `<span class="pill subtle">${escapeHtml(tag)}</span>`).join("")}
                </div>
           </div>`
        : "";

    const companyMeta = ((group.partner_companies || []).length || group.partner_company_note)
        ? `<div class="meta-block span-2 detail-block">
                <strong>Companies</strong>
                ${(group.partner_companies || []).length
                    ? `<div class="pill-list">
                            ${(group.partner_companies || []).map((company) => `<span class="pill subtle">${escapeHtml(company)}</span>`).join("")}
                       </div>`
                    : '<div class="empty-note">No named companies captured yet.</div>'}
                ${group.partner_company_last_checked ? `<div class="signal-strip"><span class="signal-chip subtle">Checked ${escapeHtml(formatDate(group.partner_company_last_checked))}</span></div>` : ""}
                ${group.partner_company_note ? `<p class="meta-note">${escapeHtml(group.partner_company_note)}</p>` : ""}
                ${renderSourceLinks(group.partner_company_sources || [])}
           </div>`
        : "";

    const posSignals = (group.preferred_pos || []).length
        ? `<div class="pill-list">
                ${(group.preferred_pos || []).map((platform) => `<span class="pill">${escapeHtml(platform)}</span>`).join("")}
           </div>`
        : '<div class="empty-note">No named platform found in current public sources.</div>';

    const posContext = [
        group.preferred_pos_confidence ? `<span class="signal-chip">${escapeHtml(humanizeToken(group.preferred_pos_confidence))} confidence</span>` : "",
        group.preferred_pos_status ? `<span class="signal-chip subtle">${escapeHtml(humanizeToken(group.preferred_pos_status))}</span>` : "",
        group.preferred_pos_last_checked ? `<span class="signal-chip subtle">Checked ${escapeHtml(formatDate(group.preferred_pos_last_checked))}</span>` : "",
    ]
        .filter(Boolean)
        .join("");

    const posMeta = `
        <div class="meta-block span-2 detail-block">
            <strong>POS signals</strong>
            ${posSignals}
            ${posContext ? `<div class="signal-strip">${posContext}</div>` : ""}
            ${group.preferred_pos_note ? `<p class="meta-note">${escapeHtml(group.preferred_pos_note)}</p>` : ""}
            ${renderSourceLinks(group.preferred_pos_sources || [])}
        </div>
    `;

    return `
        <article class="group-card">
            <span class="card-overline">${group.parent_company ? "Parent-linked group" : "Independent grouping"}</span>
            <div class="card-header">
                <div>
                    <h2 class="card-title">${escapeHtml(group.name)}</h2>
                    ${group.website ? `<a class="card-link" href="${escapeHtml(group.website)}" target="_blank" rel="noreferrer">${escapeHtml(getHostname(group.website))}</a>` : ""}
                </div>
                <span class="card-country">${escapeHtml(group.country || "Unknown")}</span>
            </div>
            <p class="card-copy">${escapeHtml(group.notes || "No notes available.")}</p>
            <div class="card-meta">
                ${group.parent_company ? `<div class="meta-block"><strong>Parent</strong><div>${escapeHtml(group.parent_company)}</div></div>` : ""}
                ${aliases}
                ${segments}
                ${tags}
                ${companyMeta}
                ${posMeta}
            </div>
            <div class="card-footer">
                <span>ID ${escapeHtml(group.id || "N/A")}</span>
                <span>Seen ${formatDate(group.last_seen || group.first_seen)}</span>
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
    const segment = document.getElementById("segment-filter").value;
    const sort = document.getElementById("sort").value;

    let groups = state.groups.filter((group) => {
        const matchesSearch = !searchTerm || getSearchBlob(group).includes(searchTerm);
        const matchesCountry = country === "all" || group.country === country;
        const matchesSegment = segment === "all" || (group.member_segments || []).includes(segment);
        return matchesSearch && matchesCountry && matchesSegment;
    });

    if (sort === "name-asc") {
        groups.sort((a, b) => a.name.localeCompare(b.name));
    } else if (sort === "name-desc") {
        groups.sort((a, b) => b.name.localeCompare(a.name));
    } else if (sort === "country") {
        groups.sort((a, b) => {
            const countryOrder = a.country.localeCompare(b.country);
            return countryOrder || a.name.localeCompare(b.name);
        });
    }

    document.getElementById("result-count").textContent =
        `${groups.length} of ${state.groups.length} buying groups shown`;

    const grid = document.getElementById("card-grid");
    if (!groups.length) {
        grid.innerHTML = '<article class="empty-state">No buying groups match these filters.</article>';
        renderSegmentStrip([]);
        return;
    }

    renderSegmentStrip(groups);
    grid.innerHTML = groups.map(renderCard).join("");
}

async function postRunRequest() {
    const endpoints = ["run", "http://127.0.0.1:8765/run", "http://localhost:8765/run"];
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
    setRunStatus("Refreshing bg.json via bg.py...", "info");

    try {
        const payload = await postRunRequest();
        await loadBuyingGroups();
        const stats = payload.stats || {};
        const totalCount = payload.metadata?.total_count ?? payload.total_count ?? state.groups.length;
        setRunStatus(
            `Refreshed ${totalCount} groups. ${stats.added ?? 0} added, ${stats.updated ?? 0} updated, ${stats.unchanged ?? 0} unchanged.`,
            "success"
        );
    } catch (error) {
        setRunStatus(
            `Could not run bg.py from the browser. Start a local runner with "python bg\\bg.py --serve". ${error.message}`,
            "error"
        );
    } finally {
        runButton.disabled = false;
        runButton.textContent = defaultRunButtonLabel;
    }
}

async function loadBuyingGroups() {
    try {
        const cacheBustUrl = `bg.json?ts=${Date.now()}`;
        const response = await fetch(cacheBustUrl, { cache: "no-store" });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const payload = await response.json();
        state.metadata = payload.metadata || {};
        state.groups = (payload.buying_groups || []).slice();

        buildCountryOptions(state.groups);
        buildSegmentOptions(state.groups);
        renderHeroMetrics();
        applyFilters();
    } catch (error) {
        document.getElementById("card-grid").innerHTML =
            `<article class="empty-state">Could not load bg.json<br>${error.message}</article>`;
        document.getElementById("result-count").textContent = "Data unavailable";
        console.error("Error loading buying groups:", error);
    }
}

document.getElementById("search").addEventListener("input", applyFilters);
document.getElementById("country-filter").addEventListener("change", applyFilters);
document.getElementById("segment-filter").addEventListener("change", applyFilters);
document.getElementById("sort").addEventListener("change", applyFilters);
if (runButton) {
    runButton.addEventListener("click", runGenerator);
}

loadBuyingGroups();
