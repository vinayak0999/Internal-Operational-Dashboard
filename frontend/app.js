/**
 * Encord Operations Dashboard — Frontend Logic
 * ==============================================
 * Fetches data from the FastAPI backend and renders the dashboard.
 * Auto-refreshes every 5 minutes. No destructive operations.
 */

const API_BASE = '';  // Same origin

// ─── State ───
let state = {
    summary: null,
    projects: [],
    annotators: [],
    outliers: [],
    filters: {},
    sortConfig: { table: null, field: null, asc: true },
};

// ─── Initialization ───
document.addEventListener('DOMContentLoaded', () => {
    loadDashboard();
    loadFilters();
    // Auto-refresh every 5 minutes
    setInterval(loadDashboard, 5 * 60 * 1000);
});

async function loadDashboard() {
    try {
        await Promise.all([
            loadSummary(),
            loadProjects(),
            loadAnnotators(),
            loadOutliers(),
        ]);
    } catch (err) {
        console.error('Dashboard load error:', err);
    }
}

// ─── API Helpers ───
async function fetchApi(endpoint, params = {}) {
    const url = new URL(`${API_BASE}${endpoint}`, window.location.origin);
    Object.entries(params).forEach(([k, v]) => {
        if (v) url.searchParams.set(k, v);
    });
    const res = await fetch(url.toString());
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
}

// ─── Summary Cards ───
async function loadSummary() {
    try {
        const data = await fetchApi('/api/metrics/summary');
        state.summary = data;

        document.getElementById('stat-projects').textContent = data.total_projects || 0;
        document.getElementById('stat-annotators').textContent = data.total_annotators || 0;
        document.getElementById('stat-tasks').textContent = formatNumber(data.total_tasks_completed || 0);
        document.getElementById('stat-rejection').textContent = formatPercent(data.avg_rejection_rate || 0);
        document.getElementById('stat-tpt').textContent = formatDuration(data.avg_tpt_seconds || 0);

        const totalFlags = (data.red_flags || 0) + (data.amber_flags || 0);
        document.getElementById('stat-flags').textContent = totalFlags;
        document.getElementById('stat-flags-detail').textContent = `${data.red_flags || 0} red / ${data.amber_flags || 0} amber`;

        // Sync status
        if (data.last_sync) {
            const syncDot = document.querySelector('.sync-status .dot');
            const syncText = document.getElementById('sync-text');
            syncDot.className = `dot ${data.last_sync.status === 'completed' ? 'green' : data.last_sync.status === 'failed' ? 'red' : 'amber'}`;
            if (data.last_sync.completed_at) {
                syncText.textContent = `Last sync: ${formatTimeAgo(data.last_sync.completed_at)}`;
            } else if (data.last_sync.status === 'never') {
                syncText.textContent = 'Last sync: never';
            }
        }
    } catch (err) {
        console.error('Summary load error:', err);
    }
}

// ─── Projects ───
async function loadProjects() {
    try {
        const params = {};
        const modality = document.getElementById('filter-modality')?.value;
        const client = document.getElementById('filter-client')?.value;
        if (modality) params.modality = modality;
        if (client) params.client = client;

        const data = await fetchApi('/api/projects', params);
        state.projects = data.projects || [];
        renderProjectsTable();
    } catch (err) {
        console.error('Projects load error:', err);
    }
}

function renderProjectsTable() {
    const tbody = document.getElementById('projects-tbody');
    if (!state.projects.length) {
        tbody.innerHTML = `
            <tr><td colspan="8">
                <div class="empty-state">
                    <div class="icon">📊</div>
                    <h3>No Projects Yet</h3>
                    <p>Click "Sync Now" to fetch data from Encord.</p>
                </div>
            </td></tr>`;
        return;
    }

    tbody.innerHTML = state.projects.map(p => `
        <tr class="clickable-row" onclick="openProjectDetail('${p.project_hash}')">
            <td><strong>${escapeHtml(p.title)}</strong></td>
            <td>${escapeHtml(p.modality || '—')}</td>
            <td>${escapeHtml(p.client_tag || '—')}</td>
            <td>${p.annotator_count}</td>
            <td>${formatNumber(p.total_tasks)}</td>
            <td>${ragCell(p.avg_rejection_rate, 'percent', getRejectionRag(p.avg_rejection_rate))}</td>
            <td>${formatDuration(p.avg_tpt_seconds)}</td>
            <td>${ragBadge(p.rag_status)}</td>
        </tr>
    `).join('');
}

// ─── Annotators ───
async function loadAnnotators() {
    try {
        const params = {};
        const project = document.getElementById('filter-project')?.value;
        if (project) params.project = project;

        const data = await fetchApi('/api/annotators', params);
        state.annotators = data.annotators || [];
        renderAnnotatorsTable();
    } catch (err) {
        console.error('Annotators load error:', err);
    }
}

function renderAnnotatorsTable() {
    const tbody = document.getElementById('annotators-tbody');
    if (!state.annotators.length) {
        tbody.innerHTML = `
            <tr><td colspan="8">
                <div class="empty-state">
                    <div class="icon">👤</div>
                    <h3>No Annotators Yet</h3>
                    <p>Sync data to see annotator performance metrics.</p>
                </div>
            </td></tr>`;
        return;
    }

    tbody.innerHTML = state.annotators.map(a => `
        <tr class="clickable-row" onclick="openAnnotatorDetail('${encodeURIComponent(a.email)}')">
            <td><strong>${escapeHtml(a.email)}</strong></td>
            <td>${a.project_count}</td>
            <td>${formatNumber(a.total_completed)}</td>
            <td>${ragCell(a.avg_rejection_rate, 'percent', getRejectionRag(a.avg_rejection_rate))}</td>
            <td>${formatDuration(a.avg_tpt_seconds)}</td>
            <td>${a.avg_throughput_per_hour}</td>
            <td>${a.total_time_hours}h</td>
            <td>${ragBadge(a.rag_status)}</td>
        </tr>
    `).join('');
}

// ─── Outliers ───
async function loadOutliers() {
    try {
        const data = await fetchApi('/api/outliers', { limit: '20' });
        state.outliers = data.outliers || [];
        renderOutliers();
    } catch (err) {
        console.error('Outliers load error:', err);
    }
}

function renderOutliers() {
    const list = document.getElementById('outlier-list');
    const count = document.getElementById('outlier-count');

    if (!state.outliers.length) {
        count.textContent = '0 flags';
        count.className = 'badge green';
        list.innerHTML = `
            <div class="empty-state">
                <div class="icon">✨</div>
                <h3>All Clear</h3>
                <p>No outliers detected. Sync data to check for anomalies.</p>
            </div>`;
        return;
    }

    const redCount = state.outliers.filter(o => o.level === 'red').length;
    const amberCount = state.outliers.filter(o => o.level === 'amber').length;
    count.textContent = `${redCount} red, ${amberCount} amber`;
    count.className = `badge ${redCount > 0 ? 'red' : 'amber'}`;

    list.innerHTML = state.outliers.map(o => `
        <div class="outlier-item ${o.level}">
            <span class="outlier-icon">${o.level === 'red' ? '🔴' : '🟡'}</span>
            <div class="outlier-text">
                <strong>${escapeHtml(o.annotator)}</strong> — ${escapeHtml(o.description)}
            </div>
            <div class="outlier-meta">
                ${escapeHtml(o.project_title || '')}<br>
                ${o.flagged_at ? formatTimeAgo(o.flagged_at) : ''}
            </div>
        </div>
    `).join('');
}

// ─── Filters ───
async function loadFilters() {
    try {
        const data = await fetchApi('/api/filters');
        populateDropdown('filter-project', data.projects?.map(p => ({ value: p.hash, label: p.title })) || []);
        populateDropdown('filter-annotator', data.annotators?.map(a => ({ value: a, label: a })) || []);
        populateDropdown('filter-modality', data.modalities?.map(m => ({ value: m, label: m })) || []);
        populateDropdown('filter-client', data.clients?.map(c => ({ value: c, label: c })) || []);
    } catch (err) {
        console.error('Filters load error:', err);
    }
}

function populateDropdown(id, options) {
    const select = document.getElementById(id);
    if (!select) return;
    const current = select.value;
    // Keep the first "All" option
    while (select.options.length > 1) select.remove(1);
    options.forEach(opt => {
        const option = document.createElement('option');
        option.value = opt.value;
        option.textContent = opt.label;
        select.appendChild(option);
    });
    select.value = current;
}

function applyFilters() {
    loadProjects();
    loadAnnotators();
}

// ─── Sync ───
async function triggerSync() {
    const btn = document.getElementById('btn-sync');
    btn.classList.add('syncing');
    btn.textContent = '⏳ Syncing...';

    try {
        const result = await fetch(`${API_BASE}/api/sync`, { method: 'POST' });
        const data = await result.json();
        console.log('Sync result:', data);

        // Reload everything
        await loadDashboard();
        await loadFilters();
    } catch (err) {
        console.error('Sync error:', err);
        alert('Sync failed. Check console for details.');
    } finally {
        btn.classList.remove('syncing');
        btn.textContent = '↻ Sync Now';
    }
}

// ─── Tabs ───
function switchTab(tab) {
    document.getElementById('tab-projects').classList.toggle('active', tab === 'projects');
    document.getElementById('tab-annotators').classList.toggle('active', tab === 'annotators');
    document.getElementById('table-projects').style.display = tab === 'projects' ? 'block' : 'none';
    document.getElementById('table-annotators').style.display = tab === 'annotators' ? 'block' : 'none';
}

// ─── Sorting ───
function sortTable(table, field) {
    const cfg = state.sortConfig;
    if (cfg.table === table && cfg.field === field) {
        cfg.asc = !cfg.asc;
    } else {
        cfg.table = table;
        cfg.field = field;
        cfg.asc = true;
    }

    const data = table === 'projects' ? state.projects : state.annotators;
    data.sort((a, b) => {
        let va = a[field], vb = b[field];
        if (typeof va === 'string') va = va.toLowerCase();
        if (typeof vb === 'string') vb = vb.toLowerCase();
        if (va < vb) return cfg.asc ? -1 : 1;
        if (va > vb) return cfg.asc ? 1 : -1;
        return 0;
    });

    if (table === 'projects') renderProjectsTable();
    else renderAnnotatorsTable();
}

// ─── Detail Modals ───
async function openProjectDetail(hash) {
    try {
        const data = await fetchApi(`/api/projects/${hash}`);
        const modal = document.getElementById('modal-overlay');
        document.getElementById('modal-title').textContent = `📊 ${data.project.title}`;

        let body = `
            <div class="modal-metrics">
                <div class="modal-metric">
                    <div class="label">Modality</div>
                    <div class="value">${escapeHtml(data.project.modality || '—')}</div>
                </div>
                <div class="modal-metric">
                    <div class="label">Client</div>
                    <div class="value">${escapeHtml(data.project.client_tag || '—')}</div>
                </div>
                <div class="modal-metric">
                    <div class="label">Last Synced</div>
                    <div class="value" style="font-size:0.9rem">${data.project.last_synced ? formatTimeAgo(data.project.last_synced) : 'Never'}</div>
                </div>
            </div>`;

        // Annotator breakdown
        const annotators = Object.entries(data.annotators || {});
        if (annotators.length) {
            body += `<h3 style="margin-bottom:12px; color:var(--text-secondary)">Annotator Breakdown</h3>
            <div class="table-container"><table><thead><tr>
                <th>Annotator</th><th>Rejection Rate</th><th>TPT</th><th>Throughput</th><th>Tasks</th>
            </tr></thead><tbody>`;
            annotators.forEach(([email, entries]) => {
                const latest = entries[0] || {};
                body += `<tr>
                    <td>${escapeHtml(email)}</td>
                    <td>${ragCell(latest.rejection_rate || 0, 'percent', getRejectionRag(latest.rejection_rate || 0))}</td>
                    <td>${formatDuration(latest.time_per_task_seconds || 0)}</td>
                    <td>${(latest.throughput_per_hour || 0).toFixed(1)}/hr</td>
                    <td>${latest.tasks_completed || 0}</td>
                </tr>`;
            });
            body += '</tbody></table></div>';
        }

        // Outliers
        if (data.outliers?.length) {
            body += `<h3 style="margin:20px 0 12px; color:var(--text-secondary)">⚠️ Outlier Flags</h3>`;
            data.outliers.forEach(o => {
                body += `<div class="outlier-item ${o.level}">
                    <span class="outlier-icon">${o.level === 'red' ? '🔴' : '🟡'}</span>
                    <div class="outlier-text"><strong>${escapeHtml(o.annotator)}</strong> — ${escapeHtml(o.description)}</div>
                </div>`;
            });
        }

        document.getElementById('modal-body').innerHTML = body;
        modal.classList.add('active');
    } catch (err) {
        console.error('Project detail error:', err);
    }
}

async function openAnnotatorDetail(email) {
    try {
        const data = await fetchApi(`/api/annotators/${decodeURIComponent(email)}`);
        const modal = document.getElementById('modal-overlay');
        document.getElementById('modal-title').textContent = `👤 ${data.annotator.email}`;

        let body = `
            <div class="modal-metrics">
                <div class="modal-metric">
                    <div class="label">Name</div>
                    <div class="value">${escapeHtml(data.annotator.name || '—')}</div>
                </div>
                <div class="modal-metric">
                    <div class="label">First Seen</div>
                    <div class="value" style="font-size:0.9rem">${data.annotator.first_seen ? formatTimeAgo(data.annotator.first_seen) : '—'}</div>
                </div>
                <div class="modal-metric">
                    <div class="label">Projects</div>
                    <div class="value">${Object.keys(data.projects || {}).length}</div>
                </div>
            </div>`;

        // Per-project breakdown
        const projects = Object.entries(data.projects || {});
        if (projects.length) {
            body += `<h3 style="margin-bottom:12px; color:var(--text-secondary)">Performance by Project</h3>`;
            projects.forEach(([hash, proj]) => {
                const latest = proj.entries[0] || {};
                body += `<div class="chart-container">
                    <h3>${escapeHtml(proj.project_title)}</h3>
                    <div class="modal-metrics" style="margin-bottom:0">
                        <div class="modal-metric">
                            <div class="label">Rejection Rate</div>
                            <div class="value">${formatPercent(latest.rejection_rate || 0)}</div>
                        </div>
                        <div class="modal-metric">
                            <div class="label">TPT</div>
                            <div class="value">${formatDuration(latest.time_per_task_seconds || 0)}</div>
                        </div>
                        <div class="modal-metric">
                            <div class="label">Throughput</div>
                            <div class="value">${(latest.throughput_per_hour || 0).toFixed(1)}/hr</div>
                        </div>
                    </div>
                </div>`;
            });
        }

        // Outliers
        if (data.outliers?.length) {
            body += `<h3 style="margin:20px 0 12px; color:var(--text-secondary)">⚠️ Flags for this annotator</h3>`;
            data.outliers.forEach(o => {
                body += `<div class="outlier-item ${o.level}">
                    <span class="outlier-icon">${o.level === 'red' ? '🔴' : '🟡'}</span>
                    <div class="outlier-text"><strong>${escapeHtml(o.project)}</strong> — ${escapeHtml(o.description)}</div>
                </div>`;
            });
        }

        document.getElementById('modal-body').innerHTML = body;
        modal.classList.add('active');
    } catch (err) {
        console.error('Annotator detail error:', err);
    }
}

function closeModal(event) {
    if (event && event.target !== document.getElementById('modal-overlay')) return;
    document.getElementById('modal-overlay').classList.remove('active');
}

// Close modal on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
});

// ─── Formatting Helpers ───
function formatNumber(n) {
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return n.toString();
}

function formatPercent(v) {
    return (v * 100).toFixed(1) + '%';
}

function formatDuration(seconds) {
    if (!seconds || seconds <= 0) return '—';
    if (seconds < 60) return Math.round(seconds) + 's';
    if (seconds < 3600) return Math.round(seconds / 60) + 'm ' + Math.round(seconds % 60) + 's';
    return Math.floor(seconds / 3600) + 'h ' + Math.round((seconds % 3600) / 60) + 'm';
}

function formatTimeAgo(isoStr) {
    if (!isoStr) return '—';
    const diff = Date.now() - new Date(isoStr).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    return `${days}d ago`;
}

function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function getRejectionRag(rate) {
    if (rate >= 0.15) return 'red';
    if (rate >= 0.10) return 'amber';
    return 'green';
}

function ragCell(value, format, level) {
    const display = format === 'percent' ? formatPercent(value) : value;
    return `<span class="rag-cell ${level}"><span class="rag-dot ${level}"></span>${display}</span>`;
}

function ragBadge(level) {
    const labels = { red: '● Critical', amber: '● Warning', green: '● Healthy' };
    return `<span class="badge ${level}">${labels[level] || '● —'}</span>`;
}
