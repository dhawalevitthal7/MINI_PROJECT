/**
 * QueryVista — AI Migration & Dual-DB Intelligence Platform
 * Migration runs FIRST, then SQLAI dual-DB exploration.
 */

(() => {
    'use strict';

    const API_BASE = window.location.origin;
    const TOAST_DURATION = 4000;

    // ===== STATE =====
    const state = {
        // SQLAI
        sourceUrl: '',
        targetUrl: '',
        sourceDbName: '',
        targetDbName: '',
        sourceDialect: '',
        targetDialect: '',
        connected: false,
        safeMode: true,
        sourceTables: [],
        targetTables: [],
        sourceSchema: null,
        targetSchema: null,
        diff: null,
        queryHistory: [],
        currentPage: 'migration',
        sidebarCollapsed: false,

        // Migration
        pipelines: [],
        selectedPipeline: null,
        migSessionId: null,
        migSourceConfig: {},
        migTargetConfig: {},
        migPlan: null,
    };

    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    const dom = {
        sidebar: $('#sidebar'),
        sidebarToggle: $('#sidebarToggle'),
        toggleIcon: $('#toggleIcon'),
        mobileMenuBtn: $('#mobileMenuBtn'),
        mobileOverlay: $('#mobileOverlay'),
        headerTitle: $('#headerTitle'),
        statusChip: $('#statusChip'),
        statusText: $('#statusText'),
        sourceDialectBadge: $('#sourceDialectBadge'),
        targetDialectBadge: $('#targetDialectBadge'),
        safeModeToggle: $('#safeModeToggle'),
        themeToggle: $('#themeToggle'),

        // Connect
        sourceUrlInput: $('#sourceUrlInput'),
        targetUrlInput: $('#targetUrlInput'),
        sourceDbNameInput: $('#sourceDbNameInput'),
        targetDbNameInput: $('#targetDbNameInput'),
        sourceDbNameGroup: $('#sourceDbNameGroup'),
        targetDbNameGroup: $('#targetDbNameGroup'),
        sourceTypeIndicator: $('#sourceTypeIndicator'),
        targetTypeIndicator: $('#targetTypeIndicator'),
        connectDualBtn: $('#connectDualBtn'),
        connectingAnim: $('#connectingAnim'),

        // Schema
        schemaEmpty: $('#schemaEmpty'),
        schemaDiffSummary: $('#schemaDiffSummary'),
        dualSchemaGrid: $('#dualSchemaGrid'),
        sourceTablesList: $('#sourceTablesList'),
        targetTablesList: $('#targetTablesList'),
        schemaSourceDialect: $('#schemaSourceDialect'),
        schemaTargetDialect: $('#schemaTargetDialect'),
        schemaDetailView: $('#schemaDetailView'),
        diffSourceOnly: $('#diffSourceOnly'),
        diffBoth: $('#diffBoth'),
        diffTargetOnly: $('#diffTargetOnly'),

        // AI Query
        aiQueryInput: $('#aiQueryInput'),
        aiSendBtn: $('#aiSendBtn'),
        aiThinking: $('#aiThinking'),
        dualResultsArea: $('#dualResultsArea'),
        sourceQueryOutput: $('#sourceQueryOutput'),
        targetQueryOutput: $('#targetQueryOutput'),
        sourceQLBadge: $('#sourceQLBadge'),
        targetQLBadge: $('#targetQLBadge'),
        sourceResultMsg: $('#sourceResultMsg'),
        targetResultMsg: $('#targetResultMsg'),
        sourceResultError: $('#sourceResultError'),
        targetResultError: $('#targetResultError'),
        sourceDataTable: $('#sourceDataTable'),
        targetDataTable: $('#targetDataTable'),
        historyList: $('#historyList'),

        // Dashboard
        genDashboardBtn: $('#genDashboardBtn'),
        dashboardDbSelect: $('#dashboardDbSelect'),
        dashboardEmpty: $('#dashboardEmpty'),
        dashboardSkeleton: $('#dashboardSkeleton'),
        dashboardGrid: $('#dashboardGrid'),

        // Optimizer
        optimizerInput: $('#optimizerInput'),
        optimizeBtn: $('#optimizeBtn'),
        optimizerDbSelect: $('#optimizerDbSelect'),
        optimizerResults: $('#optimizerResults'),
        optimizerSkeleton: $('#optimizerSkeleton'),
        perfScoreValue: $('#perfScoreValue'),
        perfScoreFill: $('#perfScoreFill'),
        origQueryDisplay: $('#origQueryDisplay'),
        optQueryDisplay: $('#optQueryDisplay'),
        copyOptimizedBtn: $('#copyOptimizedBtn'),
        explanationContent: $('#explanationContent'),

        // Modal
        chartModal: $('#chartModal'),
        chartModalTitle: $('#chartModalTitle'),
        chartModalClose: $('#chartModalClose'),
        chartModalImg: $('#chartModalImg'),
        chartNewTabBtn: $('#chartNewTabBtn'),

        toastContainer: $('#toastContainer'),

        // Migration
        migrationSteps: $('#migrationSteps'),
        migStep1: $('#migStep1'),
        migStep2: $('#migStep2'),
        migStep3: $('#migStep3'),
        migStep4: $('#migStep4'),
        migStep5: $('#migStep5'),
        pipelineGrid: $('#pipelineGrid'),
        migSourceType: $('#migSourceType'),
        migTargetType: $('#migTargetType'),
        migSourceFields: $('#migSourceFields'),
        migTargetFields: $('#migTargetFields'),
        migTestSourceBtn: $('#migTestSourceBtn'),
        migTestTargetBtn: $('#migTestTargetBtn'),
        migSourceTestResult: $('#migSourceTestResult'),
        migTargetTestResult: $('#migTargetTestResult'),
        migExtractSchemaBtn: $('#migExtractSchemaBtn'),
        migSchemaLoading: $('#migSchemaLoading'),
        migSchemaDisplay: $('#migSchemaDisplay'),
        migGenPlanBtn: $('#migGenPlanBtn'),
        migPlanLoading: $('#migPlanLoading'),
        migBackTo1: $('#migBackTo1'),
        migBackTo2: $('#migBackTo2'),
        migBackTo3: $('#migBackTo3'),
        migPlanOutput: $('#migPlanOutput'),
        migFeedbackInput: $('#migFeedbackInput'),
        migUpdatePlanBtn: $('#migUpdatePlanBtn'),
        migApproveBtn: $('#migApproveBtn'),
        migCancelBtn: $('#migCancelBtn'),
        migProgressText: $('#migProgressText'),
        migProgressBar: $('#migProgressBar'),
        migProgressDetail: $('#migProgressDetail'),
        migProgressArea: $('#migProgressArea'),
        migResultArea: $('#migResultArea'),
        migResultIcon: $('#migResultIcon'),
        migResultTitle: $('#migResultTitle'),
        migResultDetails: $('#migResultDetails'),
        migLaunchSqlaiBtn: $('#migLaunchSqlaiBtn'),
        migNewMigrationBtn: $('#migNewMigrationBtn'),
    };

    // ===== TOAST =====
    function showToast(message, type = 'info') {
        const icons = { success: '✅', error: '❌', warning: '⚠️', info: 'ℹ️' };
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `<span class="toast-icon">${icons[type]}</span><span>${escapeHtml(message)}</span>`;
        toast.addEventListener('click', () => removeToast(toast));
        dom.toastContainer.appendChild(toast);
        setTimeout(() => removeToast(toast), TOAST_DURATION);
    }

    function removeToast(toast) {
        if (!toast.parentNode) return;
        toast.classList.add('toast-out');
        setTimeout(() => toast.remove(), 200);
    }

    // ===== UTILITY =====
    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function detectDialect(url) {
        const u = url.toLowerCase();
        if (u.includes('postgres')) return 'postgresql';
        if (u.includes('mysql')) return 'mysql';
        if (u.includes('mongo')) return 'mongodb';
        if (u.includes('couch') || u.includes(':5984')) return 'couchdb';
        if (u.includes('oracle')) return 'oracle';
        return 'sql';
    }

    function isNoSQL(dialect) {
        return dialect === 'mongodb' || dialect === 'couchdb';
    }

    function getDbIcon(dialect) {
        const icons = {
            postgresql: '🐘', mysql: '🐬', mongodb: '🍃', couchdb: '🛋️', oracle: '🔶', sql: '🗄️'
        };
        return icons[dialect] || '🗄️';
    }

    function getEntityLabel(dialect) {
        if (dialect === 'mongodb') return 'Collections';
        if (dialect === 'couchdb') return 'Databases';
        return 'Tables';
    }

    function copyToClipboard(text) {
        navigator.clipboard.writeText(text).then(() => {
            showToast('Copied to clipboard!', 'success');
        }).catch(() => showToast('Failed to copy', 'error'));
    }

    function formatTime(date) {
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }

    async function apiCall(endpoint, body, method = 'POST') {
        const opts = {
            method,
            headers: { 'Content-Type': 'application/json' },
        };
        if (method !== 'GET') opts.body = JSON.stringify(body);
        const res = await fetch(`${API_BASE}${endpoint}`, opts);
        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: 'Request failed' }));
            throw new Error(err.detail || `HTTP ${res.status}`);
        }
        return res.json();
    }

    // ===== NAVIGATION =====
    const pageTitles = {
        migration: 'Data Migration',
        connect: 'Connect Databases',
        schema: 'Schema Explorer',
        query: 'AI Query',
        dashboard: 'Auto Dashboard',
        optimizer: 'SQL Optimizer',
    };

    function navigateTo(page) {
        state.currentPage = page;
        $$('.nav-item').forEach(item => item.classList.toggle('active', item.dataset.page === page));
        $$('.page').forEach(p => p.classList.toggle('active', p.id === `page-${page}`));
        dom.headerTitle.textContent = pageTitles[page] || page;
        dom.sidebar.classList.remove('mobile-open');
        dom.mobileOverlay.classList.remove('show');
    }

    // ===== SIDEBAR =====
    function toggleSidebar() {
        state.sidebarCollapsed = !state.sidebarCollapsed;
        dom.sidebar.classList.toggle('collapsed', state.sidebarCollapsed);
        dom.toggleIcon.textContent = state.sidebarCollapsed ? '▶' : '◀';
    }

    // ===== THEME =====
    function toggleTheme() {
        const html = document.documentElement;
        const next = html.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        html.setAttribute('data-theme', next);
        dom.themeToggle.textContent = next === 'dark' ? '🌙' : '☀️';
        localStorage.setItem('theme', next);
    }

    function loadTheme() {
        const saved = localStorage.getItem('theme') || 'dark';
        document.documentElement.setAttribute('data-theme', saved);
        dom.themeToggle.textContent = saved === 'dark' ? '🌙' : '☀️';
    }

    // ===== SAFE MODE =====
    function toggleSafeMode() {
        state.safeMode = !state.safeMode;
        dom.safeModeToggle.classList.toggle('active', state.safeMode);
        dom.safeModeToggle.setAttribute('aria-checked', state.safeMode);
        showToast(state.safeMode ? 'Safe Mode ON — Read-only queries' : 'Safe Mode OFF — All queries allowed', state.safeMode ? 'success' : 'warning');
    }

    // ===== CONNECTION STATUS =====
    function updateConnectionStatus(connected) {
        state.connected = connected;
        dom.statusChip.className = `status-chip ${connected ? 'connected' : 'disconnected'}`;
        dom.statusText.textContent = connected ? 'Connected' : 'Disconnected';

        if (connected) {
            dom.sourceDialectBadge.textContent = `${getDbIcon(state.sourceDialect)} ${state.sourceDialect.toUpperCase()}`;
            dom.sourceDialectBadge.className = 'dual-badge source';
            dom.sourceDialectBadge.classList.remove('hidden');

            dom.targetDialectBadge.textContent = `${getDbIcon(state.targetDialect)} ${state.targetDialect.toUpperCase()}`;
            dom.targetDialectBadge.className = 'dual-badge target';
            dom.targetDialectBadge.classList.remove('hidden');
        } else {
            dom.sourceDialectBadge.classList.add('hidden');
            dom.targetDialectBadge.classList.add('hidden');
        }
    }


    // ═══════════════════════════════════════════════════════════════════════════
    // MIGRATION PIPELINE UI
    // ═══════════════════════════════════════════════════════════════════════════

    function setMigStep(stepNum) {
        $$('.mig-step').forEach(s => {
            const n = parseInt(s.dataset.step);
            s.classList.toggle('active', n === stepNum);
            s.classList.toggle('done', n < stepNum);
        });
        dom.migStep1.style.display = stepNum === 1 ? 'block' : 'none';
        dom.migStep2.style.display = stepNum === 2 ? 'block' : 'none';
        dom.migStep3.style.display = stepNum === 3 ? 'block' : 'none';
        dom.migStep4.style.display = stepNum === 4 ? 'block' : 'none';
        if (dom.migStep5) dom.migStep5.style.display = stepNum === 5 ? 'block' : 'none';
    }

    async function loadPipelines() {
        try {
            const data = await apiCall('/api/pipelines', null, 'GET');
            state.pipelines = data.pipelines || [];
            renderPipelineCards();
        } catch (err) {
            dom.pipelineGrid.innerHTML = `<div style="color:var(--text-secondary);text-align:center;padding:24px;">Failed to load pipelines: ${escapeHtml(err.message)}</div>`;
        }
    }

    function renderPipelineCards() {
        dom.pipelineGrid.innerHTML = state.pipelines.map(p => {
            const srcIcon = getDbIcon(p.source_type);
            const tgtIcon = getDbIcon(p.target_type);
            return `
                <div class="pipeline-card" data-pid="${p.id}" tabindex="0">
                    <div class="pipeline-card-icons">
                        <span class="pipeline-db-icon">${srcIcon}</span>
                        <span class="pipeline-arrow">→</span>
                        <span class="pipeline-db-icon">${tgtIcon}</span>
                    </div>
                    <div class="pipeline-card-label">${p.source_type.toUpperCase()} → ${p.target_type.toUpperCase()}</div>
                </div>
            `;
        }).join('');
    }

    function selectPipeline(pipelineId) {
        state.selectedPipeline = state.pipelines.find(p => p.id === pipelineId);
        if (!state.selectedPipeline) return;

        const p = state.selectedPipeline;
        dom.migSourceType.textContent = `${getDbIcon(p.source_type)} ${p.source_type.toUpperCase()}`;
        dom.migTargetType.textContent = `${getDbIcon(p.target_type)} ${p.target_type.toUpperCase()}`;

        // Generate input fields
        dom.migSourceFields.innerHTML = generateDbConfigFields(p.source_type, 'source');
        dom.migTargetFields.innerHTML = generateDbConfigFields(p.target_type, 'target');

        dom.migSourceTestResult.innerHTML = '';
        dom.migTargetTestResult.innerHTML = '';

        setMigStep(2);
        showToast(`Selected: ${p.source_type.toUpperCase()} → ${p.target_type.toUpperCase()}`, 'info');
    }

    function generateDbConfigFields(dbType, role) {
        if (dbType === 'mysql' || dbType === 'postgresql') {
            const defaultUrl = dbType === 'mysql'
                ? 'mysql+pymysql://user1:pass123@localhost:3310/testdb'
                : '';
            return `
                <div class="input-group">
                    <label>Connection URL</label>
                    <input type="text" class="input-field mono" id="mig_${role}_url"
                        placeholder="${dbType}://user:pass@host:port/dbname"
                        value="${defaultUrl}" autocomplete="off" spellcheck="false">
                </div>
            `;
        } else if (dbType === 'mongodb') {
            return `
                <div class="input-group">
                    <label>MongoDB URL</label>
                    <input type="text" class="input-field mono" id="mig_${role}_url"
                        placeholder="mongodb+srv://user:pass@cluster.mongodb.net/"
                        autocomplete="off" spellcheck="false">
                </div>
                <div class="input-group mt-md">
                    <label>Database Name</label>
                    <input type="text" class="input-field" id="mig_${role}_database"
                        placeholder="e.g., migrated_db" autocomplete="off">
                </div>
            `;
        } else if (dbType === 'couchdb') {
            return `
                <div class="input-group">
                    <label>CouchDB Host</label>
                    <input type="text" class="input-field mono" id="mig_${role}_host"
                        value="http://localhost:5984" autocomplete="off">
                </div>
                <div class="input-group mt-md">
                    <label>Username</label>
                    <input type="text" class="input-field" id="mig_${role}_username"
                        value="admin" autocomplete="off">
                </div>
                <div class="input-group mt-md">
                    <label>Password</label>
                    <input type="password" class="input-field" id="mig_${role}_password"
                        value="admin123" autocomplete="off">
                </div>
            `;
        }
        return '<p style="color:var(--text-tertiary);">Unknown database type</p>';
    }

    function collectDbConfig(dbType, role) {
        const config = {};
        if (dbType === 'mysql' || dbType === 'postgresql') {
            config.connection_url = ($(`#mig_${role}_url`) || {}).value || '';
        } else if (dbType === 'mongodb') {
            config.connection_url = ($(`#mig_${role}_url`) || {}).value || '';
            config.database = ($(`#mig_${role}_database`) || {}).value || '';
        } else if (dbType === 'couchdb') {
            config.host = ($(`#mig_${role}_host`) || {}).value || 'http://localhost:5984';
            config.username = ($(`#mig_${role}_username`) || {}).value || 'admin';
            config.password = ($(`#mig_${role}_password`) || {}).value || 'admin123';
        }
        return config;
    }

    async function testMigConnection(role) {
        const p = state.selectedPipeline;
        if (!p) return;
        const dbType = role === 'source' ? p.source_type : p.target_type;
        const config = collectDbConfig(dbType, role);
        const resultEl = role === 'source' ? dom.migSourceTestResult : dom.migTargetTestResult;

        resultEl.innerHTML = '<span style="color:var(--text-tertiary);">Testing…</span>';
        try {
            const data = await apiCall('/api/test-connection', { db_type: dbType, ...config });
            if (data.success) {
                resultEl.innerHTML = `<span style="color:var(--success);">✅ ${escapeHtml(data.message)}</span>`;
            } else {
                resultEl.innerHTML = `<span style="color:var(--error);">❌ ${escapeHtml(data.message)}</span>`;
            }
        } catch (err) {
            resultEl.innerHTML = `<span style="color:var(--error);">❌ ${escapeHtml(err.message)}</span>`;
        }
    }

    async function extractSchemaOnly() {
        const p = state.selectedPipeline;
        if (!p) return;

        const sourceConfig = collectDbConfig(p.source_type, 'source');
        const targetConfig = collectDbConfig(p.target_type, 'target');
        state.migSourceConfig = sourceConfig;
        state.migTargetConfig = targetConfig;

        dom.migExtractSchemaBtn.disabled = true;
        dom.migSchemaLoading.classList.add('show');

        try {
            const schemaData = await apiCall('/api/extract-schema', {
                db_type: p.source_type, ...sourceConfig,
            });
            showToast(`Schema extracted! ${schemaData.table_count} entities found.`, 'success');

            // Store extracted schema for plan generation
            state.migExtractedSchema = schemaData.schema;
            state.migSchemaSessionId = schemaData.session_id;

            // Render schema in Step 3
            renderMigrationSchema(schemaData.schema, p.source_type);

            dom.migSchemaLoading.classList.remove('show');
            dom.migExtractSchemaBtn.disabled = false;

            setMigStep(3);
        } catch (err) {
            dom.migSchemaLoading.classList.remove('show');
            dom.migExtractSchemaBtn.disabled = false;
            showToast(`Error: ${err.message}`, 'error');
        }
    }

    function renderMigrationSchema(schema, dbType) {
        if (!schema || !Object.keys(schema).length) {
            dom.migSchemaDisplay.innerHTML = '<div class="empty-state" style="padding:20px;"><p>No schema data found.</p></div>';
            return;
        }

        const isSql = (dbType === 'mysql' || dbType === 'postgresql');
        let html = '';

        for (const [name, info] of Object.entries(schema)) {
            html += `<div class="card card-flat" style="padding:16px;margin-bottom:12px;border:1px solid var(--border-primary);">`;
            html += `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">`;
            html += `<h4 style="font-weight:700;font-size:14px;margin:0;">${isSql ? '\ud83d\udccb' : '\ud83d\udcc2'} ${escapeHtml(name)}</h4>`;

            // Row/doc count
            const count = info.row_count || info.document_count || info.doc_count || 0;
            if (count) {
                html += `<span style="font-size:12px;color:var(--text-secondary);">${count.toLocaleString()} ${isSql ? 'rows' : 'documents'}</span>`;
            }
            html += `</div>`;

            // Primary Keys
            const pks = info.primary_keys || info.primary_key || [];
            const pkList = Array.isArray(pks) ? pks : (pks ? [pks] : []);
            if (pkList.length) {
                html += `<div style="margin-bottom:6px;"><span style="font-size:11px;font-weight:600;color:var(--accent-primary);">\ud83d\udd11 Primary Key:</span> <code style="font-size:12px;background:var(--bg-tertiary);padding:2px 6px;border-radius:4px;">${pkList.map(k => escapeHtml(k)).join(', ')}</code></div>`;
            }

            // Foreign Keys  
            const fks = info.foreign_keys || [];
            if (fks.length) {
                html += `<div style="margin-bottom:6px;"><span style="font-size:11px;font-weight:600;color:#e879f9;">\ud83d\udd17 Foreign Keys:</span></div>`;
                html += `<div style="padding-left:12px;margin-bottom:6px;">`;
                fks.forEach(fk => {
                    const srcCols = (fk.constrained_columns || []).join(', ');
                    const refTable = fk.referred_table || '?';
                    const refCols = (fk.referred_columns || []).join(', ');
                    html += `<div style="font-size:12px;color:var(--text-secondary);margin-bottom:2px;"><code>${escapeHtml(srcCols)}</code> \u2192 <code>${escapeHtml(refTable)}(${escapeHtml(refCols)})</code></div>`;
                });
                html += `</div>`;
            }

            // Columns / Fields
            const columns = info.columns || [];
            const fields = info.fields || {};
            if (columns.length) {
                html += `<div style="margin-bottom:6px;"><span style="font-size:11px;font-weight:600;color:var(--text-secondary);">${isSql ? 'Columns' : 'Fields'}:</span></div>`;
                html += `<div style="display:flex;flex-wrap:wrap;gap:4px;">`;
                columns.forEach(col => {
                    const colName = col.name || col;
                    const colType = col.type || '';
                    const isPk = pkList.includes(colName);
                    const bgColor = isPk ? 'rgba(250,204,21,0.15)' : 'var(--bg-tertiary)';
                    const borderColor = isPk ? 'rgba(250,204,21,0.4)' : 'transparent';
                    html += `<span style="font-size:11px;padding:3px 8px;border-radius:4px;background:${bgColor};border:1px solid ${borderColor};font-family:var(--font-mono);">${escapeHtml(colName)}${colType ? ' <span style=color:var(--text-tertiary)>(' + escapeHtml(colType) + ')</span>' : ''}</span>`;
                });
                html += `</div>`;
            } else if (typeof fields === 'object' && !Array.isArray(fields)) {
                html += `<div style="margin-bottom:6px;"><span style="font-size:11px;font-weight:600;color:var(--text-secondary);">Fields:</span></div>`;
                html += `<div style="display:flex;flex-wrap:wrap;gap:4px;">`;
                for (const [fieldName, fieldTypes] of Object.entries(fields)) {
                    const typeStr = Array.isArray(fieldTypes) ? fieldTypes.join('/') : String(fieldTypes);
                    html += `<span style="font-size:11px;padding:3px 8px;border-radius:4px;background:var(--bg-tertiary);font-family:var(--font-mono);">${escapeHtml(fieldName)} <span style="color:var(--text-tertiary)">(${escapeHtml(typeStr)})</span></span>`;
                }
                html += `</div>`;
            }

            // Indexes
            const indexes = info.indexes || [];
            if (indexes.length) {
                html += `<div style="margin-top:6px;"><span style="font-size:11px;font-weight:600;color:#60a5fa;">\ud83d\uddc2\ufe0f Indexes:</span></div>`;
                html += `<div style="padding-left:12px;">`;
                indexes.forEach(idx => {
                    const idxCols = (idx.columns || idx.column_names || []).join(', ');
                    const uniqueStr = idx.unique ? ' (UNIQUE)' : '';
                    html += `<div style="font-size:11px;color:var(--text-secondary);margin-bottom:2px;"><code>${escapeHtml(idx.name || 'unnamed')}</code>: ${escapeHtml(idxCols)}${uniqueStr}</div>`;
                });
                html += `</div>`;
            }

            html += `</div>`;
        }

        dom.migSchemaDisplay.innerHTML = html;
    }

    async function generateMigPlan() {
        const p = state.selectedPipeline;
        if (!p || !state.migExtractedSchema) return;

        if (dom.migGenPlanBtn) dom.migGenPlanBtn.disabled = true;
        if (dom.migPlanLoading) dom.migPlanLoading.classList.add('show');

        try {
            const planData = await apiCall('/api/generate-plan', {
                source_type: p.source_type,
                target_type: p.target_type,
                schema_data: state.migExtractedSchema,
            });

            state.migSessionId = planData.session_id;
            state.migPlan = planData.plan;

            dom.migPlanOutput.textContent = JSON.stringify(planData.plan, null, 2);

            if (dom.migPlanLoading) dom.migPlanLoading.classList.remove('show');
            if (dom.migGenPlanBtn) dom.migGenPlanBtn.disabled = false;

            setMigStep(4);
            showToast('AI migration plan generated! Review it below.', 'success');
        } catch (err) {
            if (dom.migPlanLoading) dom.migPlanLoading.classList.remove('show');
            if (dom.migGenPlanBtn) dom.migGenPlanBtn.disabled = false;
            showToast(`Error: ${err.message}`, 'error');
        }
    }

    async function updateMigPlan() {
        const feedback = dom.migFeedbackInput.value.trim();
        if (!feedback) {
            showToast('Enter feedback first', 'warning');
            return;
        }
        if (!state.migSessionId) return;

        dom.migUpdatePlanBtn.disabled = true;
        try {
            const data = await apiCall('/api/update-plan', {
                session_id: state.migSessionId,
                feedback: feedback,
            });
            state.migPlan = data.plan;
            dom.migPlanOutput.textContent = JSON.stringify(data.plan, null, 2);
            dom.migFeedbackInput.value = '';
            showToast('Plan updated based on your feedback!', 'success');
        } catch (err) {
            showToast(`Update failed: ${err.message}`, 'error');
        }
        dom.migUpdatePlanBtn.disabled = false;
    }

    async function approveAndExecute() {
        if (!state.migSessionId) return;

        try {
            // Approve
            await apiCall('/api/approve-plan', { session_id: state.migSessionId });

            // Execute
            const execData = await apiCall('/api/execute-migration', {
                session_id: state.migSessionId,
                source_config: state.migSourceConfig,
                target_config: state.migTargetConfig,
            });

            setMigStep(5);
            dom.migProgressArea.style.display = 'block';
            dom.migResultArea.style.display = 'none';
            showToast('Migration started!', 'success');

            // Poll for progress
            pollMigrationStatus(state.migSessionId);
        } catch (err) {
            showToast(`Execution failed: ${err.message}`, 'error');
        }
    }

    function pollMigrationStatus(sessionId) {
        const intervalId = setInterval(async () => {
            try {
                const data = await apiCall(`/api/migration-status/${sessionId}`, null, 'GET');

                if (data.status === 'running') {
                    const prog = data.progress || {};
                    const current = prog.current || 0;
                    const total = prog.total || 1;
                    const pct = total > 0 ? Math.round((current / total) * 100) : 0;
                    dom.migProgressBar.style.width = `${pct}%`;
                    dom.migProgressText.textContent = `Migrating… ${current} / ${total} tables`;
                    dom.migProgressDetail.textContent = prog.current_table ? `Current: ${prog.current_table}` : '';
                } else if (data.status === 'completed') {
                    clearInterval(intervalId);
                    showMigrationResult(data, true);
                } else if (data.status === 'failed') {
                    clearInterval(intervalId);
                    showMigrationResult(data, false);
                }
            } catch (err) {
                clearInterval(intervalId);
                showToast(`Status check failed: ${err.message}`, 'error');
            }
        }, 1500);
    }

    function showMigrationResult(data, success) {
        dom.migProgressArea.style.display = 'none';
        dom.migResultArea.style.display = 'block';

        if (success) {
            dom.migResultIcon.textContent = '✅';
            dom.migResultTitle.textContent = 'Migration Completed Successfully!';
            const result = data.result || {};
            const tables = result.tables_migrated || [];
            const totalRows = result.total_rows || 0;
            const errors = result.errors || [];

            let html = `
                <div style="display:flex;gap:24px;justify-content:center;margin-bottom:16px;">
                    <div style="text-align:center;"><div style="font-size:28px;font-weight:800;color:var(--accent-primary);">${tables.length}</div><div style="font-size:12px;color:var(--text-secondary);">Tables Migrated</div></div>
                    <div style="text-align:center;"><div style="font-size:28px;font-weight:800;color:var(--accent-primary);">${totalRows.toLocaleString()}</div><div style="font-size:12px;color:var(--text-secondary);">Total Rows</div></div>
                </div>
            `;
            if (tables.length) {
                html += `<div class="data-table-wrapper" style="max-height:200px;overflow:auto;"><table class="data-table"><thead><tr><th>Source</th><th>Target</th><th>Rows</th></tr></thead><tbody>`;
                tables.forEach(t => {
                    html += `<tr><td>${escapeHtml(t.source)}</td><td>${escapeHtml(t.target)}</td><td>${(t.rows || 0).toLocaleString()}</td></tr>`;
                });
                html += `</tbody></table></div>`;
            }
            if (errors.length) {
                html += `<div style="margin-top:12px;color:var(--error);"><strong>Errors:</strong><ul>`;
                errors.forEach(e => { html += `<li>${escapeHtml(e.table)}: ${escapeHtml(e.error)}</li>`; });
                html += `</ul></div>`;
            }
            dom.migResultDetails.innerHTML = html;

            // Store connection details for SQLAI launch
            state._migSourceType = data.source_type;
            state._migTargetType = data.target_type;
            state._migSourceConfig = data.source_config;
            state._migTargetConfig = data.target_config;

            showToast('Migration complete! 🎉', 'success');
        } else {
            dom.migResultIcon.textContent = '❌';
            dom.migResultTitle.textContent = 'Migration Failed';
            dom.migResultDetails.innerHTML = `<div style="color:var(--error);padding:16px;background:var(--bg-tertiary);border-radius:var(--radius-md);">${escapeHtml(data.error || 'Unknown error')}</div>`;
            showToast('Migration failed!', 'error');
        }
    }

    function buildSqlaiUrl(dbType, config) {
        if (dbType === 'mysql' || dbType === 'postgresql') {
            return config.connection_url || '';
        } else if (dbType === 'mongodb') {
            let url = config.connection_url || '';
            const db = config.database || '';
            if (db && !url.includes(db)) {
                if (url.includes('?')) {
                    const [base, qs] = url.split('?', 2);
                    return `${base.replace(/\/$/, '')}/${db}?${qs}`;
                }
                return `${url.replace(/\/$/, '')}/${db}`;
            }
            return url;
        } else if (dbType === 'couchdb') {
            const host = config.host || 'http://localhost:5984';
            const user = config.username || 'admin';
            const pw = config.password || 'admin';
            try {
                const parsed = new URL(host);
                return `${parsed.protocol}//${user}:${pw}@${parsed.hostname}:${parsed.port || 5984}`;
            } catch {
                return `http://${user}:${pw}@localhost:5984`;
            }
        }
        return '';
    }

    function launchSqlaiAfterMigration() {
        const srcType = state._migSourceType;
        const tgtType = state._migTargetType;
        const srcConfig = state._migSourceConfig || {};
        const tgtConfig = state._migTargetConfig || {};

        const sourceUrl = buildSqlaiUrl(srcType, srcConfig);
        const targetUrl = buildSqlaiUrl(tgtType, tgtConfig);

        if (dom.sourceUrlInput) dom.sourceUrlInput.value = sourceUrl;
        if (dom.targetUrlInput) dom.targetUrlInput.value = targetUrl;

        // Handle db names for NoSQL
        const sourceDbName = srcConfig.database || '';
        const targetDbName = tgtConfig.database || '';
        if (sourceDbName && dom.sourceDbNameInput) {
            dom.sourceDbNameInput.value = sourceDbName;
            if (dom.sourceDbNameGroup) dom.sourceDbNameGroup.style.display = 'block';
        }
        if (targetDbName && dom.targetDbNameInput) {
            dom.targetDbNameInput.value = targetDbName;
            if (dom.targetDbNameGroup) dom.targetDbNameGroup.style.display = 'block';
        }

        // Trigger type detection
        if (dom.sourceUrlInput) dom.sourceUrlInput.dispatchEvent(new Event('input'));
        if (dom.targetUrlInput) dom.targetUrlInput.dispatchEvent(new Event('input'));

        // Navigate to Connect page and auto-connect
        navigateTo('connect');
        setTimeout(() => handleDualConnect(), 300);
    }

    function resetMigration() {
        state.selectedPipeline = null;
        state.migSessionId = null;
        state.migSchemaSessionId = null;
        state.migExtractedSchema = null;
        state.migPlan = null;
        state.migSourceConfig = {};
        state.migTargetConfig = {};
        setMigStep(1);
        loadPipelines();
    }


    // ═══════════════════════════════════════════════════════════════════════════
    // SQLAI — DUAL-DB CONNECT / SCHEMA / QUERY / DASHBOARD / OPTIMIZER
    // ═══════════════════════════════════════════════════════════════════════════

    // ===== URL TYPE DETECTION (live) =====
    function updateTypeIndicator(inputEl, indicatorEl, dbNameGroupEl) {
        const url = inputEl.value.trim();
        const dialect = detectDialect(url);
        indicatorEl.textContent = dialect !== 'sql' ? `${getDbIcon(dialect)} ${dialect.toUpperCase()}` : '—';
        if (isNoSQL(dialect) && url.length > 5) {
            dbNameGroupEl.style.display = 'block';
        } else {
            dbNameGroupEl.style.display = 'none';
        }
    }

    // ===== DUAL CONNECT =====
    async function handleDualConnect() {
        const sourceUrl = dom.sourceUrlInput.value.trim();
        const targetUrl = dom.targetUrlInput.value.trim();

        if (!sourceUrl || !targetUrl) {
            showToast('Please enter both source and target connection URLs', 'warning');
            return;
        }

        state.sourceUrl = sourceUrl;
        state.targetUrl = targetUrl;
        state.sourceDbName = dom.sourceDbNameInput.value.trim();
        state.targetDbName = dom.targetDbNameInput.value.trim();
        state.sourceDialect = detectDialect(sourceUrl);
        state.targetDialect = detectDialect(targetUrl);

        dom.connectDualBtn.disabled = true;
        dom.connectingAnim.classList.add('show');

        try {
            const data = await apiCall('/connect-dual', {
                source_url: sourceUrl,
                target_url: targetUrl,
                source_db_name: state.sourceDbName || null,
                target_db_name: state.targetDbName || null,
            });

            state.sourceDialect = data.source_dialect;
            state.targetDialect = data.target_dialect;
            state.sourceTables = data.source_tables || [];
            state.targetTables = data.target_tables || [];
            state.sourceSchema = data.source_schema;
            state.targetSchema = data.target_schema;
            state.diff = data.diff;

            dom.connectingAnim.classList.remove('show');
            dom.connectDualBtn.disabled = false;

            updateConnectionStatus(true);
            showToast(`Connected! Source: ${state.sourceTables.length} ${getEntityLabel(state.sourceDialect).toLowerCase()}, Target: ${state.targetTables.length} ${getEntityLabel(state.targetDialect).toLowerCase()}`, 'success');

            loadDualSchemaView();
            navigateTo('schema');
        } catch (err) {
            dom.connectingAnim.classList.remove('show');
            dom.connectDualBtn.disabled = false;
            updateConnectionStatus(false);
            showToast(`Connection failed: ${err.message}`, 'error');
        }
    }

    // ===== SCHEMA EXPLORER - DUAL =====
    function loadDualSchemaView() {
        dom.schemaEmpty.classList.add('hidden');
        dom.schemaDiffSummary.classList.remove('hidden');
        dom.dualSchemaGrid.classList.remove('hidden');
        dom.schemaDetailView.classList.remove('hidden');

        const diff = state.diff || {};
        dom.diffSourceOnly.textContent = (diff.only_in_source || []).length;
        dom.diffBoth.textContent = (diff.in_both || []).length;
        dom.diffTargetOnly.textContent = (diff.only_in_target || []).length;

        dom.schemaSourceDialect.textContent = state.sourceDialect.toUpperCase();
        dom.schemaTargetDialect.textContent = state.targetDialect.toUpperCase();

        renderSchemaList(dom.sourceTablesList, state.sourceTables, diff, 'source', state.sourceDialect);
        renderSchemaList(dom.targetTablesList, state.targetTables, diff, 'target', state.targetDialect);

        dom.schemaDetailView.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">👆</div>
                <h3>Select a Table or Collection</h3>
                <p>Click any item from the lists above to view its schema details and preview data.</p>
            </div>
        `;
    }

    function renderSchemaList(container, tables, diff, role, dialect) {
        const onlyInSource = new Set(diff.only_in_source || []);
        const onlyInTarget = new Set(diff.only_in_target || []);
        const inBoth = new Set(diff.in_both || []);

        const icon = dialect === 'mongodb' ? '📂' : dialect === 'couchdb' ? '📄' : '📋';

        container.innerHTML = tables.map(name => {
            let diffClass = '';
            let dotClass = '';
            if (role === 'source' && onlyInSource.has(name)) {
                diffClass = 'source-only';
                dotClass = 'source-only';
            } else if (role === 'target' && onlyInTarget.has(name)) {
                diffClass = 'target-only';
                dotClass = 'target-only';
            } else if (inBoth.has(name)) {
                diffClass = 'in-both';
                dotClass = 'in-both';
            }

            return `
                <div class="schema-table-item ${diffClass}" data-table="${escapeHtml(name)}" data-role="${role}" tabindex="0">
                    <span class="diff-dot ${dotClass}"></span>
                    <span class="table-icon">${icon}</span>
                    <span>${escapeHtml(name)}</span>
                </div>
            `;
        }).join('');
    }

    async function loadTableDetail(tableName, role) {
        const dbUrl = role === 'source' ? state.sourceUrl : state.targetUrl;
        const dialect = role === 'source' ? state.sourceDialect : state.targetDialect;
        const dbName = role === 'source' ? state.sourceDbName : state.targetDbName;
        const roleBadge = role === 'source' ? '<span class="db-role-badge source">SOURCE</span>' : '<span class="db-role-badge target">DESTINATION</span>';

        $$('.schema-table-item').forEach(item => {
            item.classList.toggle('active', item.dataset.table === tableName && item.dataset.role === role);
        });

        dom.schemaDetailView.innerHTML = `
            <div class="skeleton skeleton-text" style="width:50%;margin-bottom:16px"></div>
            <div class="skeleton-row"><div class="skeleton"></div><div class="skeleton"></div></div>
            <div class="skeleton skeleton-block"></div>
        `;

        try {
            const details = await apiCall('/table-details-dual', { db_url: dbUrl, db_name: tableName });
            const pageData = await apiCall('/table-data-dual', { db_url: dbUrl, db_name: tableName, page: 1, limit: 20 });

            const columns = details.columns || [];
            const totalRows = pageData.total_rows || details.row_count || 0;
            const entityType = isNoSQL(dialect) ? (dialect === 'mongodb' ? 'Collection' : 'Database') : 'Table';

            // Primary keys
            const pks = details.primary_keys || [];
            const pkSet = new Set(pks);

            // Build columns with PK highlighting
            const columnsHtml = columns.map(col => {
                const isPk = pkSet.has(col);
                const bgStyle = isPk ? 'background:rgba(250,204,21,0.15);border:1px solid rgba(250,204,21,0.4);' : '';
                const pkIcon = isPk ? '<span title="Primary Key" style="margin-right:2px;">🔑</span>' : '';
                return `<span class="meta-chip" style="${bgStyle}">${pkIcon}<code style="font-family:var(--font-mono);font-size:12px;">${escapeHtml(col)}</code></span>`;
            }).join('');

            // Primary keys section
            let pkHtml = '';
            if (pks.length) {
                pkHtml = `<div class="schema-section"><h4>🔑 Primary Key</h4><div style="display:flex;flex-wrap:wrap;gap:6px;">${pks.map(pk => `<span class="meta-chip" style="background:rgba(250,204,21,0.15);border:1px solid rgba(250,204,21,0.4);"><code style="font-family:var(--font-mono);font-size:12px;">${escapeHtml(pk)}</code></span>`).join('')}</div></div>`;
            }

            // Foreign keys section
            let fkHtml = '';
            const fks = details.foreign_keys || [];
            if (fks.length) {
                const fkItems = fks.map(fk => {
                    const srcCols = (fk.constrained_columns || []).join(', ');
                    const refTable = fk.referred_table || '?';
                    const refCols = (fk.referred_columns || []).join(', ');
                    return `<div style="font-size:12px;padding:4px 8px;background:var(--bg-tertiary);border-radius:4px;margin-bottom:4px;"><code>${escapeHtml(srcCols)}</code> → <strong>${escapeHtml(refTable)}</strong>(<code>${escapeHtml(refCols)}</code>)</div>`;
                }).join('');
                fkHtml = `<div class="schema-section"><h4>🔗 Foreign Keys</h4>${fkItems}</div>`;
            }

            // Indexes section
            let idxHtml = '';
            const indexes = details.indexes || [];
            if (indexes.length) {
                const idxItems = indexes.map(idx => {
                    const idxCols = (idx.columns || idx.column_names || []).join(', ');
                    const uniqueStr = idx.unique ? ' <span style="color:var(--accent-primary);font-weight:600;">UNIQUE</span>' : '';
                    return `<div style="font-size:12px;padding:4px 8px;background:var(--bg-tertiary);border-radius:4px;margin-bottom:4px;"><code>${escapeHtml(idx.name || 'unnamed')}</code>: ${escapeHtml(idxCols)}${uniqueStr}</div>`;
                }).join('');
                idxHtml = `<div class="schema-section"><h4>📇 Indexes</h4>${idxItems}</div>`;
            }

            // NoSQL specific info
            let nosqlInfoHtml = '';
            if (isNoSQL(dialect)) {
                const idType = details.id_type || 'ObjectId';
                nosqlInfoHtml = `<div class="schema-section"><h4>🆔 Document Identifier</h4><div style="font-size:12px;"><code>_id</code> type: <strong>${escapeHtml(idType)}</strong> (auto-generated primary key)</div></div>`;
                if (details.unique_fields && details.unique_fields.length) {
                    nosqlInfoHtml += `<div style="margin-top:6px;font-size:12px;">Unique indexed fields: <code>${details.unique_fields.map(f => escapeHtml(f)).join(', ')}</code></div>`;
                }
            }

            dom.schemaDetailView.innerHTML = `
                <div class="schema-detail-header">
                    <h3>${roleBadge} ${escapeHtml(tableName)}</h3>
                    <span class="db-type-indicator">${getDbIcon(dialect)} ${dialect.toUpperCase()} ${entityType}</span>
                </div>
                <div class="schema-meta">
                    <span class="meta-chip">${isNoSQL(dialect) ? 'Documents' : 'Rows'}: <span class="meta-value">${totalRows.toLocaleString()}</span></span>
                    <span class="meta-chip">${isNoSQL(dialect) ? 'Fields' : 'Columns'}: <span class="meta-value">${columns.length}</span></span>
                </div>
                ${pkHtml}
                ${fkHtml}
                ${nosqlInfoHtml}
                ${idxHtml}
                <div class="schema-section">
                    <h4>${isNoSQL(dialect) ? 'Fields' : 'Columns'}</h4>
                    <div style="display:flex;flex-wrap:wrap;gap:6px;max-height:120px;overflow-y:auto;padding-bottom:10px;">${columnsHtml}</div>
                </div>
                <div class="schema-section">
                    <h4>Data Preview</h4>
                    <div id="detailTableContainer">
                        ${renderTable(pageData.data || details.first_10 || [], columns)}
                    </div>
                </div>
            `;

            dom.schemaDetailView.style.animation = 'none';
            dom.schemaDetailView.offsetHeight;
            dom.schemaDetailView.style.animation = 'pageIn var(--duration-slow) var(--ease-default)';
        } catch (err) {
            dom.schemaDetailView.innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">❌</div>
                    <h3>Error Loading Details</h3>
                    <p>${escapeHtml(err.message)}</p>
                </div>
            `;
            showToast(`Failed to load: ${err.message}`, 'error');
        }
    }

    function renderTable(rows, explicitColumns = null) {
        if (!rows || !rows.length) return `<div class="empty-state" style="padding:20px;"><p class="text-secondary">No data available</p></div>`;
        const cols = explicitColumns || Object.keys(rows[0]);
        return `
            <div class="data-table-wrapper" style="max-height:400px;overflow:auto;">
                <table class="data-table">
                    <thead><tr>${cols.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead>
                    <tbody>${rows.map(r => `<tr>${cols.map(c => `<td title="${escapeHtml(String(r[c] ?? ''))}">${escapeHtml(String(r[c] ?? 'NULL'))}</td>`).join('')}</tr>`).join('')}</tbody>
                </table>
            </div>
        `;
    }

    // ===== AI QUERY - DUAL =====
    async function handleDualQuery() {
        const query = dom.aiQueryInput.value.trim();
        if (!query) {
            showToast('Please enter a question', 'warning');
            dom.aiQueryInput.focus();
            return;
        }
        if (!state.connected) {
            showToast('Connect to databases first', 'warning');
            return;
        }

        dom.aiSendBtn.disabled = true;
        dom.aiThinking.classList.add('show');
        dom.dualResultsArea.classList.add('hidden');

        try {
            const data = await apiCall('/generate-dual', {
                source_url: state.sourceUrl,
                target_url: state.targetUrl,
                source_db_name: state.sourceDbName || null,
                target_db_name: state.targetDbName || null,
                query: query,
                safe_mode: state.safeMode,
            });

            dom.aiThinking.classList.remove('show');
            dom.aiSendBtn.disabled = false;

            if (data.error) {
                showToast(`Error: ${data.error}`, 'error');
                return;
            }

            renderDualResults(data);
            addToHistory(query);
            showToast('Queries generated and executed!', 'success');
        } catch (err) {
            dom.aiThinking.classList.remove('show');
            dom.aiSendBtn.disabled = false;
            showToast(`Query failed: ${err.message}`, 'error');
        }
    }

    function renderDualResults(data) {
        dom.dualResultsArea.classList.remove('hidden');

        const src = data.source_result;
        if (src) {
            dom.sourceQLBadge.textContent = src.query_language || 'SQL';
            typewriterEffect(dom.sourceQueryOutput, src.query_text || 'No query generated');

            dom.sourceResultMsg.textContent = src.message || '';
            if (src.error) {
                dom.sourceResultError.textContent = src.error;
                dom.sourceResultError.classList.remove('hidden');
            } else {
                dom.sourceResultError.classList.add('hidden');
            }

            if (src.data_preview && src.data_preview.length) {
                const cols = Object.keys(src.data_preview[0]);
                dom.sourceDataTable.innerHTML = `
                    <thead><tr>${cols.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead>
                    <tbody>${src.data_preview.map(r => `<tr>${cols.map(c => `<td title="${escapeHtml(String(r[c] ?? ''))}">${escapeHtml(String(r[c] ?? 'NULL'))}</td>`).join('')}</tr>`).join('')}</tbody>
                `;
            } else {
                dom.sourceDataTable.innerHTML = '<tbody><tr><td style="text-align:center;padding:24px;color:var(--text-tertiary);">No data</td></tr></tbody>';
            }
        }

        const tgt = data.target_result;
        if (tgt) {
            dom.targetQLBadge.textContent = tgt.query_language || 'Query';
            typewriterEffect(dom.targetQueryOutput, tgt.query_text || 'No query generated');

            dom.targetResultMsg.textContent = tgt.message || '';
            if (tgt.error) {
                dom.targetResultError.textContent = tgt.error;
                dom.targetResultError.classList.remove('hidden');
            } else {
                dom.targetResultError.classList.add('hidden');
            }

            if (tgt.data_preview && tgt.data_preview.length) {
                const cols = Object.keys(tgt.data_preview[0]);
                dom.targetDataTable.innerHTML = `
                    <thead><tr>${cols.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead>
                    <tbody>${tgt.data_preview.map(r => `<tr>${cols.map(c => `<td title="${escapeHtml(String(r[c] ?? ''))}">${escapeHtml(String(r[c] ?? 'NULL'))}</td>`).join('')}</tr>`).join('')}</tbody>
                `;
            } else {
                dom.targetDataTable.innerHTML = '<tbody><tr><td style="text-align:center;padding:24px;color:var(--text-tertiary);">No data</td></tr></tbody>';
            }
        }
    }

    function typewriterEffect(element, text) {
        element.textContent = '';
        element.classList.add('typewriter');
        let i = 0;
        const speed = Math.max(5, Math.min(30, 1500 / text.length));
        function type() {
            if (i < text.length) {
                element.textContent += text.charAt(i);
                i++;
                setTimeout(type, speed);
            } else {
                element.classList.remove('typewriter');
            }
        }
        type();
    }

    // ===== QUERY HISTORY =====
    function addToHistory(query) {
        state.queryHistory.unshift({ query, time: new Date() });
        if (state.queryHistory.length > 20) state.queryHistory.pop();
        renderHistory();
    }

    function renderHistory() {
        if (!state.queryHistory.length) return;
        dom.historyList.innerHTML = state.queryHistory.map((item, i) => `
            <div class="history-item" data-idx="${i}" tabindex="0">
                <span class="history-query">${escapeHtml(item.query)}</span>
                <span class="history-time">${formatTime(item.time)}</span>
            </div>
        `).join('');
    }

    // ===== DASHBOARD =====
    async function handleGenDashboard() {
        if (!state.connected) {
            showToast('Connect to databases first', 'warning');
            return;
        }

        const dbChoice = dom.dashboardDbSelect.value;
        const dbUrl = dbChoice === 'source' ? state.sourceUrl : state.targetUrl;
        const dialect = dbChoice === 'source' ? state.sourceDialect : state.targetDialect;

        if (isNoSQL(dialect)) {
            showToast('Dashboard generation is supported for SQL databases only.', 'warning');
            return;
        }

        dom.genDashboardBtn.disabled = true;
        dom.dashboardEmpty.classList.add('hidden');
        dom.dashboardGrid.classList.add('hidden');
        dom.dashboardSkeleton.classList.remove('hidden');

        try {
            const data = await apiCall('/gen-dashboard', { db_url: dbUrl });
            dom.dashboardSkeleton.classList.add('hidden');
            dom.genDashboardBtn.disabled = false;

            if (data.error) {
                showToast(`Dashboard error: ${data.error}`, 'error');
                dom.dashboardEmpty.classList.remove('hidden');
                return;
            }
            if (!data.charts || !data.charts.length) {
                dom.dashboardEmpty.classList.remove('hidden');
                showToast('No insights could be generated', 'warning');
                return;
            }

            renderDashboard(data.charts);
            showToast(`Dashboard: ${data.charts.length} insights generated!`, 'success');
        } catch (err) {
            dom.dashboardSkeleton.classList.add('hidden');
            dom.genDashboardBtn.disabled = false;
            dom.dashboardEmpty.classList.remove('hidden');
            showToast(`Dashboard failed: ${err.message}`, 'error');
        }
    }

    function renderDashboard(charts) {
        dom.dashboardGrid.classList.remove('hidden');
        dom.dashboardGrid.innerHTML = charts.map((chart, i) => `
            <div class="card dashboard-card animate-in" style="animation-delay:${i * 80}ms;" data-img="${chart.graph_base64}" data-title="${escapeHtml(chart.title)}">
                <div class="fullscreen-hint">🔍 Click to expand</div>
                <div class="card-content">
                    <h4>${escapeHtml(chart.title)}</h4>
                    <p>${escapeHtml(chart.description)}</p>
                    <img src="data:image/png;base64,${chart.graph_base64}" alt="${escapeHtml(chart.title)}" loading="lazy">
                </div>
            </div>
        `).join('');
    }

    // ===== OPTIMIZER =====
    async function handleOptimize() {
        const sql = dom.optimizerInput.value.trim();
        if (!sql) {
            showToast('Please enter a query to optimize', 'warning');
            dom.optimizerInput.focus();
            return;
        }
        if (!state.connected) {
            showToast('Connect to databases first', 'warning');
            return;
        }

        const dbChoice = dom.optimizerDbSelect.value;
        const dbUrl = dbChoice === 'source' ? state.sourceUrl : state.targetUrl;

        dom.optimizeBtn.disabled = true;
        dom.optimizerResults.classList.add('hidden');
        dom.optimizerSkeleton.classList.remove('hidden');

        try {
            const data = await apiCall('/optimize', { db_url: dbUrl, query: sql });
            dom.optimizerSkeleton.classList.add('hidden');
            dom.optimizeBtn.disabled = false;
            renderOptimizerResults(data);
            showToast('Query optimized!', 'success');
        } catch (err) {
            dom.optimizerSkeleton.classList.add('hidden');
            dom.optimizeBtn.disabled = false;
            showToast(`Optimization failed: ${err.message}`, 'error');
        }
    }

    function renderOptimizerResults(data) {
        dom.optimizerResults.classList.remove('hidden');
        const score = Math.min(100, Math.max(0, data.difference_score || 0));
        const scoreClass = score < 30 ? 'low' : score < 70 ? 'medium' : 'high';
        dom.perfScoreValue.textContent = `${score}%`;
        dom.perfScoreFill.className = `perf-score-fill ${scoreClass}`;
        setTimeout(() => { dom.perfScoreFill.style.width = `${score}%`; }, 100);
        dom.origQueryDisplay.textContent = data.original_query;
        dom.optQueryDisplay.textContent = data.optimized_query;
        dom.explanationContent.innerHTML = renderMarkdown(data.explanation);
        dom.optimizerResults.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function renderMarkdown(text) {
        if (!text) return '<p>No explanation provided.</p>';
        return text
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.+?)\*/g, '<em>$1</em>')
            .replace(/`(.+?)`/g, '<code style="background:var(--bg-tertiary);padding:2px 6px;border-radius:4px;font-family:var(--font-mono);font-size:12px;">$1</code>')
            .replace(/\n\n/g, '</p><p>')
            .replace(/\n- /g, '</p><li>')
            .replace(/\n/g, '<br>')
            .replace(/^/, '<p>')
            .replace(/$/, '</p>');
    }

    // ===== MODAL =====
    function openChartModal(imgBase64, title) {
        dom.chartModalImg.src = `data:image/png;base64,${imgBase64}`;
        dom.chartModalTitle.textContent = title || 'Chart';
        dom.chartModal.classList.add('open');
        dom.chartModal.setAttribute('aria-hidden', 'false');
        document.body.style.overflow = 'hidden';
    }

    function closeChartModal() {
        dom.chartModal.classList.remove('open');
        dom.chartModal.setAttribute('aria-hidden', 'true');
        document.body.style.overflow = '';
    }

    // ===== EVENT LISTENERS =====
    function initEvents() {
        // Navigation
        $$('.nav-item').forEach(item => {
            item.addEventListener('click', () => navigateTo(item.dataset.page));
            item.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); navigateTo(item.dataset.page); }
            });
        });

        // Sidebar
        dom.sidebarToggle.addEventListener('click', toggleSidebar);

        // Mobile
        dom.mobileMenuBtn.addEventListener('click', () => {
            dom.sidebar.classList.toggle('mobile-open');
            dom.mobileOverlay.classList.toggle('show');
        });
        dom.mobileOverlay.addEventListener('click', () => {
            dom.sidebar.classList.remove('mobile-open');
            dom.mobileOverlay.classList.remove('show');
        });

        // Theme & Safe mode
        dom.themeToggle.addEventListener('click', toggleTheme);
        dom.safeModeToggle.addEventListener('click', toggleSafeMode);
        dom.safeModeToggle.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleSafeMode(); }
        });

        // URL type detection (live input)
        dom.sourceUrlInput.addEventListener('input', () => {
            updateTypeIndicator(dom.sourceUrlInput, dom.sourceTypeIndicator, dom.sourceDbNameGroup);
        });
        dom.targetUrlInput.addEventListener('input', () => {
            updateTypeIndicator(dom.targetUrlInput, dom.targetTypeIndicator, dom.targetDbNameGroup);
        });

        // Presets
        $$('.preset-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const target = btn.dataset.target;
                const input = target === 'source' ? dom.sourceUrlInput : dom.targetUrlInput;
                input.value = btn.dataset.preset;
                input.focus();
                input.dispatchEvent(new Event('input'));
            });
        });

        // Dual connect
        dom.connectDualBtn.addEventListener('click', handleDualConnect);

        // Schema table clicks
        dom.sourceTablesList.addEventListener('click', (e) => {
            const item = e.target.closest('.schema-table-item');
            if (item) loadTableDetail(item.dataset.table, item.dataset.role);
        });
        dom.targetTablesList.addEventListener('click', (e) => {
            const item = e.target.closest('.schema-table-item');
            if (item) loadTableDetail(item.dataset.table, item.dataset.role);
        });

        // AI Query
        dom.aiSendBtn.addEventListener('click', handleDualQuery);
        dom.aiQueryInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleDualQuery(); }
        });
        dom.aiQueryInput.addEventListener('input', () => {
            dom.aiQueryInput.style.height = 'auto';
            dom.aiQueryInput.style.height = Math.min(200, dom.aiQueryInput.scrollHeight) + 'px';
        });

        // Copy query buttons
        $$('.copy-query-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const targetId = btn.dataset.target;
                const el = document.getElementById(targetId);
                if (el) copyToClipboard(el.textContent);
            });
        });

        // Chart fullscreen
        document.addEventListener('click', (e) => {
            const chartCard = e.target.closest('.chart-card, .dashboard-card');
            if (chartCard && chartCard.dataset.img) {
                openChartModal(chartCard.dataset.img, chartCard.dataset.title || 'Chart');
            }
        });

        // Modal
        dom.chartModalClose.addEventListener('click', closeChartModal);
        dom.chartNewTabBtn.addEventListener('click', () => {
            const win = window.open();
            win.document.write(`<iframe src="${dom.chartModalImg.src}" frameborder="0" style="border:0;width:100%;height:100%;" allowfullscreen></iframe>`);
        });
        dom.chartModal.addEventListener('click', (e) => { if (e.target === dom.chartModal) closeChartModal(); });
        document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeChartModal(); });

        // Dashboard
        dom.genDashboardBtn.addEventListener('click', handleGenDashboard);

        // Optimizer
        dom.optimizeBtn.addEventListener('click', handleOptimize);
        dom.copyOptimizedBtn.addEventListener('click', () => copyToClipboard(dom.optQueryDisplay.textContent));

        // History clicks
        dom.historyList.addEventListener('click', (e) => {
            const item = e.target.closest('.history-item');
            if (item) {
                const idx = parseInt(item.dataset.idx);
                if (state.queryHistory[idx]) {
                    dom.aiQueryInput.value = state.queryHistory[idx].query;
                    dom.aiQueryInput.dispatchEvent(new Event('input'));
                }
            }
        });

        // ═══ MIGRATION EVENTS ═══

        // Pipeline card clicks
        dom.pipelineGrid.addEventListener('click', (e) => {
            const card = e.target.closest('.pipeline-card');
            if (card) selectPipeline(card.dataset.pid);
        });
        dom.pipelineGrid.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                const card = e.target.closest('.pipeline-card');
                if (card) { e.preventDefault(); selectPipeline(card.dataset.pid); }
            }
        });

        // Back buttons
        dom.migBackTo1.addEventListener('click', () => setMigStep(1));
        dom.migBackTo2.addEventListener('click', () => setMigStep(2));
        if (dom.migBackTo3) dom.migBackTo3.addEventListener('click', () => setMigStep(3));

        // Test connections
        dom.migTestSourceBtn.addEventListener('click', () => testMigConnection('source'));
        dom.migTestTargetBtn.addEventListener('click', () => testMigConnection('target'));

        // Extract schema (Step 2 → Step 3)
        dom.migExtractSchemaBtn.addEventListener('click', extractSchemaOnly);

        // Generate AI Plan (Step 3 → Step 4)
        if (dom.migGenPlanBtn) dom.migGenPlanBtn.addEventListener('click', generateMigPlan);

        // Update plan (HITL)
        dom.migUpdatePlanBtn.addEventListener('click', updateMigPlan);

        // Approve & Execute
        dom.migApproveBtn.addEventListener('click', approveAndExecute);

        // Cancel
        dom.migCancelBtn.addEventListener('click', () => {
            setMigStep(1);
            showToast('Migration cancelled.', 'info');
        });

        // Launch SQLAI after migration
        dom.migLaunchSqlaiBtn.addEventListener('click', launchSqlaiAfterMigration);

        // New Migration
        dom.migNewMigrationBtn.addEventListener('click', resetMigration);
    }

    // ===== AUTO-POPULATE FROM URL PARAMS =====
    function checkUrlParams() {
        const params = new URLSearchParams(window.location.search);
        const source = params.get('source_url');
        const target = params.get('target_url');
        const sourceDb = params.get('source_db');
        const targetDb = params.get('target_db');

        if (source) {
            dom.sourceUrlInput.value = source;
            dom.sourceUrlInput.dispatchEvent(new Event('input'));
        }
        if (target) {
            dom.targetUrlInput.value = target;
            dom.targetUrlInput.dispatchEvent(new Event('input'));
        }
        if (sourceDb) dom.sourceDbNameInput.value = sourceDb;
        if (targetDb) dom.targetDbNameInput.value = targetDb;

        // Auto-connect if both provided
        if (source && target) {
            navigateTo('connect');
            setTimeout(() => handleDualConnect(), 500);
        }
    }

    // ===== INIT =====
    function init() {
        loadTheme();
        initEvents();
        navigateTo('migration');
        loadPipelines();
        checkUrlParams();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
