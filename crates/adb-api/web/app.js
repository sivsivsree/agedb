// AgenticDB Web Dashboard Client Application

(function () {
  'use strict';

  // State
  const state = {
    apiKey: localStorage.getItem('agdb_api_key') || '',
    tenant: localStorage.getItem('agdb_tenant') || 'local',
    currentDatabase: '',
    currentTable: '',
    currentSchema: null,
    browseLimit: 100,
    browseOffset: 0,
    browseRows: [],
    browseTotalCount: 0,
    sortColumn: null,
    sortDirection: 'asc', // 'asc' | 'desc'
    queryMode: 'nl', // 'nl' | 'plan'
    activeTab: 'browse',
    databases: [],
    tables: []
  };

  // DOM Elements
  const el = {
    dbSelect: document.getElementById('database-select'),
    tenantBadge: document.getElementById('active-tenant-badge'),
    healthIndicator: document.getElementById('health-indicator'),
    healthStatusText: document.getElementById('health-status-text'),
    catalogEpochVal: document.getElementById('catalog-epoch-val'),
    nlEngineVal: document.getElementById('nl-engine-val'),
    tablesList: document.getElementById('tables-list'),
    btnRefreshTables: document.getElementById('btn-refresh-tables'),
    btnSettings: document.getElementById('btn-settings'),
    authModal: document.getElementById('auth-modal'),
    btnCloseAuthModal: document.getElementById('btn-close-auth-modal'),
    btnSaveAuth: document.getElementById('btn-save-auth'),
    inputApiKey: document.getElementById('input-api-key'),
    inputTenant: document.getElementById('input-tenant'),

    // Tabs
    tabBtns: document.querySelectorAll('.tab-btn'),
    tabContents: document.querySelectorAll('.tab-content'),

    // Browse Tab
    activeTableTitle: document.getElementById('active-table-title'),
    tableRowCountChip: document.getElementById('table-row-count-chip'),
    btnRefreshData: document.getElementById('btn-refresh-data'),
    browseLimitSelect: document.getElementById('browse-limit-select'),
    btnPagePrev: document.getElementById('btn-page-prev'),
    btnPageNext: document.getElementById('btn-page-next'),
    paginationPageDisplay: document.getElementById('pagination-page-display'),
    dataGridHead: document.getElementById('data-grid-head'),
    dataGridBody: document.getElementById('data-grid-body'),

    // Query Tab
    btnQueryModeNl: document.getElementById('btn-query-mode-nl'),
    btnQueryModePlan: document.getElementById('btn-query-mode-plan'),
    queryInputNlBox: document.getElementById('query-input-nl-box'),
    queryInputPlanBox: document.getElementById('query-input-plan-box'),
    queryNlText: document.getElementById('query-nl-text'),
    queryPlanText: document.getElementById('query-plan-text'),
    btnRunQuery: document.getElementById('btn-run-query'),
    btnExplainQuery: document.getElementById('btn-explain-query'),
    queryFeedback: document.getElementById('query-feedback'),
    queryInterpretationBox: document.getElementById('query-interpretation-box'),
    queryInterpretationText: document.getElementById('query-interpretation-text'),
    queryWarningsBox: document.getElementById('query-warnings-box'),
    queryWarningsText: document.getElementById('query-warnings-text'),
    queryTelemetryBar: document.getElementById('query-telemetry-bar'),
    telemetryDuration: document.getElementById('telemetry-duration'),
    telemetryRows: document.getElementById('telemetry-rows'),
    telemetrySegments: document.getElementById('telemetry-segments'),
    telemetryBytes: document.getElementById('telemetry-bytes'),
    explainOutput: document.getElementById('explain-output'),
    queryResultsHead: document.getElementById('query-results-head'),
    queryResultsBody: document.getElementById('query-results-body'),

    // Schema Tab
    schemaTableName: document.getElementById('schema-table-name'),
    schemaPkTag: document.getElementById('schema-pk-tag'),
    schemaPartitionsTag: document.getElementById('schema-partitions-tag'),
    schemaVersionTag: document.getElementById('schema-version-tag'),
    schemaGridBody: document.getElementById('schema-grid-body'),

    // Drawer
    rowDrawer: document.getElementById('row-drawer'),
    btnCloseDrawer: document.getElementById('btn-close-drawer'),
    drawerKeyBadge: document.getElementById('drawer-key-badge'),
    drawerFields: document.getElementById('drawer-fields'),
    drawerRawJson: document.getElementById('drawer-raw-json'),

    // Connect AI Modal
    btnConnectAi: document.getElementById('btn-connect-ai'),
    aiModal: document.getElementById('ai-modal'),
    btnCloseAiModal: document.getElementById('btn-close-ai-modal'),
    aiTabChatgpt: document.getElementById('ai-tab-chatgpt'),
    aiTabMcp: document.getElementById('ai-tab-mcp'),
    aiContentChatgpt: document.getElementById('ai-content-chatgpt'),
    aiContentMcp: document.getElementById('ai-content-mcp'),
    chatgptSchemaUrl: document.getElementById('chatgpt-schema-url'),
    btnCopySchemaUrl: document.getElementById('btn-copy-schema-url'),
    btnCopyRawOpenapi: document.getElementById('btn-copy-raw-openapi'),
    btnCopyMcpConfig: document.getElementById('btn-copy-mcp-config'),
    mcpConfigCode: document.getElementById('mcp-config-code')
  };

  // Helper: API Fetch wrapper
  async function api(path, options = {}) {
    const headers = options.headers || {};
    if (state.apiKey) {
      headers['Authorization'] = 'Bearer ' + state.apiKey;
    }
    headers['Content-Type'] = 'application/json';

    try {
      const response = await fetch(path, { ...options, headers });
      const contentType = response.headers.get('content-type') || '';
      let data = null;
      if (contentType.includes('application/json')) {
        data = await response.json();
      } else {
        data = await response.text();
      }

      if (!response.ok) {
        const errorMsg = (data && data.error && data.error.message) ? data.error.message : (typeof data === 'string' ? data : response.statusText);
        throw new Error(errorMsg);
      }
      return data;
    } catch (err) {
      console.error('API request error:', path, err);
      throw err;
    }
  }

  // Check health and update status
  async function checkHealth() {
    try {
      const health = await api('/healthz');
      el.healthIndicator.className = 'status-indicator online';
      el.healthStatusText.textContent = 'v' + (health.version || '0.1');
      el.catalogEpochVal.textContent = health.catalog_epoch != null ? health.catalog_epoch : '-';
      el.nlEngineVal.textContent = health.natural_language || 'rules';
    } catch (e) {
      el.healthIndicator.className = 'status-indicator offline';
      el.healthStatusText.textContent = 'Offline';
    }
  }

  // Load Databases
  async function loadDatabases() {
    try {
      const res = await api('/v1/databases');
      state.databases = res.databases || [];
      el.dbSelect.innerHTML = '';

      if (state.databases.length === 0) {
        el.dbSelect.innerHTML = '<option value="">(No databases found)</option>';
        return;
      }

      state.databases.forEach(db => {
        const opt = document.createElement('option');
        opt.value = db;
        opt.textContent = db;
        el.dbSelect.appendChild(opt);
      });

      if (!state.currentDatabase || !state.databases.includes(state.currentDatabase)) {
        state.currentDatabase = state.databases[0];
      }
      el.dbSelect.value = state.currentDatabase;
      await loadTables();
    } catch (err) {
      el.dbSelect.innerHTML = '<option value="">(Failed to load databases)</option>';
      console.error('Failed to load databases:', err);
    }
  }

  // Load Tables for current database
  async function loadTables() {
    if (!state.currentDatabase) {
      el.tablesList.innerHTML = '<div class="empty-state">No database selected</div>';
      return;
    }

    try {
      el.tablesList.innerHTML = '<div class="empty-state">Loading tables...</div>';
      const res = await api('/v1/databases/' + encodeURIComponent(state.currentDatabase) + '/tables');

      const rawList = res.tables || [];
      state.tableMeta = {};
      state.tables = rawList.map(t => {
        if (typeof t === 'string') {
          return t;
        }
        const name = t.table || t.name;
        state.tableMeta[name] = t;
        return name;
      });

      if (state.tables.length === 0) {
        el.tablesList.innerHTML = '<div class="empty-state">No tables in database</div>';
        state.currentTable = '';
        state.currentSchema = null;
        renderBrowseGrid([], null);
        return;
      }

      el.tablesList.innerHTML = '';
      state.tables.forEach(tableName => {
        const meta = state.tableMeta[tableName];
        const rowCount = meta && meta.rows != null ? meta.rows : null;
        const item = document.createElement('div');
        item.className = 'sidebar-item' + (tableName === state.currentTable ? ' active' : '');
        item.dataset.table = tableName;
        item.innerHTML = `
          <div class="sidebar-item-name">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect>
              <line x1="3" y1="9" x2="21" y2="9"></line>
              <line x1="9" y1="21" x2="9" y2="9"></line>
            </svg>
            <span>${escapeHtml(tableName)}</span>
          </div>
          ${rowCount != null ? `<span class="chip chip-dim" style="font-size:10px">${rowCount.toLocaleString()}</span>` : ''}
        `;
        item.addEventListener('click', () => selectTable(tableName));
        el.tablesList.appendChild(item);
      });

      if (!state.currentTable || !state.tables.includes(state.currentTable)) {
        selectTable(state.tables[0]);
      } else {
        selectTable(state.currentTable);
      }
    } catch (err) {
      el.tablesList.innerHTML = `<div class="empty-state">Error: ${escapeHtml(err.message)}</div>`;
    }
  }

  // Select Table
  async function selectTable(tableName) {
    if (!tableName || typeof tableName !== 'string') return;
    state.currentTable = tableName;
    state.browseOffset = 0;
    state.sortColumn = null;

    // Update active highlight in sidebar
    document.querySelectorAll('.sidebar-item').forEach(item => {
      const name = item.dataset.table;
      item.classList.toggle('active', name === tableName);
    });

    el.activeTableTitle.textContent = tableName;
    el.schemaTableName.textContent = tableName + ' Schema';

    // Fetch Table Schema and stats
    try {
      const res = await api('/v1/databases/' + encodeURIComponent(state.currentDatabase) + '/tables/' + encodeURIComponent(tableName));
      state.currentSchema = res.schema || res;
      if (res.stats && res.stats.rows != null) {
        state.browseTotalCount = res.stats.rows;
      }
      renderSchemaView(state.currentSchema);

      // Pre-fill query template with this table
      el.queryPlanText.value = JSON.stringify({
        operation: 'select',
        table: tableName,
        limit: 100
      }, null, 2);

      await loadTableData();
    } catch (err) {
      console.error('Failed to describe table:', err);
    }
  }

  // Render Schema Tab
  function renderSchemaView(schema) {
    if (!schema) return;

    el.schemaPkTag.textContent = schema.primary_key ? 'PK: ' + schema.primary_key.join(', ') : 'PK: None';
    el.schemaPartitionsTag.textContent = 'Partitions: ' + (schema.partitions || 1);
    el.schemaVersionTag.textContent = 'Version: ' + (schema.version || 1);

    if (!schema.columns || schema.columns.length === 0) {
      el.schemaGridBody.innerHTML = '<tr><td colspan="7" class="cell-empty">No columns declared.</td></tr>';
      return;
    }

    el.schemaGridBody.innerHTML = schema.columns.map(col => {
      const isPk = schema.primary_key && schema.primary_key.includes(col.name);
      const flags = [];
      if (isPk) flags.push('<span class="chip" style="background:#58a6ff22;color:#58a6ff">PRIMARY KEY</span>');
      if (col.nullable) flags.push('<span class="chip chip-dim">NULLABLE</span>');
      if (col.is_sensitive) flags.push('<span class="chip" style="background:#f8514922;color:#f85149">SENSITIVE</span>');

      return `
        <tr>
          <td class="font-mono" style="font-weight:600;color:var(--text-primary);">${escapeHtml(col.name)}</td>
          <td><span class="type-badge ${getTypeClass(col.data_type)}">${escapeHtml(col.data_type || 'Unknown')}</span></td>
          <td>${col.semantic_type ? '<span class="chip chip-dim">' + escapeHtml(col.semantic_type) + '</span>' : '-'}</td>
          <td>${escapeHtml(col.currency || col.unit || '-')}</td>
          <td>${escapeHtml(col.default_aggregate || '-')}</td>
          <td>${flags.join(' ') || '-'}</td>
          <td style="color:var(--text-secondary);">${escapeHtml(col.description || '-')}</td>
        </tr>
      `;
    }).join('');
  }

  // Load Table Data for Data Browser
  async function loadTableData() {
    if (!state.currentDatabase || !state.currentTable) return;

    // Fetch total row count (answered in 0 ms from segment metadata)
    try {
      const countRes = await api('/v1/databases/' + encodeURIComponent(state.currentDatabase) + '/query', {
        method: 'POST',
        body: JSON.stringify({
          plan: {
            operation: 'aggregate',
            table: state.currentTable,
            metrics: [{ function: 'count' }]
          }
        })
      });
      if (countRes.rows && countRes.rows.length > 0) {
        state.browseTotalCount = countRes.rows[0].count != null ? countRes.rows[0].count : (countRes.rows[0].count_star || 0);
      }
    } catch (_) {}

    const isAll = state.browseLimit === 'all';
    const plan = {
      operation: 'select',
      table: state.currentTable
    };

    if (!isAll && typeof state.browseLimit === 'number') {
      plan.limit = state.browseLimit;
    }
    if (!isAll && state.browseOffset > 0) {
      plan.offset = state.browseOffset;
    }

    if (state.sortColumn) {
      plan.order_by = [{
        column: state.sortColumn,
        direction: state.sortDirection
      }];
    }

    try {
      el.dataGridBody.innerHTML = '<tr><td class="cell-empty">Loading records...</td></tr>';
      const outcome = await api('/v1/databases/' + encodeURIComponent(state.currentDatabase) + '/query', {
        method: 'POST',
        body: JSON.stringify({ plan })
      });

      state.browseRows = outcome.rows || [];
      renderBrowseGrid(state.browseRows, outcome.schema);

      const count = state.browseRows.length;
      if (isAll) {
        el.tableRowCountChip.textContent = `All ${count.toLocaleString()} rows`;
        el.paginationPageDisplay.textContent = 'All rows';
        el.btnPagePrev.disabled = true;
        el.btnPageNext.disabled = true;
      } else {
        const total = Math.max(state.browseTotalCount, count);
        const start = total === 0 ? 0 : state.browseOffset + 1;
        const end = Math.min(state.browseOffset + count, total);
        el.tableRowCountChip.textContent = `${start}-${end} of ${total.toLocaleString()} rows`;

        const page = Math.floor(state.browseOffset / state.browseLimit) + 1;
        const totalPages = Math.max(1, Math.ceil(total / state.browseLimit));
        el.paginationPageDisplay.textContent = `Page ${page} of ${totalPages}`;
        el.btnPagePrev.disabled = state.browseOffset === 0;
        el.btnPageNext.disabled = end >= total;
      }
    } catch (err) {
      el.dataGridBody.innerHTML = `<tr><td class="cell-empty" style="color:var(--accent-red)">Error: ${escapeHtml(err.message)}</td></tr>`;
    }
  }

  // Render Spreadsheet Grid
  function renderBrowseGrid(rows, schema) {
    if (!rows || rows.length === 0) {
      el.dataGridHead.innerHTML = '<tr><th class="cell-empty">Empty</th></tr>';
      el.dataGridBody.innerHTML = '<tr><td class="cell-empty">No rows found in table.</td></tr>';
      return;
    }

    // Determine column keys from schema or first row
    const columns = (schema && schema.columns) ? schema.columns.map(c => c.name) : Object.keys(rows[0]);
    const colMetaMap = {};
    if (schema && schema.columns) {
      schema.columns.forEach(c => { colMetaMap[c.name] = c; });
    }

    // Render Headers
    el.dataGridHead.innerHTML = `<tr>` + columns.map(colName => {
      const meta = colMetaMap[colName];
      const typeStr = meta ? meta.data_type : '';
      const isSorted = state.sortColumn === colName;
      const sortArrow = isSorted ? (state.sortDirection === 'asc' ? ' &uarr;' : ' &darr;') : '';

      return `
        <th data-column="${escapeHtml(colName)}">
          <div class="th-content">
            <span class="th-name">${escapeHtml(colName)}${sortArrow}</span>
            ${typeStr ? `<span class="type-badge ${getTypeClass(typeStr)}">${escapeHtml(typeStr)}</span>` : ''}
          </div>
        </th>
      `;
    }).join('') + `</tr>`;

    // Add sort click listeners to headers
    el.dataGridHead.querySelectorAll('th').forEach(th => {
      th.addEventListener('click', () => {
        const col = th.dataset.column;
        if (!col) return;
        if (state.sortColumn === col) {
          state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
          state.sortColumn = col;
          state.sortDirection = 'asc';
        }
        loadTableData();
      });
    });

    // Render Rows
    el.dataGridBody.innerHTML = rows.map((row, idx) => {
      const cells = columns.map(col => {
        const val = row[col];
        if (val === null || val === undefined) {
          return '<td class="cell-null">null</td>';
        }
        if (typeof val === 'number') {
          return `<td class="cell-num">${val.toLocaleString()}</td>`;
        }
        if (typeof val === 'boolean') {
          return `<td><span class="chip ${val ? 'chip-dim' : ''}" style="color:${val ? 'var(--accent-green)' : 'var(--accent-red)'}">${val}</span></td>`;
        }
        return `<td>${escapeHtml(String(val))}</td>`;
      }).join('');

      return `<tr data-row-idx="${idx}">${cells}</tr>`;
    }).join('');

    // Attach Row Click event to open Drawer
    el.dataGridBody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        const idx = tr.dataset.rowIdx;
        if (idx != null) {
          openRowDrawer(rows[idx]);
        }
      });
    });
  }

  // Open Row Detail Drawer
  function openRowDrawer(rowObj) {
    if (!rowObj) return;

    el.drawerFields.innerHTML = Object.entries(rowObj).map(([k, v]) => `
      <div class="drawer-field">
        <span class="drawer-field-name">${escapeHtml(k)}</span>
        <span class="drawer-field-val">${v === null ? '<span class="cell-null">null</span>' : escapeHtml(String(v))}</span>
      </div>
    `).join('');

    el.drawerRawJson.textContent = JSON.stringify(rowObj, null, 2);
    el.rowDrawer.classList.remove('hidden');
  }

  // Execute Query
  async function runQuery(explainOnly = false) {
    if (!state.currentDatabase) {
      alert('Please select a database first.');
      return;
    }

    let payload = {};
    if (state.queryMode === 'nl') {
      const text = el.queryNlText.value.trim();
      if (!text) {
        alert('Please enter a plain language question.');
        return;
      }
      payload.request = text;
    } else {
      try {
        const plan = JSON.parse(el.queryPlanText.value);
        payload.plan = plan;
      } catch (e) {
        alert('Invalid JSON in structured plan: ' + e.message);
        return;
      }
    }

    // Reset UI
    el.queryFeedback.classList.add('hidden');
    el.queryInterpretationBox.classList.add('hidden');
    el.queryWarningsBox.classList.add('hidden');
    el.queryTelemetryBar.classList.add('hidden');
    el.explainOutput.classList.add('hidden');

    const endpoint = explainOnly
      ? '/v1/databases/' + encodeURIComponent(state.currentDatabase) + '/explain'
      : '/v1/databases/' + encodeURIComponent(state.currentDatabase) + '/query';

    try {
      el.queryResultsBody.innerHTML = '<tr><td class="cell-empty">Executing query...</td></tr>';
      const res = await api(endpoint, {
        method: 'POST',
        body: JSON.stringify(payload)
      });

      if (explainOnly) {
        el.explainOutput.textContent = JSON.stringify(res.plan || res, null, 2);
        el.explainOutput.classList.remove('hidden');
        el.queryResultsBody.innerHTML = '<tr><td class="cell-empty">Explain plan completed. See above.</td></tr>';
        return;
      }

      // Display Natural Language Interpretation
      if (res.interpretation) {
        el.queryInterpretationText.textContent = res.interpretation;
        el.queryInterpretationBox.classList.remove('hidden');
        el.queryFeedback.classList.remove('hidden');
      }

      // Display Warnings
      if (res.warnings && res.warnings.length > 0) {
        el.queryWarningsText.textContent = res.warnings.join(' | ');
        el.queryWarningsBox.classList.remove('hidden');
        el.queryFeedback.classList.remove('hidden');
      }

      // Display Telemetry
      if (res.stats) {
        el.telemetryDuration.textContent = (res.stats.duration_ms != null ? res.stats.duration_ms.toFixed(2) + ' ms' : '-');
        el.telemetryRows.textContent = (res.stats.rows_returned != null ? res.stats.rows_returned.toLocaleString() : (res.rows ? res.rows.length : 0));
        el.telemetrySegments.textContent = (res.stats.segments_scanned != null ? res.stats.segments_scanned : 0) + ' / ' + (res.stats.segments_pruned != null ? res.stats.segments_pruned : 0);
        el.telemetryBytes.textContent = formatBytes(res.stats.bytes_scanned || 0);
        el.queryTelemetryBar.classList.remove('hidden');
      }

      // Render Query Results
      renderQueryResults(res.rows || [], res.schema);
    } catch (err) {
      el.queryResultsBody.innerHTML = `<tr><td class="cell-empty" style="color:var(--accent-red)">Query execution error: ${escapeHtml(err.message)}</td></tr>`;
    }
  }

  // Render Query Results Grid
  function renderQueryResults(rows, schema) {
    if (!rows || rows.length === 0) {
      el.queryResultsHead.innerHTML = '<tr><th class="cell-empty">Result</th></tr>';
      el.queryResultsBody.innerHTML = '<tr><td class="cell-empty">Query executed successfully: 0 rows returned.</td></tr>';
      return;
    }

    const columns = (schema && schema.columns) ? schema.columns.map(c => c.name) : Object.keys(rows[0]);

    el.queryResultsHead.innerHTML = '<tr>' + columns.map(c => `<th><span class="th-name">${escapeHtml(c)}</span></th>`).join('') + '</tr>';
    el.queryResultsBody.innerHTML = rows.map((r, idx) => {
      const cells = columns.map(c => {
        const val = r[c];
        if (val === null || val === undefined) return '<td class="cell-null">null</td>';
        if (typeof val === 'number') return `<td class="cell-num">${val.toLocaleString()}</td>`;
        return `<td>${escapeHtml(String(val))}</td>`;
      }).join('');
      return `<tr data-row-idx="${idx}">${cells}</tr>`;
    }).join('');

    el.queryResultsBody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        const idx = tr.dataset.rowIdx;
        if (idx != null) openRowDrawer(rows[idx]);
      });
    });
  }

  // Utilities
  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function getTypeClass(typeStr) {
    if (!typeStr) return 'type-string';
    const s = String(typeStr).toLowerCase();
    if (s.includes('int')) return 'type-int';
    if (s.includes('float') || s.includes('decimal')) return 'type-float';
    if (s.includes('bool')) return 'type-bool';
    if (s.includes('time') || s.includes('date')) return 'type-timestamp';
    return 'type-string';
  }

  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  // Event Listeners Initialization
  function initListeners() {
    // Database change
    el.dbSelect.addEventListener('change', (e) => {
      state.currentDatabase = e.target.value;
      state.currentTable = '';
      state.currentSchema = null;
      loadTables();
    });

    // Refresh tables
    el.btnRefreshTables.addEventListener('click', () => loadTables());

    // Tabs switching
    el.tabBtns.forEach(btn => {
      btn.addEventListener('click', () => {
        const tab = btn.dataset.tab;
        el.tabBtns.forEach(b => b.classList.toggle('active', b === btn));
        el.tabContents.forEach(c => c.classList.toggle('active', c.id === 'view-' + tab));
        state.activeTab = tab;
      });
    });

    // Data browser pagination and limit
    el.browseLimitSelect.addEventListener('change', (e) => {
      const val = e.target.value;
      state.browseLimit = val === 'all' ? 'all' : parseInt(val, 10);
      state.browseOffset = 0;
      loadTableData();
    });

    el.btnPagePrev.addEventListener('click', () => {
      if (typeof state.browseLimit === 'number' && state.browseOffset >= state.browseLimit) {
        state.browseOffset -= state.browseLimit;
        loadTableData();
      }
    });

    el.btnPageNext.addEventListener('click', () => {
      if (typeof state.browseLimit === 'number') {
        state.browseOffset += state.browseLimit;
        loadTableData();
      }
    });

    el.btnRefreshData.addEventListener('click', () => loadTableData());

    // Query mode switch
    el.btnQueryModeNl.addEventListener('click', () => {
      state.queryMode = 'nl';
      el.btnQueryModeNl.classList.add('active');
      el.btnQueryModePlan.classList.remove('active');
      el.queryInputNlBox.classList.remove('hidden');
      el.queryInputPlanBox.classList.add('hidden');
    });

    el.btnQueryModePlan.addEventListener('click', () => {
      state.queryMode = 'plan';
      el.btnQueryModePlan.classList.add('active');
      el.btnQueryModeNl.classList.remove('active');
      el.queryInputPlanBox.classList.remove('hidden');
      el.queryInputNlBox.classList.add('hidden');
    });

    // Run / Explain Query
    el.btnRunQuery.addEventListener('click', () => runQuery(false));
    el.btnExplainQuery.addEventListener('click', () => runQuery(true));

    // Drawer close
    el.btnCloseDrawer.addEventListener('click', () => el.rowDrawer.classList.add('hidden'));

    // Auth Modal
    el.btnSettings.addEventListener('click', () => {
      el.inputApiKey.value = state.apiKey;
      el.inputTenant.value = state.tenant;
      el.authModal.classList.remove('hidden');
    });

    el.btnCloseAuthModal.addEventListener('click', () => el.authModal.classList.add('hidden'));

    el.btnSaveAuth.addEventListener('click', () => {
      state.apiKey = el.inputApiKey.value.trim();
      state.tenant = el.inputTenant.value.trim() || 'local';
      localStorage.setItem('agdb_api_key', state.apiKey);
      localStorage.setItem('agdb_tenant', state.tenant);
      el.tenantBadge.textContent = state.tenant;
      el.authModal.classList.add('hidden');
      checkHealth();
      loadDatabases();
    });

    // Connect AI Assistant Modal Listeners
    if (el.btnConnectAi) {
      el.btnConnectAi.addEventListener('click', () => {
        const origin = window.location.origin;
        if (el.chatgptSchemaUrl) {
          el.chatgptSchemaUrl.value = origin + '/openapi.json';
        }
        el.aiModal.classList.remove('hidden');
      });
    }

    if (el.btnCloseAiModal) {
      el.btnCloseAiModal.addEventListener('click', () => {
        el.aiModal.classList.add('hidden');
      });
    }

    if (el.aiTabChatgpt && el.aiTabMcp) {
      el.aiTabChatgpt.addEventListener('click', () => {
        el.aiTabChatgpt.classList.add('active');
        el.aiTabMcp.classList.remove('active');
        el.aiContentChatgpt.classList.remove('hidden');
        el.aiContentMcp.classList.add('hidden');
      });

      el.aiTabMcp.addEventListener('click', () => {
        el.aiTabMcp.classList.add('active');
        el.aiTabChatgpt.classList.remove('active');
        el.aiContentMcp.classList.remove('hidden');
        el.aiContentChatgpt.classList.add('hidden');
      });
    }

    if (el.btnCopySchemaUrl) {
      el.btnCopySchemaUrl.addEventListener('click', async () => {
        await navigator.clipboard.writeText(el.chatgptSchemaUrl.value);
        const orig = el.btnCopySchemaUrl.textContent;
        el.btnCopySchemaUrl.textContent = 'Copied!';
        setTimeout(() => { el.btnCopySchemaUrl.textContent = orig; }, 1500);
      });
    }

    if (el.btnCopyRawOpenapi) {
      el.btnCopyRawOpenapi.addEventListener('click', async () => {
        try {
          const res = await api('/openapi.json');
          await navigator.clipboard.writeText(JSON.stringify(res, null, 2));
          const orig = el.btnCopyRawOpenapi.textContent;
          el.btnCopyRawOpenapi.textContent = 'Copied JSON!';
          setTimeout(() => { el.btnCopyRawOpenapi.textContent = orig; }, 1500);
        } catch (e) {
          alert('Failed to copy schema: ' + e.message);
        }
      });
    }

    if (el.btnCopyMcpConfig && el.mcpConfigCode) {
      el.btnCopyMcpConfig.addEventListener('click', async () => {
        await navigator.clipboard.writeText(el.mcpConfigCode.textContent.trim());
        const orig = el.btnCopyMcpConfig.textContent;
        el.btnCopyMcpConfig.textContent = 'Copied!';
        setTimeout(() => { el.btnCopyMcpConfig.textContent = orig; }, 1500);
      });
    }
  }

  // App Bootstrap
  async function init() {
    el.tenantBadge.textContent = state.tenant;
    initListeners();
    await checkHealth();
    await loadDatabases();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
