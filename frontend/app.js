/**
 * QueryVista — Frontend Application
 * AI-Powered Database Migration Platform
 */

const API_BASE = "http://localhost:8000";

// ─── State ──────────────────────────────────────────────────────────────────
const state = {
  currentStep: 1,
  sourceDb: null,
  targetDb: null,
  sessionId: null,
  schema: null,
  plan: null,
  migrationId: null,
  sourceConfig: {},
  targetConfig: {},
};

// ─── Database Definitions ───────────────────────────────────────────────────
const DATABASES = [
  {
    id: "mysql",
    name: "MySQL",
    type: "sql",
    icon: "🐬",
    description: "MySQL / MariaDB relational database",
    configFields: [
      { key: "connection_url", label: "Connection URL", placeholder: "mysql+pymysql://user:pass@localhost:3310/dbname", type: "text" },
    ],
    defaults: {
      connection_url: "mysql+pymysql://user1:pass123@localhost:3310/testdb",
    },
  },
  {
    id: "postgresql",
    name: "PostgreSQL",
    type: "sql",
    icon: "🐘",
    description: "PostgreSQL relational database (Neon cloud supported)",
    configFields: [
      { key: "connection_url", label: "Connection URL", placeholder: "postgresql+psycopg2://user:pass@host/dbname", type: "text" },
    ],
    defaults: {
      connection_url: "",
    },
  },
  {
    id: "mongodb",
    name: "MongoDB",
    type: "nosql",
    icon: "🍃",
    description: "MongoDB document database (Atlas cloud supported)",
    configFields: [
      { key: "connection_url", label: "Connection URL", placeholder: "mongodb+srv://user:pass@cluster.mongodb.net/", type: "text" },
      { key: "database", label: "Database Name", placeholder: "my_database", type: "text" },
    ],
    defaults: {
      connection_url: "",
      database: "",
    },
  },
  {
    id: "couchdb",
    name: "CouchDB",
    type: "nosql",
    icon: "🛋️",
    description: "Apache CouchDB document database",
    configFields: [
      { key: "host", label: "CouchDB Host", placeholder: "http://localhost:5984", type: "text" },
      { key: "username", label: "Username", placeholder: "admin", type: "text" },
      { key: "password", label: "Password", placeholder: "admin123", type: "password" },
    ],
    defaults: {
      host: "http://localhost:5984",
      username: "admin",
      password: "admin123",
    },
  },
];

// ─── Initialize ─────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  renderDbSelectors();
});

// ─── Database Selectors ─────────────────────────────────────────────────────
function renderDbSelectors() {
  const sourceGrid = document.getElementById("sourceDbGrid");
  const targetGrid = document.getElementById("targetDbGrid");

  sourceGrid.innerHTML = DATABASES.map(db => `
    <div class="db-option" id="source-${db.id}" onclick="selectDb('source', '${db.id}')">
      <div class="db-option-icon">${db.icon}</div>
      <div class="db-option-info">
        <h3>${db.name} <span class="db-type-badge ${db.type}">${db.type}</span></h3>
        <p>${db.description}</p>
      </div>
    </div>
  `).join("");

  targetGrid.innerHTML = DATABASES.map(db => `
    <div class="db-option" id="target-${db.id}" onclick="selectDb('target', '${db.id}')">
      <div class="db-option-icon">${db.icon}</div>
      <div class="db-option-info">
        <h3>${db.name} <span class="db-type-badge ${db.type}">${db.type}</span></h3>
        <p>${db.description}</p>
      </div>
    </div>
  `).join("");
}

function selectDb(role, dbId) {
  const db = DATABASES.find(d => d.id === dbId);
  if (!db) return;

  // Deselect all in this role
  document.querySelectorAll(`#${role}DbGrid .db-option`).forEach(el => {
    el.classList.remove("selected");
  });

  // Select this one
  document.getElementById(`${role}-${dbId}`).classList.add("selected");

  if (role === "source") {
    state.sourceDb = db;
  } else {
    state.targetDb = db;
  }

  // Show pipeline confirmation if both selected
  const confirm = document.getElementById("pipelineConfirm");
  if (state.sourceDb && state.targetDb) {
    // Validate: can't migrate same db type to same db type
    if (state.sourceDb.id === state.targetDb.id) {
      showAlert("pipelineConfirm", "warning", "Source and target cannot be the same database type.");
      confirm.style.display = "none";
      return;
    }

    document.getElementById("pipelineSourceName").textContent = state.sourceDb.name;
    document.getElementById("pipelineTargetName").textContent = state.targetDb.name;
    confirm.style.display = "block";
    confirm.scrollIntoView({ behavior: "smooth", block: "center" });
  } else {
    confirm.style.display = "none";
  }
}

// ─── Step Navigation ────────────────────────────────────────────────────────
function goToStep(step) {
  // Validate before moving forward
  if (step === 2 && (!state.sourceDb || !state.targetDb)) {
    return;
  }

  state.currentStep = step;

  // Update panels
  document.querySelectorAll(".step-panel").forEach(p => p.classList.remove("active"));
  document.getElementById(`step${step}Panel`).classList.add("active");

  // Update stepper circles
  for (let i = 1; i <= 5; i++) {
    const circle = document.getElementById(`stepCircle${i}`);
    const label = document.getElementById(`stepLabel${i}`);
    const connector = document.getElementById(`connector${i}`);

    circle.className = "step-circle";
    label.className = "step-label";

    if (i < step) {
      circle.classList.add("completed");
      circle.innerHTML = "✓";
      if (connector) connector.classList.add("completed");
    } else if (i === step) {
      circle.classList.add("active");
      label.classList.add("active");
      circle.innerHTML = i;
      if (connector) connector.classList.remove("completed");
    } else {
      circle.innerHTML = i;
      if (connector) connector.classList.remove("completed");
    }
  }

  // Render step-specific content
  if (step === 2) {
    renderConnectionForms();
  }

  // Scroll to top
  window.scrollTo({ top: 0, behavior: "smooth" });
}

// ─── Connection Forms ───────────────────────────────────────────────────────
function renderConnectionForms() {
  renderConfigFields("source", state.sourceDb);
  renderConfigFields("target", state.targetDb);

  document.getElementById("sourceConfigTitle").textContent = `Source: ${state.sourceDb.name}`;
  document.getElementById("targetConfigTitle").textContent = `Target: ${state.targetDb.name}`;
  document.getElementById("sourceConfigIcon").textContent = state.sourceDb.icon;
  document.getElementById("targetConfigIcon").textContent = state.targetDb.icon;
}

function renderConfigFields(role, db) {
  const container = document.getElementById(`${role}ConfigFields`);
  container.innerHTML = db.configFields.map(field => `
    <div class="form-group">
      <label class="form-label" for="${role}_${field.key}">${field.label}</label>
      <input
        class="form-input"
        type="${field.type || 'text'}"
        id="${role}_${field.key}"
        placeholder="${field.placeholder}"
        value="${db.defaults[field.key] || ''}"
      />
    </div>
  `).join("");
}

function getConfig(role) {
  const db = role === "source" ? state.sourceDb : state.targetDb;
  const config = { db_type: db.id };

  db.configFields.forEach(field => {
    const input = document.getElementById(`${role}_${field.key}`);
    if (input) config[field.key] = input.value.trim();
  });

  return config;
}

// ─── Test Connection ────────────────────────────────────────────────────────
async function testConnection(role) {
  const config = getConfig(role);
  const resultDiv = document.getElementById(`${role}TestResult`);

  resultDiv.innerHTML = `<div class="alert alert-info"><span class="spinner" style="margin-right:8px;"></span> Testing connection...</div>`;

  try {
    const res = await fetch(`${API_BASE}/api/test-connection`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(config),
    });

    const data = await res.json();

    if (data.success) {
      resultDiv.innerHTML = `<div class="alert alert-success"><span class="alert-icon">✅</span>${data.message}</div>`;
      state[`${role}Config`] = config;
    } else {
      resultDiv.innerHTML = `<div class="alert alert-error"><span class="alert-icon">❌</span>${data.message || data.detail}</div>`;
    }
  } catch (err) {
    resultDiv.innerHTML = `<div class="alert alert-error"><span class="alert-icon">❌</span>Connection failed: ${err.message}</div>`;
  }
}

// ─── Extract Schema ─────────────────────────────────────────────────────────
async function extractSchema() {
  const config = getConfig("source");
  showLoading("Extracting Schema...", `Reading ${state.sourceDb.name} metadata`);

  try {
    const res = await fetch(`${API_BASE}/api/extract-schema`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(config),
    });

    const data = await res.json();
    hideLoading();

    if (data.success) {
      state.sessionId = data.session_id;
      state.schema = data.schema;

      renderSchemaDisplay(data.schema, data.tables);
      goToStep(3);
    } else {
      alert("Schema extraction failed: " + (data.detail || "Unknown error"));
    }
  } catch (err) {
    hideLoading();
    alert("Schema extraction failed: " + err.message);
  }
}

function renderSchemaDisplay(schema, tableNames) {
  const display = document.getElementById("schemaDisplay");
  const stats = document.getElementById("schemaStats");

  let totalRows = 0;
  let totalCols = 0;

  let html = "";
  for (const tableName of tableNames || Object.keys(schema)) {
    const table = schema[tableName];

    // Handle SQL schema
    if (table.columns) {
      totalCols += table.columns.length;
      totalRows += table.row_count || 0;

      html += `
        <div class="schema-table">
          <div class="schema-table-header">
            <span style="color:var(--accent-warning);">📁</span>
            <span class="schema-table-name">${tableName}</span>
            <span class="schema-row-count">${(table.row_count || 0).toLocaleString()} rows</span>
            ${table.primary_keys?.length ? `<span class="schema-col-pk">PK: ${table.primary_keys.join(", ")}</span>` : ""}
          </div>
          <div class="schema-columns">
            ${table.columns.map(col => `
              <div class="schema-col">
                <span class="schema-col-name">${col.name}</span>
                <span class="schema-col-type">${col.type}</span>
                ${table.primary_keys?.includes(col.name) ? '<span class="schema-col-pk">PK</span>' : ""}
                ${col.nullable === false ? '<span style="font-size:0.7rem;color:var(--accent-danger);">NOT NULL</span>' : ""}
              </div>
            `).join("")}
          </div>
        </div>
      `;
    }
    // Handle NoSQL schema
    else if (table.fields) {
      const fieldCount = Object.keys(table.fields).length;
      totalCols += fieldCount;
      totalRows += table.document_count || table.doc_count || 0;

      html += `
        <div class="schema-table">
          <div class="schema-table-header">
            <span style="color:var(--accent-secondary);">📦</span>
            <span class="schema-table-name">${tableName}</span>
            <span class="schema-row-count">${(table.document_count || table.doc_count || 0).toLocaleString()} docs</span>
          </div>
          <div class="schema-columns">
            ${Object.entries(table.fields).map(([k, v]) => `
              <div class="schema-col">
                <span class="schema-col-name">${k}</span>
                <span class="schema-col-type">${Array.isArray(v) ? v.join(" | ") : v}</span>
              </div>
            `).join("")}
          </div>
        </div>
      `;
    }
  }

  display.innerHTML = html || "<p style='color:var(--text-muted);'>No schema data available</p>";
  stats.textContent = `${tableNames?.length || Object.keys(schema).length} tables/collections · ${totalCols} fields · ${totalRows.toLocaleString()} total rows`;
}

// ─── Generate AI Plan ───────────────────────────────────────────────────────
async function generatePlan() {
  showLoading("Generating Migration Plan...", "Azure OpenAI GPT-4o is analyzing your schema");

  try {
    const res = await fetch(`${API_BASE}/api/generate-plan`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source_type: state.sourceDb.id,
        target_type: state.targetDb.id,
        schema_data: state.schema,
      }),
    });

    const data = await res.json();
    hideLoading();

    if (data.success) {
      state.sessionId = data.session_id;
      state.plan = data.plan;

      renderPlan(data.plan);
      goToStep(4);
    } else {
      alert("Plan generation failed: " + (data.detail || "Unknown error"));
    }
  } catch (err) {
    hideLoading();
    alert("Plan generation failed: " + err.message);
  }
}

function renderPlan(plan) {
  const content = document.getElementById("planContent");
  content.textContent = JSON.stringify(plan, null, 2);
}

// ─── Update Plan with Feedback (HITL) ───────────────────────────────────────
async function updatePlan() {
  const feedback = document.getElementById("feedbackInput").value.trim();
  if (!feedback) {
    alert("Please enter your feedback before updating.");
    return;
  }

  showLoading("Updating Plan...", "AI is incorporating your feedback");

  try {
    const res = await fetch(`${API_BASE}/api/update-plan`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: state.sessionId,
        feedback: feedback,
      }),
    });

    const data = await res.json();
    hideLoading();

    if (data.success) {
      state.plan = data.plan;
      renderPlan(data.plan);
      document.getElementById("feedbackInput").value = "";

      const planStatus = document.getElementById("planStatus");
      planStatus.textContent = "Updated";
      planStatus.className = "status-badge success";
    } else {
      alert("Update failed: " + (data.detail || "Unknown error"));
    }
  } catch (err) {
    hideLoading();
    alert("Update failed: " + err.message);
  }
}

// ─── Approve Plan ────────────────────────────────────────────────────────────
async function approvePlan() {
  showLoading("Approving Plan...", "Locking in the migration blueprint");

  try {
    // 1. Approve
    const approveRes = await fetch(`${API_BASE}/api/approve-plan`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: state.sessionId }),
    });

    const approveData = await approveRes.json();
    if (!approveData.success) {
      hideLoading();
      alert("Approval failed: " + (approveData.detail || "Unknown"));
      return;
    }

    // 2. Execute
    const sourceConfig = getConfig("source");
    const targetConfig = getConfig("target");

    const execRes = await fetch(`${API_BASE}/api/execute-migration`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: state.sessionId,
        source_config: sourceConfig,
        target_config: targetConfig,
      }),
    });

    const execData = await execRes.json();
    hideLoading();

    if (execData.success) {
      state.migrationId = execData.migration_id;
      goToStep(5);
      pollMigrationStatus();
    } else {
      alert("Execution failed: " + (execData.detail || "Unknown"));
    }
  } catch (err) {
    hideLoading();
    alert("Error: " + err.message);
  }
}

// ─── Poll Migration Status ──────────────────────────────────────────────────
async function pollMigrationStatus() {
  const interval = setInterval(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/migration-status/${state.sessionId}`);
      const data = await res.json();

      if (data.progress) {
        const { current, total, current_table } = data.progress;
        const pct = total > 0 ? Math.round((current / total) * 100) : 0;

        document.getElementById("progressPercent").textContent = `${pct}%`;
        document.getElementById("progressFill").style.width = `${pct}%`;
        document.getElementById("progressTable").textContent = `Current: ${current_table || "—"}`;
        document.getElementById("progressCount").textContent = `${current} / ${total} tables`;
      }

      if (data.status === "completed") {
        clearInterval(interval);
        showResults(data.result);
      } else if (data.status === "failed") {
        clearInterval(interval);
        showError(data.error);
      }
    } catch (err) {
      // Keep polling
    }
  }, 1000);
}

function showResults(result) {
  document.getElementById("progressCard").style.display = "none";
  document.getElementById("migrationResults").style.display = "block";
  document.getElementById("migrationStatusText").textContent = "Migration completed successfully!";

  const tables = result.tables_migrated || [];
  const errors = result.errors || [];

  document.getElementById("statTables").textContent = tables.length;
  document.getElementById("statRows").textContent = (result.total_rows || 0).toLocaleString();
  document.getElementById("statErrors").textContent = errors.length;

  // Results table
  const tbody = document.getElementById("migrationTableBody");
  tbody.innerHTML = "";

  tables.forEach(t => {
    tbody.innerHTML += `
      <tr>
        <td style="font-family:'JetBrains Mono',monospace;font-size:0.85rem;">${t.source}</td>
        <td style="font-family:'JetBrains Mono',monospace;font-size:0.85rem;">${t.target}</td>
        <td>${(t.rows || 0).toLocaleString()}</td>
        <td><span class="status-badge success">✓ Success</span></td>
      </tr>
    `;
  });

  errors.forEach(e => {
    tbody.innerHTML += `
      <tr>
        <td style="font-family:'JetBrains Mono',monospace;font-size:0.85rem;">${e.table}</td>
        <td>—</td>
        <td>—</td>
        <td><span class="status-badge error">✗ ${e.error.substring(0, 50)}</span></td>
      </tr>
    `;
  });

  if (errors.length > 0) {
    document.getElementById("resultIcon").textContent = "⚠️";
    document.getElementById("resultTitle").textContent = "Migration Completed with Warnings";
    document.getElementById("resultMessage").textContent = `${tables.length} tables migrated, ${errors.length} errors encountered.`;
  }
}

function showError(error) {
  document.getElementById("progressCard").style.display = "none";
  document.getElementById("migrationResults").style.display = "block";
  document.getElementById("resultIcon").textContent = "❌";
  document.getElementById("resultTitle").textContent = "Migration Failed";
  document.getElementById("resultMessage").textContent = error;
}

// ─── Start Over ──────────────────────────────────────────────────────────────
function startOver() {
  state.currentStep = 1;
  state.sourceDb = null;
  state.targetDb = null;
  state.sessionId = null;
  state.schema = null;
  state.plan = null;
  state.migrationId = null;
  state.sourceConfig = {};
  state.targetConfig = {};

  // Reset UI
  document.querySelectorAll(".db-option").forEach(el => el.classList.remove("selected"));
  document.getElementById("pipelineConfirm").style.display = "none";
  document.getElementById("progressCard").style.display = "block";
  document.getElementById("migrationResults").style.display = "none";
  document.getElementById("progressFill").style.width = "0%";
  document.getElementById("progressPercent").textContent = "0%";
  document.getElementById("progressTable").textContent = "Waiting...";
  document.getElementById("progressCount").textContent = "0 / 0 tables";

  const planStatus = document.getElementById("planStatus");
  planStatus.textContent = "AI Generated";
  planStatus.className = "status-badge running";

  goToStep(1);
}

// ─── Loading Helpers ─────────────────────────────────────────────────────────
function showLoading(text, subtext) {
  document.getElementById("loadingText").textContent = text || "Processing...";
  document.getElementById("loadingSubtext").textContent = subtext || "This may take a moment";
  document.getElementById("loadingOverlay").classList.add("active");
}

function hideLoading() {
  document.getElementById("loadingOverlay").classList.remove("active");
}

// ─── Alert Helper ────────────────────────────────────────────────────────────
function showAlert(containerId, type, message) {
  const container = document.getElementById(containerId);
  if (!container) return;

  const icons = { success: "✅", error: "❌", warning: "⚠️", info: "ℹ️" };
  container.innerHTML = `
    <div class="alert alert-${type}">
      <span class="alert-icon">${icons[type] || ""}</span>
      ${message}
    </div>
  `;
}
