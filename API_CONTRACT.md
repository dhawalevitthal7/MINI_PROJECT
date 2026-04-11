# 🔌 QueryVista API Contract & Architecture

This document describes the primary RESTful API endpoints powering the **QueryVista FastAPI Backend (`app2.py`)**. The API is conceptually divided into two interconnected domains: **Migration Pipelines** (ETL) and **SQLAI Dual-Intelligence** (Analytics/Exploration).

---

## 🚀 Domain 1: Migration Pipeline Methods (ETL)
These endpoints serve the 5-step interactive migration wizard, allowing for extraction, schema display, plan formulation via LLM, and execution.

### 1. `GET /api/pipelines`
- **Description:** Retrieves all available ETL migration directions (e.g. `postgresql_to_couchdb`, `mysql_to_mongodb`). 
- **Request:** (None)
- **Response:** JSON list of supported source/target configurations.

### 2. `POST /api/test-connection`
- **Description:** Verifies database accessibility and valid credentials before attempting any migration tasks.
- **Request Body:**
  ```json
  {
    "db_type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "user": "root",
    "password": "password",
    "database": "sales_db"
  }
  ```
- **Response:** `{ "status": "success", "message": "Connection successful" }` or error.

### 3. `POST /api/extract-schema`
- **Description:** Inspects the Source database. For SQL, maps relationships, PKs, FKs, and indexes. For NoSQL, it infers document mappings by sampling.
- **Request Body:** Connection credentials JSON (same as test-connection).
- **Response:**
  ```json
  {
    "session_id": "uuid-1234",
    "table_count": 10,
    "schema": {
        "users": {"columns": [{"name": "id", "type": "int"}...], "primary_keys": ["id"], "foreign_keys": []}
    }
  }
  ```

### 4. `POST /api/generate-plan`
- **Description:** Submits the extracted schema to Azure OpenAI to synthesize a detailed JSON translation plan outlining exactly how to morph the source data into the target database type constraints.
- **Request Body:**
  ```json
  {
    "source_type": "postgresql",
    "target_type": "mongodb",
    "schema_data": { /* JSON Payload from /extract-schema */ }
  }
  ```
- **Response:** `{ "session_id": "uuid", "plan": { "collections_to_create": [...], "mappings": [...] } }`

### 5. `POST /api/update-plan`
- **Description:** The Human-In-The-Loop modifier. Submits human language feedback to explicitly adjust the migration blueprint.
- **Request Body:** `{ "session_id": "uuid", "feedback": "Embed addresses inside the users collection" }`
- **Response:** Regenerated `plan` JSON object conforming to the constraints plus the feedback.

### 6. `POST /api/execute-migration`
- **Description:** Triggers the underlying Python ETL workers (like `postgres_to_mongo.py`) to launch the data batching based on the approved AI generated blueprint mapping.
- **Request Body:** `{ "session_id": "uuid", "source_config": {}, "target_config": {} }`

### 7. `GET /api/migration-status/{session_id}`
- **Description:** Polling endpoint for the frontend progress bar to show percentage metrics and row-counts during data-hydration.

---

## 📊 Domain 2: SQLAI Dual-Database Intelligence Methods
These endpoints power post-migration analytics. They allow single-point interactions across two completely different platform ecosystems simultaneously.

### 8. `POST /connect-dual`
- **Description:** Initializes the post-migration diffing environment by connecting to both the legacy database and modernized database.
- **Request Body:**
  ```json
  {
    "source_url": "postgresql://...",
    "target_url": "mongodb://...",
    "source_db_name": "sales_db",
    "target_db_name": "sales_mongo"
  }
  ```
- **Response:** A massive schema map representing differences (`diff`), identical data lengths, schemas, and dialects.

### 9. `POST /generate-dual`
- **Description:** The powerhouse Natural Language endpoint. Converts plain English into two discrete database languages, fires them at their respective instances, and returns matching DataFrames for QA testing.
- **Request Body:** 
  ```json
  {
    "query": "Show me total active users who made a purchase over $500",
    "source_url": "...", "target_url": "...",
    "source_db_name": "...", "target_db_name": "...",
    "safe_mode": true
  }
  ```
- **Response:**
  ```json
  {
    "source_result": { "query_text": "SELECT...", "data_preview": [...], "csv_base64": "..." },
    "target_result": { "query_text": "db.collection.aggregate(...)", "data_preview": [...], ... },
    "explanation": "I joined the users table and transactions collection..."
  }
  ```

### 10. `POST /table-details-dual` & `POST /table-data-dual`
- **Description:** Explorer routes. Retrieves the detailed UI elements (Foreign keys, documents, rows, indices) explicitly highlighting primary keys and references for the Visualizer, alongside paginated deep data sets (Limit 20/page).
- **Request Body:** `{ "db_url": "...", "db_name": "specific_table_or_collection" }`

### 11. `POST /optimize`
- **Description:** Sends an expensive database query (either SQL or JSON MQL) to Azure OpenAI with instructions to rewrite the command targeting query planners for efficiency (i.e., avoiding full table scans, predicting indexes).
- **Request Body:** `{ "query": "SELECT * from X", "dialect": "postgresql", "schema_str": "{...}" }`
- **Response:** Returns an `optimized_query` and a markdown paragraph containing `reasoning` and indexing suggestions.

### 12. `POST /gen-dashboard`
- **Description:** Auto-creates an analytics interface utilizing high level schemas to predict what business logic stakeholders would want to see charted over time.
