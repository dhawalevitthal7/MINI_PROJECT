# 🚀 QueryVista — AI-Powered Database Migration Platform

> **Migrate data between any databases with AI-generated schema mapping and human-in-the-loop approval.**

QueryVista is a comprehensive ETL (Extract, Transform, Load) platform that enables companies to migrate data between different database systems — SQL to NoSQL, NoSQL to SQL, or any combination. The platform uses **Azure OpenAI GPT-4o** to intelligently draft migration schemas, provides a **human-in-the-loop review** process, and executes migrations with full validation.

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Supported Migration Pipelines](#supported-migration-pipelines)
- [Pipeline Details & Credentials](#pipeline-details--credentials)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [API Endpoints](#api-endpoints)
- [Frontend Usage](#frontend-usage)
- [User Journey](#user-journey)

---

## 🏗️ Architecture Overview

Every migration pipeline follows a **4-phase architecture**:

```
Phase 1: DISCOVERY      → Extract schema/metadata from source DB
Phase 2: AI ARCHITECT   → Azure GPT-4o drafts migration plan (JSON blueprint)
Phase 3: HUMAN REVIEW   → User reviews, edits, and approves the plan
Phase 4: EXECUTION      → ETL engine migrates data with validation
```

```mermaid
graph LR
    A[Source DB] --> B[Schema Extraction]
    B --> C[AI Migration Plan]
    C --> D[Human Review & Approval]
    D --> E[ETL Execution]
    E --> F[Target DB]
    E --> G[Validation Report]
```

---

## 🔄 Supported Migration Pipelines

| # | Pipeline Name | Source DB | Target DB | Notebook/Script |
|---|--------------|-----------|-----------|-----------------|
| 1 | `mysql_to_couchdb` | MySQL | Apache CouchDB | `mysql_to_couchdb_pipeline (3) (1).ipynb` |
| 2 | `postgres_to_couchdb` | PostgreSQL (Neon) | Apache CouchDB | `postgres_to_couchdb_pipeline (1).ipynb` |
| 3 | `mysql_to_mongo` | MySQL | MongoDB Atlas | `mysql_to_mongo.ipynb` |
| 4 | `postgres_to_mongo` | PostgreSQL (Neon) | MongoDB Atlas | `Query_vista.ipynb` / `query_vista_postgrestomongo.py` |
| 5 | `couchdb_to_mysql` | Apache CouchDB | MySQL | `CouchDB-MySQL (1).ipynb` |
| 6 | `couchdb_to_postgres` | Apache CouchDB | PostgreSQL | `CouchDB-PostgreSQL.ipynb` |
| 7 | `mongo_to_mysql` | MongoDB Atlas | MySQL | `Mongo-Sql (1).ipynb` |
| 8 | `mongo_to_couchdb` | MongoDB Atlas | Apache CouchDB | `Mongodb-COuchdb.ipynb` |

---

## 🔐 Pipeline Details & Credentials

### Pipeline 1: `mysql_to_couchdb`

**Direction:** MySQL → Apache CouchDB

| Credential | Value |
|-----------|-------|
| MySQL Host | `localhost` |
| MySQL Port | `3310` (Docker mapped) |
| MySQL User | `user1` |
| MySQL Password | `pass123` |
| MySQL Database | `testdb` |
| MySQL URL | `mysql+pymysql://user1:pass123@localhost:3310/testdb` |
| CouchDB Host | `http://localhost:5984` |
| CouchDB User | `admin` |
| CouchDB Password | `admin123` |
| CouchDB Database | `migrated_db` (auto-created per table) |

**Pipeline Steps:** CSV → MySQL → Schema Extract → AI Plan → Human Review → Migrate → Validate

---

### Pipeline 2: `postgres_to_couchdb`

**Direction:** PostgreSQL (Neon Cloud) → Apache CouchDB

| Credential | Value |
|-----------|-------|
| PostgreSQL URL | Stored in `.env` as `DATABASE_URL` / `SQL_URL` (Neon or any Postgres) |
| PostgreSQL Schema | `public` |
| CouchDB Host | `http://localhost:5984` |
| CouchDB User | `admin` |
| CouchDB Password | `admin123` |

---

### Pipeline 3: `mysql_to_mongo`

**Direction:** MySQL → MongoDB Atlas

| Credential | Value |
|-----------|-------|
| MySQL URL | `mysql+mysqlconnector://etl_user:etl_pass@localhost:3310/etl_db` |
| MySQL Host | `localhost` |
| MySQL Port | `3310` |
| MySQL User | `etl_user` |
| MySQL Password | `etl_pass` |
| MySQL Database | `etl_db` |
| MongoDB URL | Stored in `.env` as `MONGO_URL` |
| MongoDB Database | `mysql_refined_migration` |

---

### Pipeline 4: `postgres_to_mongo`

**Direction:** PostgreSQL (Neon Cloud) → MongoDB Atlas

| Credential | Value |
|-----------|-------|
| PostgreSQL URL | Stored in `.env` as `SQL_URL` / `SQL_URL_ALT` |
| MongoDB URL | Stored in `.env` as `MONGO_URL` |
| MongoDB Database | `migrated_db` |

---

### Pipeline 5: `couchdb_to_mysql`

**Direction:** Apache CouchDB → MySQL

| Credential | Value |
|-----------|-------|
| CouchDB URL | `http://admin:password@localhost:5984` |
| MySQL URL | `mysql+pymysql://user1:pass123@localhost:3310/testdb` |
| Migration Modes | `REPLACE` / `APPEND` / `UPSERT` |

**Features:** Auto MySQL version detection, JSON column support, VARCHAR safety guardrails

---

### Pipeline 6: `couchdb_to_postgres`

**Direction:** Apache CouchDB → PostgreSQL

| Credential | Value |
|-----------|-------|
| CouchDB URL | `http://admin:password@localhost:5984` |
| PostgreSQL URL | `postgresql+psycopg2://postgres:password@localhost:5432/migrated_db` |
| Migration Modes | `REPLACE` / `APPEND` / `UPSERT` |

**Features:** JSONB support, BYTEA for binary, TIMESTAMP for datetime, ON CONFLICT upsert

---

### Pipeline 7: `mongo_to_mysql`

**Direction:** MongoDB Atlas → MySQL

| Credential | Value |
|-----------|-------|
| MongoDB URL | `mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/` |
| MongoDB Database | Source database name |
| MySQL URL | `mysql+pymysql://root:secret@localhost:3306/migrated_db` |
| Migration Modes | `REPLACE` / `APPEND` / `UPSERT` |

**Features:** ObjectId → string conversion, nested object flattening, JSON column auto-detection

---

### Pipeline 8: `mongo_to_couchdb`

**Direction:** MongoDB Atlas → Apache CouchDB

| Credential | Value |
|-----------|-------|
| MongoDB URL | `mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/` |
| MongoDB Database | Source database name |
| CouchDB URL | `http://admin:password@localhost:5984` |
| Migration Modes | `REPLACE` / `APPEND` / `UPSERT` |

**Features:** REST API bulk inserts, _rev handling for upserts, doc_type tagging

---

### Shared: Azure OpenAI Credentials

| Credential | Value |
|-----------|-------|
| Azure Endpoint | `https://openai-04.openai.azure.com/` |
| API Key | Stored in `.env` as `AZURE_API_KEY` |
| API Version | `2024-12-01-preview` |
| Deployment Name | `gpt-4o` |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **AI Engine** | Azure OpenAI GPT-4o |
| **Backend** | FastAPI (Python) |
| **Frontend** | HTML5, CSS3, JavaScript |
| **Source DBs** | MySQL, PostgreSQL (Neon), MongoDB Atlas, Apache CouchDB |
| **Target DBs** | MySQL, PostgreSQL, MongoDB Atlas, Apache CouchDB |
| **ORM** | SQLAlchemy |
| **DB Drivers** | pymysql, psycopg2, pymongo, couchdb (python-couchdb) |
| **Containerization** | Docker Compose (MySQL + CouchDB + phpMyAdmin) |

---

## 📁 Project Structure

```
QueryVista_pipelines/
├── .env                          # All database & API credentials
├── .gitignore
├── README.md                     # This file
├── docker-compose.yml           # MySQL + CouchDB + phpMyAdmin containers
│
├── all_pipelinee/               # All migration pipeline notebooks & scripts
│   ├── mysql_to_couchdb_pipeline (3) (1).ipynb
│   ├── postgres_to_couchdb_pipeline (1).ipynb
│   ├── mysql_to_mongo.ipynb
│   ├── Query_vista.ipynb                    # Postgres → Mongo
│   ├── query_vista_postgrestomongo.py       # Postgres → Mongo (script)
│   ├── CouchDB-MySQL (1).ipynb
│   ├── CouchDB-PostgreSQL.ipynb
│   ├── Mongo-Sql (1).ipynb                  # Mongo → MySQL
│   └── Mongodb-COuchdb.ipynb
│
├── backend/                     # FastAPI backend
│   ├── main.py                  # FastAPI application
│   ├── pipelines/               # Pipeline modules
│   │   ├── __init__.py
│   │   ├── base.py              # Base pipeline class
│   │   ├── mysql_to_mongo.py
│   │   ├── mysql_to_couchdb.py
│   │   ├── postgres_to_mongo.py
│   │   ├── postgres_to_couchdb.py
│   │   ├── mongo_to_mysql.py
│   │   ├── mongo_to_couchdb.py
│   │   ├── couchdb_to_mysql.py
│   │   └── couchdb_to_postgres.py
│   └── requirements.txt
│
├── frontend/                    # Frontend UI
│   ├── index.html
│   ├── style.css
│   └── app.js
│
├── mysql_couch/                 # Docker setup for MySQL ↔ CouchDB
│   └── docker-compose.yml
│
└── postgres_couch/              # Postgres ↔ CouchDB pipeline copy
    └── postgres_to_couchdb_pipeline (1).ipynb
```

---

## ⚡ Setup & Installation

### Prerequisites
- Python 3.10+
- Docker & Docker Compose (for MySQL + CouchDB)
- MongoDB Atlas account (cloud)
- PostgreSQL / Neon account (cloud)
- Azure OpenAI API key

### 1. Clone & Install Dependencies

```bash
cd QueryVista_pipelines
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r backend/requirements.txt
```

### 2. Start Docker Services

```bash
cd mysql_couch
docker-compose up -d
```

This starts:
- **MySQL** on port `3310`
- **CouchDB** on port `5984` (Fauxton UI: http://localhost:5984/_utils)
- **phpMyAdmin** on port `8081`

### 3. Configure Environment Variables

Edit `.env` with your credentials (see Pipeline Details above).

### 4. Run the Backend

```bash
cd backend
uvicorn main:app --reload --port 8000
```

### 5. Open the Frontend

Open `frontend/index.html` in your browser, or serve it:
```bash
cd frontend
python -m http.server 3000
```

---

## 🌐 API Endpoints

### Base URL: `http://localhost:8000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Health check |
| `GET` | `/api/pipelines` | List all available migration pipelines |
| `GET` | `/api/databases` | List all supported database types |
| `POST` | `/api/test-connection` | Test connection to a database |
| `POST` | `/api/extract-schema` | Extract schema/metadata from source DB |
| `POST` | `/api/generate-plan` | AI generates migration plan from schema |
| `POST` | `/api/update-plan` | Send feedback to AI, update migration plan |
| `POST` | `/api/approve-plan` | Approve the migration plan |
| `POST` | `/api/execute-migration` | Execute the approved migration |
| `GET` | `/api/migration-status/{id}` | Get migration progress/status |
| `GET` | `/api/migration-history` | List past migrations |

### Example: Test Connection

```bash
curl -X POST http://localhost:8000/api/test-connection \
  -H "Content-Type: application/json" \
  -d '{
    "db_type": "mysql",
    "host": "localhost",
    "port": 3310,
    "user": "user1",
    "password": "pass123",
    "database": "testdb"
  }'
```

### Example: Extract Schema

```bash
curl -X POST http://localhost:8000/api/extract-schema \
  -H "Content-Type: application/json" \
  -d '{
    "db_type": "mysql",
    "connection_url": "mysql+pymysql://user1:pass123@localhost:3310/testdb"
  }'
```

### Example: Generate Migration Plan

```bash
curl -X POST http://localhost:8000/api/generate-plan \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "mysql",
    "target_type": "mongodb",
    "schema_text": "<schema extracted from previous step>"
  }'
```

---

## 🎯 User Journey

1. **Select Source & Target** — User picks which database to migrate FROM and TO
2. **Connect** — Enter/use hardcoded credentials, test connection
3. **Extract Schema** — System reads source DB schema
4. **AI Plans Migration** — GPT-4o generates a JSON migration blueprint
5. **Human Reviews** — User can modify field mappings, embeddings, drops
6. **Approve & Execute** — Migration runs with progress tracking
7. **Validate** — Side-by-side query comparison of source vs target

---

## 📝 License

This project is for educational and portfolio purposes.

---

Built with ❤️ by the QueryVista team.
