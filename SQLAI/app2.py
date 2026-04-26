"""
QueryVista SQLAI — Unified Dual-Database AI Agent + Migration Pipeline
Supports PostgreSQL, MySQL, MongoDB, CouchDB.
Migration runs FIRST, then dual-DB AI exploration kicks in.
"""

import os
import sys
import json
import math
import uuid
import base64
import tempfile
import io
import traceback
from datetime import datetime

import pandas as pd
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from models import (
    DBConnectionRequest, UserRequest, AnalysisResponse,
    TableDetailsResponse, DashboardResponse, DashboardChart,
    OptimizeRequest, OptimizeResponse, PaginationRequest, PaginationResponse,
    DualDBConnectionRequest, DualDBSchemaResponse, DualQueryRequest,
    DualQueryResponse, SingleDBQueryResult, DualTableDetailRequest,
)
from database_manager import DatabaseManager
from nosql_manager import MongoDBManager, CouchDBManager
from cache_manager import CacheManager
from ai_service import AIService
from viz_service import VizService
from utils import get_hash, get_dialect_name, is_sql_db, is_nosql_db, get_query_language

# --- Import Migration Pipelines ---
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
from pipelines.base import (
    extract_sql_schema,
    extract_mongo_schema,
    extract_couch_schema,
    generate_migration_plan,
    json_dumps as pipeline_json_dumps,
    get_pipeline_logger,
)
from pipelines.mysql_to_mongo import MySQLToMongoPipeline
from pipelines.mysql_to_couchdb import MySQLToCouchDBPipeline
from pipelines.postgres_to_mongo import PostgresToMongoPipeline
from pipelines.postgres_to_couchdb import PostgresToCouchDBPipeline
from pipelines.mongo_to_mysql import MongoToMySQLPipeline
from pipelines.mongo_to_couchdb import MongoToCouchDBPipeline
from pipelines.couchdb_to_mysql import CouchDBToMySQLPipeline
from pipelines.couchdb_to_postgres import CouchDBToPostgresPipeline

migration_logger = get_pipeline_logger("SQLAI.Migration")

# --- Pipeline Registry ---
MIGRATION_PIPELINES = {
    "mysql_to_mongodb": MySQLToMongoPipeline(),
    "mysql_to_couchdb": MySQLToCouchDBPipeline(),
    "postgresql_to_mongodb": PostgresToMongoPipeline(),
    "postgresql_to_couchdb": PostgresToCouchDBPipeline(),
    "mongodb_to_mysql": MongoToMySQLPipeline(),
    "mongodb_to_couchdb": MongoToCouchDBPipeline(),
    "couchdb_to_mysql": CouchDBToMySQLPipeline(),
    "couchdb_to_postgresql": CouchDBToPostgresPipeline(),
}

# In-memory session/migration store
migration_sessions: Dict[str, Dict[str, Any]] = {}
migration_history: List[Dict[str, Any]] = []

# --- Configuration ---
CACHE_DB_URL = os.getenv("CACHE_DB_URL", "")

# --- Services ---
ai_service = AIService()
cache_manager = CacheManager(cache_db_url=CACHE_DB_URL)
db_manager = DatabaseManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        cache_manager.init_cache_db()
    except Exception as e:
        print(f"[WARN] Cache init skipped: {e}")
    yield


app = FastAPI(title="QueryVista SQLAI — Dual-DB Agent", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend")
os.makedirs(FRONTEND_DIR, exist_ok=True)


@app.get("/")
def serve_frontend():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# ─── Helper: Get schema string for any DB type ──────────────────────────────
def _get_schema_str(db_url: str, db_name: Optional[str] = None) -> str:
    """Fetch schema as a string for LLM context, regardless of DB type."""
    dialect = get_dialect_name(db_url)
    if dialect == "mongodb":
        name = db_name or MongoDBManager.get_database_name(db_url)
        return MongoDBManager.fetch_schema(db_url, name)
    elif dialect == "couchdb":
        return CouchDBManager.fetch_schema(db_url)
    else:
        return db_manager.fetch_universal_schema(db_url)


def _get_tables_or_collections(db_url: str, db_name: Optional[str] = None) -> List[str]:
    """Get list of tables/collections/databases depending on DB type."""
    dialect = get_dialect_name(db_url)
    if dialect == "mongodb":
        name = db_name or MongoDBManager.get_database_name(db_url)
        return MongoDBManager.get_collections(db_url, name)
    elif dialect == "couchdb":
        return CouchDBManager.get_databases(db_url)
    else:
        return db_manager.get_tables(db_url)


def _get_structured_schema(db_url: str, db_name: Optional[str] = None) -> Dict[str, Any]:
    """Get structured schema data for UI display."""
    dialect = get_dialect_name(db_url)
    if dialect == "mongodb":
        name = db_name or MongoDBManager.get_database_name(db_url)
        return MongoDBManager.get_structured_schema(db_url, name)
    elif dialect == "couchdb":
        return CouchDBManager.get_structured_schema(db_url)
    else:
        return db_manager.get_all_schemas(db_url)


def _execute_ai_query(db_url: str, query_text: str, dialect: str, db_name: Optional[str] = None):
    """Execute a query generated by AI against the appropriate database.
    Returns (data_preview, csv_base64, message, error)
    """
    if dialect == "mongodb":
        name = db_name or MongoDBManager.get_database_name(db_url)
        results = MongoDBManager.execute_query(db_url, name, query_text)
        if not results:
            return None, None, "Query executed but returned no data.", None
        df = pd.DataFrame(results)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_b64 = base64.b64encode(csv_buffer.getvalue().encode("utf-8")).decode("utf-8")
        data_preview = json.loads(df.head(20).to_json(orient="records", date_format="iso"))
        return data_preview, csv_b64, f"Retrieved {len(results)} documents.", None

    elif dialect == "couchdb":
        # For CouchDB, we need to extract the target db from the query
        try:
            query_obj = json.loads(query_text)
            target_db = query_obj.get("database", "")
        except Exception:
            target_db = ""
        results = CouchDBManager.execute_query(db_url, target_db, query_text)
        if not results:
            return None, None, "Query executed but returned no data.", None
        df = pd.DataFrame(results)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_b64 = base64.b64encode(csv_buffer.getvalue().encode("utf-8")).decode("utf-8")
        data_preview = json.loads(df.head(20).to_json(orient="records", date_format="iso"))
        return data_preview, csv_b64, f"Retrieved {len(results)} documents.", None

    else:
        # SQL databases
        engine = db_manager.get_engine(db_url)
        is_select = any(query_text.strip().lower().startswith(p) for p in ["select", "with"])
        if is_select:
            with engine.connect() as conn:
                df = pd.read_sql(text(query_text), conn)
            if df.empty:
                return None, None, "Query executed but returned no data.", None
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_b64 = base64.b64encode(csv_buffer.getvalue().encode("utf-8")).decode("utf-8")
            data_preview = json.loads(df.head(20).to_json(orient="records", date_format="iso"))
            return data_preview, csv_b64, f"Retrieved {len(df)} rows.", None
        else:
            with engine.begin() as conn:
                conn.execute(text(query_text))
            return None, None, "Command executed successfully. Database updated.", None


# ═══════════════════════════════════════════════════════════════════════════════
# DUAL-DB ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════


@app.post("/connect-dual", response_model=DualDBSchemaResponse)
def connect_dual_databases(req: DualDBConnectionRequest):
    """Connect to both source and target databases, return schemas & diff."""
    source_dialect = get_dialect_name(req.source_url)
    target_dialect = get_dialect_name(req.target_url)

    source_tables = _get_tables_or_collections(req.source_url, req.source_db_name)
    target_tables = _get_tables_or_collections(req.target_url, req.target_db_name)

    source_schema = _get_structured_schema(req.source_url, req.source_db_name)
    target_schema = _get_structured_schema(req.target_url, req.target_db_name)

    # Compute diff
    diff = _compute_schema_diff(source_tables, target_tables, source_schema, target_schema)

    return DualDBSchemaResponse(
        source_dialect=source_dialect,
        target_dialect=target_dialect,
        source_tables=source_tables,
        target_tables=target_tables,
        source_schema=source_schema,
        target_schema=target_schema,
        diff=diff,
    )


def _compute_schema_diff(source_tables, target_tables, source_schema, target_schema):
    """Compare schemas between source and target databases."""
    source_set = set(source_tables)
    target_set = set(target_tables)

    only_in_source = list(source_set - target_set)
    only_in_target = list(target_set - source_set)
    in_both = list(source_set & target_set)

    def _extract_field_names(entry):
        """Extract field names from various schema formats."""
        if isinstance(entry, list):
            # Old format: list of {name, type} dicts
            return {f["name"] for f in entry if isinstance(f, dict) and "name" in f}
        if isinstance(entry, dict):
            # New SQL format: {"columns": [...], "primary_keys": [...], ...}
            if "columns" in entry:
                cols = entry["columns"]
                if isinstance(cols, list):
                    return {(c["name"] if isinstance(c, dict) else c) for c in cols}
            # NoSQL format: {"fields": [...], ...}
            if "fields" in entry:
                fields = entry["fields"]
                if isinstance(fields, list):
                    return {f["name"] for f in fields if isinstance(f, dict) and "name" in f}
                if isinstance(fields, dict):
                    return set(fields.keys())
        return set()

    field_diffs = {}
    for name in in_both:
        src = source_schema.get(name, {})
        tgt = target_schema.get(name, {})

        src_fields = _extract_field_names(src)
        tgt_fields = _extract_field_names(tgt)

        if src_fields or tgt_fields:
            field_diffs[name] = {
                "only_in_source": list(src_fields - tgt_fields),
                "only_in_target": list(tgt_fields - src_fields),
                "in_both": list(src_fields & tgt_fields),
            }

    return {
        "only_in_source": only_in_source,
        "only_in_target": only_in_target,
        "in_both": in_both,
        "field_diffs": field_diffs,
    }


@app.post("/generate-dual", response_model=DualQueryResponse)
def generate_dual_query(req: DualQueryRequest):
    """Generate and execute queries for BOTH databases from one NL query."""
    source_dialect = get_dialect_name(req.source_url)
    target_dialect = get_dialect_name(req.target_url)
    source_ql = get_query_language(source_dialect)
    target_ql = get_query_language(target_dialect)

    # Fetch schemas for both
    source_schema_str = _get_schema_str(req.source_url, req.source_db_name)
    target_schema_str = _get_schema_str(req.target_url, req.target_db_name)

    if not source_schema_str and not target_schema_str:
        return DualQueryResponse(
            natural_language=req.query,
            error="Could not fetch schema from either database.",
        )

    # ─── Generate SOURCE query ───
    source_result = _generate_and_execute_for_db(
        db_url=req.source_url,
        db_name=req.source_db_name,
        dialect=source_dialect,
        query_language=source_ql,
        schema_str=source_schema_str,
        nl_query=req.query,
        safe_mode=req.safe_mode,
        label="Source",
    )

    # ─── Generate TARGET query ───
    target_result = _generate_and_execute_for_db(
        db_url=req.target_url,
        db_name=req.target_db_name,
        dialect=target_dialect,
        query_language=target_ql,
        schema_str=target_schema_str,
        nl_query=req.query,
        safe_mode=req.safe_mode,
        label="Target",
    )

    return DualQueryResponse(
        natural_language=req.query,
        source_result=source_result,
        target_result=target_result,
    )


def _generate_and_execute_for_db(
    db_url: str,
    db_name: Optional[str],
    dialect: str,
    query_language: str,
    schema_str: str,
    nl_query: str,
    safe_mode: bool,
    label: str,
) -> SingleDBQueryResult:
    """Generate a query for a specific DB, execute it, and return results."""
    if not schema_str:
        return SingleDBQueryResult(
            dialect=dialect,
            query_text="",
            query_language=query_language,
            error=f"Could not fetch {label} database schema.",
        )

    mode_instructions = "STRICTLY READ-ONLY. No mutations." if safe_mode else "UNRESTRICTED MODE."

    if is_sql_db(dialect):
        system_prompt = f"""You are a {dialect.upper()} SQL Expert.
Schema: {schema_str}
MODE: {mode_instructions}
Rules:
- Return strictly raw SQL. No markdown, no explanation.
- Handle date comparisons using dialect-specific functions.
- For {dialect.upper()} syntax only.
"""
    elif dialect == "mongodb":
        system_prompt = f"""You are a MongoDB Query Expert. You must generate executable MongoDB queries based on the user's natural language question.

DATABASE SCHEMA AND STRUCTURE:
{schema_str}

MODE: {mode_instructions}

IMPORTANT RULES:
1. You MUST return a valid JSON object. No markdown, no explanation, no code fences.
2. The JSON must have a "collection" key with the exact collection name from the schema above.
3. Use either:
   a) "pipeline" key with an array of MongoDB aggregation stages, OR
   b) "filter" key with a find query object, plus optional "projection", "sort", "limit"
4. Use the EXACT field names from the schema above. Field names are CASE-SENSITIVE.
5. For counting, use {{"$group": {{"_id": null, "count": {{"$sum": 1}}}}}} in a pipeline.
6. For filtering by string values, match the EXACT format shown in the sample values.
7. For date fields, use ISODate-compatible string format.
8. Always include {{"$limit": 100}} at the end of pipelines unless the user asks for all results.
9. When projecting, explicitly exclude _id: {{"$project": {{"_id": 0, "field1": 1, "field2": 1}}}}

EXAMPLES:
- "Show all users": {{"collection": "users", "pipeline": [{{"$project": {{"_id": 0}}}}, {{"$limit": 100}}]}}
- "Count orders by status": {{"collection": "orders", "pipeline": [{{"$group": {{"_id": "$status", "count": {{"$sum": 1}}}}}}]}}
- "Find users named John": {{"collection": "users", "filter": {{"name": "John"}}, "projection": {{"_id": 0}}, "limit": 10}}
"""
    elif dialect == "couchdb":
        system_prompt = f"""You are a CouchDB Mango Query Expert. You must generate executable CouchDB Mango queries based on the user's natural language question.

DATABASE SCHEMA AND STRUCTURE:
{schema_str}

MODE: {mode_instructions}

IMPORTANT RULES:
1. You MUST return a valid JSON object. No markdown, no explanation, no code fences.
2. The JSON must have a "database" key with the exact database name from the schema above.
3. The JSON must have a "selector" key with a CouchDB Mango selector object.
4. Optionally include "fields" (array of field names to return), "sort" (array), "limit" (number).
5. Use the EXACT field names from the schema above. Field names are CASE-SENSITIVE.
6. For selecting all documents, use {{"selector": {{}}}} or {{"selector": {{"_id": {{"$gt": null}}}}}}.
7. For string matching, use exact values or $regex operator.
8. Default limit to 100 if user doesn't specify.
9. Use only valid Mango operators: $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin, $exists, $regex, $and, $or, $not

EXAMPLES:
- "Show all users": {{"database": "users", "selector": {{}}, "limit": 100}}
- "Find users named John": {{"database": "users", "selector": {{"name": "John"}}, "limit": 10}}
- "Users with age > 25": {{"database": "users", "selector": {{"age": {{"$gt": 25}}}}, "fields": ["name", "age"], "limit": 50}}
"""
    else:
        system_prompt = f"""You are a database query expert for {dialect}.
Schema: {schema_str}
MODE: {mode_instructions}
Return only the raw query. No markdown."""

    query_text = ai_service.ai_call(system_prompt, nl_query)
    if not query_text:
        return SingleDBQueryResult(
            dialect=dialect,
            query_text="",
            query_language=query_language,
            error=f"AI failed to generate {label} query.",
        )

    # Execute
    try:
        data_preview, csv_b64, message, error = _execute_ai_query(
            db_url, query_text, dialect, db_name
        )

        row_count = len(data_preview) if data_preview else 0

        # Generate visualizations for SQL results
        graphs = []
        if data_preview and is_sql_db(dialect):
            try:
                df = pd.DataFrame(data_preview)
                with tempfile.TemporaryDirectory() as temp_dir:
                    graphs = VizService.generate_visualizations(df, nl_query, ai_service, temp_dir)
            except Exception:
                pass

        return SingleDBQueryResult(
            dialect=dialect,
            query_text=query_text,
            query_language=query_language,
            data_preview=data_preview,
            row_count=row_count,
            message=message,
            csv_base64=csv_b64,
            graphs_base64=graphs,
        )
    except Exception as e:
        # Try self-healing — works for both SQL and NoSQL
        try:
            if is_sql_db(dialect):
                fixed_query = ai_service.fix_sql(query_text, str(e), schema_str, dialect)
            else:
                fixed_query = ai_service.fix_nosql_query(query_text, str(e), schema_str, dialect)

            if fixed_query:
                data_preview, csv_b64, message, error = _execute_ai_query(
                    db_url, fixed_query, dialect, db_name
                )
                row_count = len(data_preview) if data_preview else 0
                return SingleDBQueryResult(
                    dialect=dialect,
                    query_text=fixed_query,
                    query_language=query_language,
                    data_preview=data_preview,
                    row_count=row_count,
                    message=f"{message} (auto-corrected)" if message else "Auto-corrected query.",
                    csv_base64=csv_b64,
                )
        except Exception:
            pass

        return SingleDBQueryResult(
            dialect=dialect,
            query_text=query_text,
            query_language=query_language,
            error=f"Query Error: {str(e)}",
        )




@app.post("/table-details-dual")
def get_table_details_dual(req: DualTableDetailRequest):
    """Get table/collection details for any DB type."""
    dialect = get_dialect_name(req.db_url)

    if dialect == "mongodb":
        db_name = MongoDBManager.get_database_name(req.db_url)
        collection_name = req.db_name or ""
        return MongoDBManager.get_collection_details(req.db_url, db_name, collection_name)
    elif dialect == "couchdb":
        return CouchDBManager.get_database_details(req.db_url, req.db_name or "")
    else:
        details = db_manager.get_table_details(req.db_url, req.db_name or "", dialect)
        return TableDetailsResponse(**details)


@app.post("/table-data-dual")
def get_table_data_dual(req: DualTableDetailRequest):
    """Get paginated data for any DB type."""
    dialect = get_dialect_name(req.db_url)
    table_name = req.db_name or ""

    if dialect == "mongodb":
        db_name_parsed = MongoDBManager.get_database_name(req.db_url)
        return MongoDBManager.get_collection_data(req.db_url, db_name_parsed, table_name, req.page, req.limit)
    elif dialect == "couchdb":
        return CouchDBManager.get_database_data(req.db_url, table_name, req.page, req.limit)
    else:
        try:
            existing_tables = db_manager.get_tables(req.db_url)
            if table_name not in existing_tables:
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

            engine = db_manager.get_engine(req.db_url)
            dl = dialect.lower()

            count_sql = f"SELECT COUNT(*) FROM {table_name}"
            with engine.connect() as conn:
                total_rows = conn.execute(text(count_sql)).scalar()
                req.page = max(1, req.page)
                offset = (req.page - 1) * req.limit
                total_pages = math.ceil(total_rows / req.limit) if total_rows > 0 else 1

                if "mssql" in dl or "sqlserver" in dl:
                    data_sql = f"SELECT * FROM {table_name} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {req.limit} ROWS ONLY"
                elif "oracle" in dl:
                    data_sql = f"SELECT * FROM {table_name} OFFSET {offset} ROWS FETCH NEXT {req.limit} ROWS ONLY"
                else:
                    data_sql = f"SELECT * FROM {table_name} LIMIT {req.limit} OFFSET {offset}"

                df = pd.read_sql(text(data_sql), conn)
                data_json = json.loads(df.to_json(orient="records", date_format="iso"))

                return PaginationResponse(
                    data=data_json,
                    total_rows=total_rows,
                    page=req.page,
                    total_pages=total_pages,
                )
        except HTTPException:
            raise
        except Exception as e:
            return PaginationResponse(
                data=[], total_rows=0, page=0, total_pages=0,
                error=f"Error fetching data: {str(e)}",
            )


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLE-DB ENDPOINTS (backward compatibility)
# ═══════════════════════════════════════════════════════════════════════════════


@app.post("/schemas")
def get_all_schemas(req: DBConnectionRequest):
    return {"tables": _get_tables_or_collections(req.db_url)}


@app.post("/schemas/{table_name}", response_model=TableDetailsResponse)
def get_table_details(table_name: str, req: DBConnectionRequest):
    dialect = get_dialect_name(req.db_url)
    if dialect == "mongodb":
        db_name = MongoDBManager.get_database_name(req.db_url)
        return MongoDBManager.get_collection_details(req.db_url, db_name, table_name)
    elif dialect == "couchdb":
        return CouchDBManager.get_database_details(req.db_url, table_name)
    else:
        details = db_manager.get_table_details(req.db_url, table_name, dialect)
        return TableDetailsResponse(**details)


@app.post("/schemas/{table_name}/data", response_model=PaginationResponse)
def get_table_data(table_name: str, req: PaginationRequest):
    dialect = get_dialect_name(req.db_url)
    if dialect == "mongodb":
        db_name = MongoDBManager.get_database_name(req.db_url)
        result = MongoDBManager.get_collection_data(req.db_url, db_name, table_name, req.page, req.limit)
        return PaginationResponse(**result)
    elif dialect == "couchdb":
        result = CouchDBManager.get_database_data(req.db_url, table_name, req.page, req.limit)
        return PaginationResponse(**result)
    else:
        try:
            existing_tables = db_manager.get_tables(req.db_url)
            if table_name not in existing_tables:
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found.")

            engine = db_manager.get_engine(req.db_url)
            dl = dialect.lower()

            with engine.connect() as conn:
                total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                req.page = max(1, req.page)
                offset = (req.page - 1) * req.limit
                total_pages = math.ceil(total_rows / req.limit) if total_rows > 0 else 1

                if "mssql" in dl or "sqlserver" in dl:
                    data_sql = f"SELECT * FROM {table_name} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {req.limit} ROWS ONLY"
                elif "oracle" in dl:
                    data_sql = f"SELECT * FROM {table_name} OFFSET {offset} ROWS FETCH NEXT {req.limit} ROWS ONLY"
                else:
                    data_sql = f"SELECT * FROM {table_name} LIMIT {req.limit} OFFSET {offset}"

                df = pd.read_sql(text(data_sql), conn)
                data_json = json.loads(df.to_json(orient="records", date_format="iso"))

                return PaginationResponse(
                    data=data_json, total_rows=total_rows,
                    page=req.page, total_pages=total_pages,
                )
        except HTTPException:
            raise
        except Exception as e:
            return PaginationResponse(
                data=[], total_rows=0, page=0, total_pages=0,
                error=f"Error: {str(e)}",
            )


@app.post("/generate", response_model=AnalysisResponse)
def generate_response(req: UserRequest):
    dialect = get_dialect_name(req.db_url)
    schema_str = _get_schema_str(req.db_url)
    if not schema_str:
        return AnalysisResponse(sql_query="", error="Could not fetch database schema.")

    mode_instructions = "STRICTLY READ-ONLY. SELECT only." if req.safe_mode else "UNRESTRICTED MODE."
    system_prompt = f"""You are a {dialect.upper()} Expert.
Schema: {schema_str}
MODE: {mode_instructions}
Rules:
- Return strictly raw query. No markdown.
- Handle date comparisons using dialect-specific functions.
"""
    query_text = ai_service.ai_call(system_prompt, req.query)
    if not query_text:
        return AnalysisResponse(sql_query="", error="AI failed to generate query.")

    try:
        data_preview, csv_b64, message, error = _execute_ai_query(req.db_url, query_text, dialect)
        graphs = []
        if data_preview:
            try:
                df = pd.DataFrame(data_preview)
                with tempfile.TemporaryDirectory() as temp_dir:
                    graphs = VizService.generate_visualizations(df, req.query, ai_service, temp_dir)
            except Exception:
                pass

        return AnalysisResponse(
            sql_query=query_text,
            message=message,
            data_preview=data_preview,
            graphs_base64=graphs,
            csv_base64=csv_b64,
            error=error,
        )
    except Exception as e:
        return AnalysisResponse(sql_query=query_text, error=f"Execution Error: {str(e)}")


@app.post("/gen-dashboard", response_model=DashboardResponse)
def generate_dashboard(req: DBConnectionRequest):
    dialect = get_dialect_name(req.db_url)
    schema_str = _get_schema_str(req.db_url)
    if not schema_str:
        return DashboardResponse(charts=[], error="Could not fetch database schema.")

    if not is_sql_db(dialect):
        return DashboardResponse(charts=[], error="Dashboard generation is currently supported for SQL databases only.")

    strategy_prompt = f"""You are a Senior Data Scientist using {dialect.upper()} SQL.
Your goal is to create a professional dashboard with 5 distinct, high-value insights.
Schema: {schema_str}
Task: Generate a JSON list of 5 objects. Each must have:
1. 'title': A business title for the chart.
2. 'description': A 1-sentence insight.
3. 'sql_query': The raw SQL query (must be SELECT).
Format strictly as JSON."""

    try:
        raw_plan = ai_service.ai_call(strategy_prompt, "Generate Dashboard Plan")
        dashboard_plan = json.loads(raw_plan)
        if not isinstance(dashboard_plan, list):
            raise ValueError("AI returned invalid JSON structure")
    except Exception as e:
        return DashboardResponse(charts=[], error=f"Failed to generate dashboard plan: {e}")

    generated_charts = []
    engine = db_manager.get_engine(req.db_url)

    for item in dashboard_plan[:5]:
        try:
            sql = item.get("sql_query")
            title = item.get("title")
            desc = item.get("description")
            with engine.connect() as conn:
                df = pd.read_sql(text(sql), conn)
            if df.empty:
                continue
            with tempfile.TemporaryDirectory() as temp_dir:
                graphs = VizService.generate_visualizations(df, desc, ai_service, temp_dir)
                if graphs:
                    generated_charts.append(DashboardChart(
                        title=title, description=desc, graph_base64=graphs[0],
                    ))
        except Exception as err:
            print(f"Chart generation failed for {item.get('title', 'Unknown')}: {err}")
            continue

    return DashboardResponse(charts=generated_charts)


@app.post("/optimize", response_model=OptimizeResponse)
def optimize_sql(req: OptimizeRequest):
    dialect = get_dialect_name(req.db_url)
    schema_str = _get_schema_str(req.db_url)
    if not schema_str:
        raise HTTPException(status_code=400, detail="Could not fetch database schema.")

    system_prompt = f"""You are a Senior {dialect.upper()} DBA and Performance Expert.
Database Schema: {schema_str}
Task: Analyze and optimize the user's query.
1. Check for syntax errors.
2. Check for logical errors.
3. Optimize for performance.
4. Ensure valid {dialect.upper()} syntax.
Output strictly valid JSON:
{{
    "optimized_sql": "THE_REFINED_QUERY",
    "explanation": "Brief markdown explanation.",
    "difference_score": 0
}}"""

    try:
        response_text = ai_service.ai_call(system_prompt, f"Input Query: {req.query}")
        result = json.loads(response_text)
        return OptimizeResponse(
            original_query=req.query,
            optimized_query=result.get("optimized_sql", req.query),
            explanation=result.get("explanation", "Analysis complete."),
            difference_score=result.get("difference_score", 0),
        )
    except json.JSONDecodeError:
        return OptimizeResponse(
            original_query=req.query,
            optimized_query=req.query,
            explanation="AI Analysis failed to format response correctly.",
            difference_score=0,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Optimization Error: {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════════
# MIGRATION PIPELINE ENDPOINTS — Run migration FIRST, then explore with SQLAI
# ═══════════════════════════════════════════════════════════════════════════════


# --- Migration Pydantic Models ---
from pydantic import BaseModel as _BaseModel, Field as _Field


class MigrationTestConnectionRequest(_BaseModel):
    db_type: str
    connection_url: Optional[str] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


class MigrationExtractSchemaRequest(_BaseModel):
    db_type: str
    connection_url: Optional[str] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


class MigrationGeneratePlanRequest(_BaseModel):
    source_type: str
    target_type: str
    schema_data: Optional[Dict[str, Any]] = None
    schema_text: Optional[str] = None


class MigrationUpdatePlanRequest(_BaseModel):
    session_id: str
    feedback: str


class MigrationApprovePlanRequest(_BaseModel):
    session_id: str


class MigrationExecuteRequest(_BaseModel):
    session_id: str
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]


@app.get("/api/pipelines")
def list_migration_pipelines():
    """List all available migration pipelines."""
    pipelines = []
    for key, pipe in MIGRATION_PIPELINES.items():
        parts = key.split("_to_")
        source, target = parts[0], parts[1]
        pipelines.append({
            "id": key,
            "name": f"{source.upper()} → {target.upper()}",
            "source_type": source,
            "target_type": target,
        })
    return {"pipelines": pipelines}


@app.post("/api/test-connection")
def test_migration_connection(req: MigrationTestConnectionRequest):
    """Test database connection."""
    db_type = req.db_type.lower()
    migration_logger.info(f"Testing connection to {db_type}...")

    config = {}
    if req.connection_url:
        config["connection_url"] = req.connection_url
    if req.host:
        config["host"] = req.host
    if req.username:
        config["username"] = req.username
    if req.password:
        config["password"] = req.password
    if req.database:
        config["database"] = req.database

    for key, pipe in MIGRATION_PIPELINES.items():
        if pipe.source_type == db_type:
            return pipe.test_source_connection(config)
        if pipe.target_type == db_type:
            return pipe.test_target_connection(config)

    # Fallback
    try:
        if db_type in ("mysql", "postgresql"):
            from sqlalchemy import create_engine
            engine = create_engine(config["connection_url"])
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return {"success": True, "message": f"{db_type} connection successful"}
        elif db_type == "mongodb":
            import pymongo
            client = pymongo.MongoClient(config["connection_url"], serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
            return {"success": True, "message": "MongoDB connection successful"}
        elif db_type == "couchdb":
            import httpx
            r = httpx.get(f"{config['host']}/", auth=(config["username"], config["password"]), timeout=10)
            r.raise_for_status()
            return {"success": True, "message": "CouchDB connection successful"}
    except Exception as e:
        return {"success": False, "message": str(e)}

    raise HTTPException(status_code=400, detail=f"Unsupported: {db_type}")


@app.post("/api/extract-schema")
def extract_migration_schema(req: MigrationExtractSchemaRequest):
    """Extract schema from source database."""
    db_type = req.db_type.lower()
    migration_logger.info(f"Extracting schema for {db_type}...")

    try:
        if db_type in ("mysql", "postgresql"):
            schema = extract_sql_schema(req.connection_url)
        elif db_type == "mongodb":
            schema = extract_mongo_schema(req.connection_url, req.database)
        elif db_type == "couchdb":
            schema = extract_couch_schema(req.host, req.username, req.password)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported: {db_type}")

        session_id = str(uuid.uuid4())
        migration_sessions[session_id] = {
            "id": session_id,
            "source_type": db_type,
            "schema": schema,
            "plan": None,
            "approved": False,
            "created_at": datetime.now().isoformat(),
        }

        migration_logger.info(f"Schema extracted. {len(schema)} entities. Session: {session_id}")

        return {
            "success": True,
            "session_id": session_id,
            "schema": json.loads(pipeline_json_dumps(schema)),
            "table_count": len(schema),
            "tables": list(schema.keys()),
        }
    except HTTPException:
        raise
    except Exception as e:
        migration_logger.error(f"Schema extraction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/generate-plan")
def gen_migration_plan(req: MigrationGeneratePlanRequest):
    """Generate AI migration plan using Azure OpenAI."""
    try:
        schema_text = req.schema_text
        if not schema_text and req.schema_data:
            schema_text = pipeline_json_dumps(req.schema_data)

        if not schema_text:
            raise HTTPException(status_code=400, detail="Provide schema_data or schema_text")

        migration_logger.info(f"Generating AI plan for {req.source_type} → {req.target_type}...")
        plan = generate_migration_plan(req.source_type, req.target_type, schema_text)

        session_id = str(uuid.uuid4())
        migration_sessions[session_id] = {
            "id": session_id,
            "source_type": req.source_type,
            "target_type": req.target_type,
            "schema_text": schema_text,
            "plan": plan,
            "approved": False,
            "created_at": datetime.now().isoformat(),
        }

        return {"success": True, "session_id": session_id, "plan": plan}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/update-plan")
def update_migration_plan(req: MigrationUpdatePlanRequest):
    """HITL: Send feedback to AI to refine the migration plan."""
    session = migration_sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        existing_plan = pipeline_json_dumps(session["plan"])
        schema_text = session.get("schema_text", "")

        updated = generate_migration_plan(
            session["source_type"],
            session["target_type"],
            schema_text,
            feedback=req.feedback,
            existing_plan=existing_plan,
        )

        session["plan"] = updated
        return {"success": True, "plan": updated}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/approve-plan")
def approve_migration_plan(req: MigrationApprovePlanRequest):
    """Approve the migration plan."""
    session = migration_sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    session["approved"] = True
    return {"success": True, "message": "Plan approved. Ready to execute."}


@app.post("/api/execute-migration")
def execute_migration(req: MigrationExecuteRequest, bg: BackgroundTasks):
    """Execute the approved migration in the background."""
    session = migration_sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if not session.get("approved"):
        raise HTTPException(status_code=400, detail="Plan not yet approved")

    source_type = session["source_type"]
    target_type = session["target_type"]
    pipeline_key = f"{source_type}_to_{target_type}"

    pipeline = MIGRATION_PIPELINES.get(pipeline_key)
    if not pipeline:
        raise HTTPException(status_code=400, detail=f"No pipeline for {pipeline_key}")

    migration_id = str(uuid.uuid4())
    session["migration_id"] = migration_id
    session["status"] = "running"
    session["progress"] = {"current": 0, "total": 0, "current_table": ""}
    session["source_config"] = req.source_config
    session["target_config"] = req.target_config

    migration_logger.info(f"[{migration_id}] Starting {pipeline_key} migration...")

    def run_migration():
        try:
            def on_progress(current, total, table):
                session["progress"] = {
                    "current": current,
                    "total": total,
                    "current_table": table,
                }
                migration_logger.info(f"[{migration_id}] ({current}/{total}) Migrated: {table}")

            try:
                from backend.pipelines.dynamic_executor import execute_dynamic_migration
                migration_logger.info(f"[{migration_id}] Attempting dynamic LLM-generated script execution...")
                result = execute_dynamic_migration(
                    source_type=source_type,
                    target_type=target_type,
                    source_config=req.source_config,
                    target_config=req.target_config,
                    plan=session["plan"],
                    on_progress=on_progress,
                )
            except Exception as dyn_e:
                migration_logger.warning(f"[{migration_id}] Dynamic script execution failed: {dyn_e}. Falling back to standard pipeline...")
                result = pipeline.execute(
                    req.source_config, req.target_config, session["plan"],
                    on_progress=on_progress,
                )
            session["status"] = "completed"
            session["result"] = result

            migration_history.append({
                "id": migration_id,
                "pipeline": pipeline_key,
                "source_type": source_type,
                "target_type": target_type,
                "status": "completed",
                "result": result,
                "completed_at": datetime.now().isoformat(),
            })
            migration_logger.info(f"[{migration_id}] Migration completed!")
        except Exception as e:
            session["status"] = "failed"
            session["error"] = str(e)
            session["traceback"] = traceback.format_exc()
            migration_history.append({
                "id": migration_id,
                "pipeline": pipeline_key,
                "status": "failed",
                "error": str(e),
                "completed_at": datetime.now().isoformat(),
            })
            migration_logger.error(f"[{migration_id}] Migration failed: {e}")

    bg.add_task(run_migration)

    return {
        "success": True,
        "migration_id": migration_id,
        "session_id": req.session_id,
        "message": "Migration started in background",
        "status": "running",
    }


@app.get("/api/migration-status/{session_id}")
def migration_status(session_id: str):
    """Get migration progress."""
    session = migration_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return {
        "session_id": session_id,
        "status": session.get("status", "idle"),
        "progress": session.get("progress"),
        "result": session.get("result"),
        "error": session.get("error"),
        "source_config": session.get("source_config"),
        "target_config": session.get("target_config"),
        "source_type": session.get("source_type"),
        "target_type": session.get("target_type"),
    }


@app.get("/api/migration-history")
def get_migration_history():
    """List completed/failed migrations."""
    return {"migrations": migration_history}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)