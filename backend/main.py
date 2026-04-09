"""
QueryVista — FastAPI Backend
AI-Powered Database Migration Platform
"""

import os
import uuid
import json
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from pipelines.base import (
    extract_sql_schema,
    extract_mongo_schema,
    extract_couch_schema,
    generate_migration_plan,
    json_dumps,
    get_pipeline_logger,
)

logger = get_pipeline_logger("API")
from pipelines.mysql_to_mongo import MySQLToMongoPipeline
from pipelines.mysql_to_couchdb import MySQLToCouchDBPipeline
from pipelines.postgres_to_mongo import PostgresToMongoPipeline
from pipelines.postgres_to_couchdb import PostgresToCouchDBPipeline
from pipelines.mongo_to_mysql import MongoToMySQLPipeline
from pipelines.mongo_to_postgres import MongoToPostgresPipeline
from pipelines.mongo_to_couchdb import MongoToCouchDBPipeline
from pipelines.couchdb_to_mysql import CouchDBToMySQLPipeline
from pipelines.couchdb_to_postgres import CouchDBToPostgresPipeline


# ─── App setup ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="QueryVista API",
    description="AI-Powered Database Migration Platform",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Pipeline Registry ───────────────────────────────────────────────────────
PIPELINES = {
    "mysql_to_mongodb": MySQLToMongoPipeline(),
    "mysql_to_couchdb": MySQLToCouchDBPipeline(),
    "postgresql_to_mongodb": PostgresToMongoPipeline(),
    "postgresql_to_couchdb": PostgresToCouchDBPipeline(),
    "mongodb_to_mysql": MongoToMySQLPipeline(),
    "mongodb_to_postgresql": MongoToPostgresPipeline(),
    "mongodb_to_couchdb": MongoToCouchDBPipeline(),
    "couchdb_to_mysql": CouchDBToMySQLPipeline(),
    "couchdb_to_postgresql": CouchDBToPostgresPipeline(),
}

# In-memory store for sessions and migration state
sessions: Dict[str, Dict[str, Any]] = {}
migration_history: List[Dict[str, Any]] = []


# ─── Request/Response Models ─────────────────────────────────────────────────
class TestConnectionRequest(BaseModel):
    db_type: str  # mysql, postgresql, mongodb, couchdb
    connection_url: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


class ExtractSchemaRequest(BaseModel):
    db_type: str
    connection_url: Optional[str] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


class GeneratePlanRequest(BaseModel):
    source_type: str
    target_type: str
    schema_data: Optional[Dict[str, Any]] = None
    schema_text: Optional[str] = None


class UpdatePlanRequest(BaseModel):
    session_id: str
    feedback: str


class ApprovePlanRequest(BaseModel):
    session_id: str


class ExecuteMigrationRequest(BaseModel):
    session_id: str
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]


class FullMigrationRequest(BaseModel):
    """One-shot migration: provide everything and go."""
    source_type: str
    target_type: str
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    plan: Optional[Dict[str, Any]] = None


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/")
def health():
    return {
        "status": "running",
        "app": "QueryVista API",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/databases")
def list_databases():
    """List all supported database types."""
    return {
        "databases": [
            {
                "id": "mysql",
                "name": "MySQL",
                "type": "sql",
                "icon": "🐬",
                "config_fields": ["connection_url"],
                "description": "MySQL / MariaDB relational database",
            },
            {
                "id": "postgresql",
                "name": "PostgreSQL",
                "type": "sql",
                "icon": "🐘",
                "config_fields": ["connection_url"],
                "description": "PostgreSQL relational database (supports Neon cloud)",
            },
            {
                "id": "mongodb",
                "name": "MongoDB",
                "type": "nosql",
                "icon": "🍃",
                "config_fields": ["connection_url", "database"],
                "description": "MongoDB document database (supports Atlas cloud)",
            },
            {
                "id": "couchdb",
                "name": "CouchDB",
                "type": "nosql",
                "icon": "🛋️",
                "config_fields": ["host", "username", "password"],
                "description": "Apache CouchDB document database",
            },
        ]
    }


@app.get("/api/pipelines")
def list_pipelines():
    """List all available migration pipelines."""
    pipelines = []
    for key, pipe in PIPELINES.items():
        source, target = key.split("_to_")
        pipelines.append({
            "id": key,
            "name": f"{source.upper()} → {target.upper()}",
            "source_type": source,
            "target_type": target,
            "description": f"Migrate data from {source} to {target}",
        })
    return {"pipelines": pipelines}


@app.post("/api/test-connection")
def test_connection(req: TestConnectionRequest):
    """Test database connection."""
    db_type = req.db_type.lower()
    logger.info(f"Received test-connection request for DB: {db_type}")

    # Build config
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

    # Find a pipeline that uses this db type as source or target
    for key, pipe in PIPELINES.items():
        if pipe.source_type == db_type:
            return pipe.test_source_connection(config)
        if pipe.target_type == db_type:
            return pipe.test_target_connection(config)

    # Fallback: direct test
    try:
        if db_type in ("mysql", "postgresql"):
            from sqlalchemy import create_engine, text
            engine = create_engine(config["connection_url"])
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            logger.info(f"Successfully connected to {db_type}")
            return {"success": True, "message": f"{db_type} connection successful"}
        elif db_type == "mongodb":
            import pymongo
            client = pymongo.MongoClient(config["connection_url"], serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
            logger.info(f"Successfully connected to {db_type}")
            return {"success": True, "message": "MongoDB connection successful"}
        elif db_type == "couchdb":
            import httpx
            r = httpx.get(f"{config['host']}/", auth=(config["username"], config["password"]), timeout=10)
            r.raise_for_status()
            logger.info(f"Successfully connected to {db_type}")
            return {"success": True, "message": "CouchDB connection successful"}
    except Exception as e:
        logger.error(f"Failed to connect to {db_type}: {e}")
        return {"success": False, "message": str(e)}

    raise HTTPException(status_code=400, detail=f"Unsupported database type: {db_type}")


@app.post("/api/extract-schema")
def extract_schema(req: ExtractSchemaRequest):
    """Extract schema from a database."""
    db_type = req.db_type.lower()
    logger.info(f"Extracting schema for DB: {db_type}...")

    try:
        if db_type in ("mysql", "postgresql"):
            schema = extract_sql_schema(req.connection_url)
        elif db_type == "mongodb":
            schema = extract_mongo_schema(req.connection_url, req.database)
        elif db_type == "couchdb":
            schema = extract_couch_schema(req.host, req.username, req.password)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported: {db_type}")

        # Create a session
        session_id = str(uuid.uuid4())
        sessions[session_id] = {
            "id": session_id,
            "source_type": db_type,
            "schema": schema,
            "plan": None,
            "approved": False,
            "created_at": datetime.now().isoformat(),
        }

        logger.info(f"Schema extraction completed for {db_type}. Found {len(schema)} tables/collections. Session ID: {session_id}")

        return {
            "success": True,
            "session_id": session_id,
            "schema": json.loads(json_dumps(schema)),
            "table_count": len(schema),
            "tables": list(schema.keys()),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Schema extraction failed for {db_type}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/generate-plan")
def gen_plan(req: GeneratePlanRequest):
    """Generate AI migration plan."""
    try:
        schema_text = req.schema_text
        if not schema_text and req.schema_data:
            schema_text = json_dumps(req.schema_data)

        if not schema_text:
            raise HTTPException(status_code=400, detail="Provide schema_data or schema_text")

        logger.info(f"Generating AI migration plan for {req.source_type} -> {req.target_type}...")
        plan = generate_migration_plan(req.source_type, req.target_type, schema_text)

        # Create or update session
        session_id = str(uuid.uuid4())
        sessions[session_id] = {
            "id": session_id,
            "source_type": req.source_type,
            "target_type": req.target_type,
            "schema_text": schema_text,
            "plan": plan,
            "approved": False,
            "created_at": datetime.now().isoformat(),
        }

        logger.info(f"Generated AI migration plan. Session ID: {session_id}")

        return {
            "success": True,
            "session_id": session_id,
            "plan": plan,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/update-plan")
def update_plan(req: UpdatePlanRequest):
    """Send feedback to AI to update the migration plan (HITL)."""
    session = sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    try:
        existing_plan = json_dumps(session["plan"])
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
def approve_plan(req: ApprovePlanRequest):
    """Approve the migration plan (HITL approval)."""
    session = sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    session["approved"] = True
    return {"success": True, "message": "Plan approved. Ready to execute."}


@app.post("/api/execute-migration")
def execute_migration(req: ExecuteMigrationRequest, bg: BackgroundTasks):
    """Execute the approved migration."""
    session = sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if not session.get("approved"):
        raise HTTPException(status_code=400, detail="Plan not yet approved")

    source_type = session["source_type"]
    target_type = session["target_type"]
    pipeline_key = f"{source_type}_to_{target_type}"
    
    logger.info(f"Executing migration pipeine: {pipeline_key} for Session: {req.session_id}")

    pipeline = PIPELINES.get(pipeline_key)
    if not pipeline:
        raise HTTPException(status_code=400, detail=f"No pipeline for {pipeline_key}")

    migration_id = str(uuid.uuid4())
    session["migration_id"] = migration_id
    session["status"] = "running"
    session["progress"] = {"current": 0, "total": 0, "current_table": ""}

    def run_migration():
        logger.info(f"[{migration_id}] Starting background execution for {pipeline_key}...")
        try:
            def on_progress(current, total, table):
                session["progress"] = {
                    "current": current,
                    "total": total,
                    "current_table": table,
                }
                logger.info(f"[{migration_id}] Progress ({current}/{total}): Migrated {table}")

            result = pipeline.execute(
                req.source_config,
                req.target_config,
                session["plan"],
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
            logger.info(f"[{migration_id}] Migration completed successfully for {pipeline_key}.")
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
            logger.error(f"[{migration_id}] Migration failed for {pipeline_key}: {e}")

    bg.add_task(run_migration)

    return {
        "success": True,
        "migration_id": migration_id,
        "message": "Migration started in background",
        "status": "running",
    }


@app.get("/api/migration-status/{session_id}")
def migration_status(session_id: str):
    """Get migration progress."""
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return {
        "session_id": session_id,
        "status": session.get("status", "idle"),
        "progress": session.get("progress"),
        "result": session.get("result"),
        "error": session.get("error"),
    }


@app.get("/api/migration-history")
def get_history():
    """List past migrations."""
    return {"migrations": migration_history}


@app.get("/api/session/{session_id}")
def get_session(session_id: str):
    """Get full session details."""
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session
