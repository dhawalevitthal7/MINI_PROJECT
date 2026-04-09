"""
QueryVista — Base Pipeline Module
Shared logic for schema extraction, AI plan generation, and ETL execution.
"""

import os
import json
import re
import datetime
import decimal
import logging
import sys
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from openai import AzureOpenAI
from sqlalchemy import create_engine, inspect, text
import pymongo
import httpx

# ─── Logging Setup ───────────────────────────────────────────────────────────
def get_pipeline_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"QueryVista.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s | [%(levelname)s] | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

logger = get_pipeline_logger("Base")


load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

# ─── Azure OpenAI client ─────────────────────────────────────────────────────
_azure_client: Optional[AzureOpenAI] = None

def get_ai_client() -> AzureOpenAI:
    global _azure_client
    if _azure_client is None:
        _azure_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_ENDPOINT", ""),
            api_key=os.getenv("AZURE_API_KEY", ""),
            api_version=os.getenv("AZURE_API_VERSION", "2024-12-01-preview"),
        )
    return _azure_client

DEPLOYMENT = os.getenv("DEPLOYMENT_NAME", "gpt-4o")


# ─── JSON serialization helpers ──────────────────────────────────────────────
def safe_json(obj):
    """Make an object JSON-serializable."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return str(obj)
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", errors="replace")
        except Exception:
            return "<binary>"
    if isinstance(obj, set):
        return list(obj)
    return str(obj)


def json_dumps(obj) -> str:
    return json.dumps(obj, default=safe_json, indent=2)


# ─── SQL Schema Extractor ────────────────────────────────────────────────────
def extract_sql_schema(connection_url: str) -> Dict[str, Any]:
    """Extract full schema metadata from any SQL database via SQLAlchemy."""
    engine = create_engine(connection_url)
    inspector = inspect(engine)

    schema_info = {}
    for table_name in inspector.get_table_names():
        columns = []
        for col in inspector.get_columns(table_name):
            columns.append({
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "default": str(col.get("default")) if col.get("default") else None,
            })

        pks = inspector.get_pk_constraint(table_name)
        fks = inspector.get_foreign_keys(table_name)
        indexes = inspector.get_indexes(table_name)

        schema_info[table_name] = {
            "columns": columns,
            "primary_keys": pks.get("constrained_columns", []) if pks else [],
            "foreign_keys": [
                {
                    "constrained_columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_columns": fk["referred_columns"],
                }
                for fk in fks
            ],
            "indexes": [
                {"name": idx["name"], "columns": idx["column_names"]}
                for idx in indexes
            ],
            "row_count": None,  # populated next
        }

    # Get row counts
    with engine.connect() as conn:
        for table_name in schema_info:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                schema_info[table_name]["row_count"] = result.scalar()
            except Exception:
                try:
                    result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                    schema_info[table_name]["row_count"] = result.scalar()
                except Exception:
                    schema_info[table_name]["row_count"] = "unknown"

    engine.dispose()
    return schema_info


# ─── MongoDB Schema Profiler ─────────────────────────────────────────────────
def extract_mongo_schema(connection_url: str, database: str, sample_size: int = 100) -> Dict[str, Any]:
    """Profile collections in a MongoDB database."""
    client = pymongo.MongoClient(connection_url)
    db = client[database]

    schema_info = {}
    for coll_name in db.list_collection_names():
        coll = db[coll_name]
        doc_count = coll.estimated_document_count()
        sample = list(coll.find().limit(sample_size))

        # Infer field types from sample
        field_types: Dict[str, set] = {}
        for doc in sample:
            for key, value in doc.items():
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(type(value).__name__)

        schema_info[coll_name] = {
            "document_count": doc_count,
            "fields": {
                k: list(v) for k, v in field_types.items()
            },
            "sample_doc": json.loads(json_dumps(sample[0])) if sample else {},
        }

    client.close()
    return schema_info


# ─── CouchDB Schema Profiler ─────────────────────────────────────────────────
def extract_couch_schema(host: str, username: str, password: str) -> Dict[str, Any]:
    """Profile databases in a CouchDB instance."""
    auth = (username, password)
    schema_info = {}

    r = httpx.get(f"{host}/_all_dbs", auth=auth, timeout=30)
    r.raise_for_status()
    all_dbs = [db for db in r.json() if not db.startswith("_")]

    for db_name in all_dbs:
        # Get db info
        info_r = httpx.get(f"{host}/{db_name}", auth=auth, timeout=30)
        info = info_r.json()

        # Sample docs
        docs_r = httpx.get(
            f"{host}/{db_name}/_all_docs",
            params={"include_docs": "true", "limit": 50},
            auth=auth,
            timeout=30,
        )
        docs_data = docs_r.json()
        rows = docs_data.get("rows", [])
        sample_docs = [row["doc"] for row in rows if "doc" in row]

        # Infer field types
        field_types: Dict[str, set] = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key.startswith("_"):
                    continue
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(type(value).__name__)

        schema_info[db_name] = {
            "doc_count": info.get("doc_count", 0),
            "fields": {k: list(v) for k, v in field_types.items()},
            "sample_doc": sample_docs[0] if sample_docs else {},
        }

    return schema_info


# ─── AI Plan Generator ───────────────────────────────────────────────────────
def generate_migration_plan(
    source_type: str,
    target_type: str,
    schema_text: str,
    feedback: Optional[str] = None,
    existing_plan: Optional[str] = None,
) -> Dict[str, Any]:
    """Use Azure OpenAI to generate a migration plan."""
    client = get_ai_client()

    system_prompt = f"""You are a database migration architect.
You are migrating data from {source_type.upper()} to {target_type.upper()}.

Given the source schema metadata, produce a JSON migration plan.

The plan must be a JSON object with a key "collections" (or "tables") that is a list.
Each item should have:
- "source": the source table/collection name
- "target": the target table/collection name
- "field_mappings": a list of {{ "source_field", "target_field", "type", "notes" }}
- "strategy": one of "flat", "embed", "reference", "normalize", "denormalize"
- "notes": any relevant migration notes
- "embedding": (if NoSQL target) describes how to embed related data

Return ONLY valid JSON. No markdown, no explanation.
"""

    messages = [{"role": "system", "content": system_prompt}]

    if existing_plan and feedback:
        messages.append({
            "role": "user",
            "content": f"Here is the current migration plan:\n\n{existing_plan}\n\nUser feedback:\n{feedback}\n\nPlease update the plan based on the feedback. Return ONLY the updated JSON.",
        })
    else:
        messages.append({
            "role": "user",
            "content": f"Source {source_type} schema:\n\n{schema_text}\n\nGenerate the migration plan to {target_type}. Return ONLY valid JSON.",
        })

    response = client.chat.completions.create(
        model=DEPLOYMENT,
        messages=messages,
        temperature=0.2,
        max_tokens=4096,
    )

    raw = response.choices[0].message.content.strip()

    # Try to parse JSON from the response
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

    try:
        plan = json.loads(raw)
    except json.JSONDecodeError:
        plan = {"raw_response": raw, "error": "Failed to parse AI response as JSON"}

    return plan


# ─── Base Pipeline Class ─────────────────────────────────────────────────────
class BasePipeline(ABC):
    """Abstract base for all migration pipelines."""

    source_type: str = ""
    target_type: str = ""

    @abstractmethod
    def test_source_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Test connection to source DB. Returns {"success": bool, "message": str}."""
        pass

    @abstractmethod
    def test_target_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Test connection to target DB. Returns {"success": bool, "message": str}."""
        pass

    @abstractmethod
    def extract_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract schema from source DB."""
        pass

    @abstractmethod
    def execute(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        plan: Dict[str, Any],
        on_progress: Any = None,
    ) -> Dict[str, Any]:
        """Execute the migration based on the approved plan."""
        pass
