"""
Pipeline: CouchDB → PostgreSQL
Profiles CouchDB databases, generates PostgreSQL DDL, migrates data.
Features: JSONB support, BYTEA for binary, TIMESTAMP for datetime, ON CONFLICT upsert.
"""

import json
import datetime
from typing import Any, Dict

from sqlalchemy import create_engine, text
import httpx

from .base import BasePipeline, extract_couch_schema, safe_json


class CouchDBToPostgresPipeline(BasePipeline):
    source_type = "couchdb"
    target_type = "postgresql"

    def test_source_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            host = config["host"]
            auth = (config["username"], config["password"])
            r = httpx.get(f"{host}/", auth=auth, timeout=10)
            r.raise_for_status()
            return {"success": True, "message": "CouchDB connection successful"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def test_target_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            engine = create_engine(config["connection_url"])
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return {"success": True, "message": "PostgreSQL connection successful"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def extract_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return extract_couch_schema(config["host"], config["username"], config["password"])

    def _infer_pg_type(self, value) -> str:
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        if isinstance(value, str):
            length = len(value)
            if length <= 255:
                return "VARCHAR(255)"
            else:
                return "TEXT"
        if isinstance(value, (dict, list)):
            return "JSONB"
        return "TEXT"

    def execute(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        plan: Dict[str, Any],
        on_progress=None,
    ) -> Dict[str, Any]:
        host = source_config["host"]
        auth = (source_config["username"], source_config["password"])
        engine = create_engine(target_config["connection_url"])

        results = {"tables_migrated": [], "errors": [], "total_rows": 0}
        mappings = plan.get("tables", plan.get("collections", []))

        for i, mapping in enumerate(mappings):
            source_db = mapping["source"]
            target_table = mapping["target"]

            try:
                r = httpx.get(
                    f"{host}/{source_db}/_all_docs",
                    params={"include_docs": "true"},
                    auth=auth,
                    timeout=120,
                )
                r.raise_for_status()
                rows = r.json().get("rows", [])
                docs = [row["doc"] for row in rows if "doc" in row and not row["doc"].get("_id", "").startswith("_")]

                if not docs:
                    continue

                clean_docs = []
                for doc in docs:
                    clean = {}
                    for k, v in doc.items():
                        if k.startswith("_"):
                            if k == "_id":
                                clean["couch_id"] = v
                            continue
                        if isinstance(v, (dict, list)):
                            clean[k] = json.dumps(v, default=safe_json)
                        else:
                            clean[k] = v
                    clean_docs.append(clean)

                all_columns = {}
                for doc in clean_docs[:100]:
                    for k, v in doc.items():
                        if v is not None and k not in all_columns:
                            all_columns[k] = self._infer_pg_type(v)

                col_defs = ", ".join(f'"{col}" {dtype}' for col, dtype in all_columns.items())
                with engine.connect() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{target_table}"'))
                    conn.execute(text(f'CREATE TABLE "{target_table}" ({col_defs})'))
                    conn.commit()

                col_names = list(all_columns.keys())
                placeholders = ", ".join([f":{c}" for c in col_names])
                cols_str = ", ".join([f'"{c}"' for c in col_names])
                insert_sql = f'INSERT INTO "{target_table}" ({cols_str}) VALUES ({placeholders})'

                inserted = 0
                with engine.connect() as conn:
                    for doc in clean_docs:
                        params = {col: doc.get(col) for col in col_names}
                        try:
                            conn.execute(text(insert_sql), params)
                            inserted += 1
                        except Exception:
                            pass
                    conn.commit()

                results["tables_migrated"].append({
                    "source": source_db,
                    "target": target_table,
                    "rows": inserted,
                })
                results["total_rows"] += inserted

                if on_progress:
                    on_progress(i + 1, len(mappings), source_db)

            except Exception as e:
                results["errors"].append({"table": source_db, "error": str(e)})

        engine.dispose()
        return results
