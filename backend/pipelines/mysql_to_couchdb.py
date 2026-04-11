"""
Pipeline: MySQL → CouchDB
Extracts tables from MySQL, transforms rows, inserts into CouchDB databases.
"""

import json
import datetime
import decimal
from typing import Any, Dict

from sqlalchemy import create_engine, text
import httpx

from .base import BasePipeline, extract_sql_schema, safe_json


class MySQLToCouchDBPipeline(BasePipeline):
    source_type = "mysql"
    target_type = "couchdb"

    def test_source_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            engine = create_engine(config["connection_url"])
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return {"success": True, "message": "MySQL connection successful"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def test_target_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            host = config["host"]
            auth = (config["username"], config["password"])
            r = httpx.get(f"{host}/", auth=auth, timeout=10)
            r.raise_for_status()
            return {"success": True, "message": "CouchDB connection successful"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def extract_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return extract_sql_schema(config["connection_url"])

    def execute(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        plan: Dict[str, Any],
        on_progress=None,
    ) -> Dict[str, Any]:
        engine = create_engine(source_config["connection_url"])
        host = target_config["host"]
        auth = (target_config["username"], target_config["password"])

        results = {"tables_migrated": [], "errors": [], "total_rows": 0}
        mappings = plan.get("collections", plan.get("tables", []))

        for i, mapping in enumerate(mappings):
            source_table = mapping["source"]
            target_db_name = mapping["target"].lower().replace(" ", "_")

            try:
                # Create CouchDB database
                httpx.put(f"{host}/{target_db_name}", auth=auth, timeout=30)

                with engine.connect() as conn:
                    rows = conn.execute(text(f"SELECT * FROM `{source_table}`"))
                    columns = list(rows.keys())
                    data = rows.fetchall()

                field_maps = mapping.get("field_mappings", [])
                src_to_tgt = {}
                for fm in field_maps:
                    s_field = fm.get("source_field") or fm.get("source") or fm.get("source_column")
                    t_field = fm.get("target_field") or fm.get("target") or fm.get("target_column")
                    if s_field and t_field:
                        src_to_tgt[s_field] = t_field

                # Transform rows to docs
                documents = []
                for row in data:
                    doc = {}
                    for col_name, value in zip(columns, row):
                        # If field mappings exist but this column isn't mapped, skip it!
                        if field_maps and col_name not in src_to_tgt:
                            continue

                        final_col = src_to_tgt.get(col_name, col_name)
                        if isinstance(value, (datetime.datetime, datetime.date)):
                            doc[final_col] = value.isoformat()
                        elif isinstance(value, decimal.Decimal):
                            doc[final_col] = float(value)
                        elif isinstance(value, bytes):
                            doc[final_col] = value.decode("utf-8", errors="replace")
                        else:
                            doc[final_col] = value
                    doc["source_table"] = source_table
                    documents.append(doc)

                # Bulk insert into CouchDB
                if documents:
                    batch_size = 500
                    for batch_start in range(0, len(documents), batch_size):
                        batch = documents[batch_start : batch_start + batch_size]
                        r = httpx.post(
                            f"{host}/{target_db_name}/_bulk_docs",
                            json={"docs": batch},
                            auth=auth,
                            timeout=60,
                        )
                        r.raise_for_status()

                results["tables_migrated"].append({
                    "source": source_table,
                    "target": target_db_name,
                    "rows": len(documents),
                })
                results["total_rows"] += len(documents)

                if on_progress:
                    on_progress(i + 1, len(mappings), source_table)

            except Exception as e:
                results["errors"].append({"table": source_table, "error": str(e)})

        engine.dispose()
        return results
