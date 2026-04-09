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

                # Transform rows to docs
                documents = []
                for row in data:
                    doc = {}
                    for col_name, value in zip(columns, row):
                        if isinstance(value, (datetime.datetime, datetime.date)):
                            doc[col_name] = value.isoformat()
                        elif isinstance(value, decimal.Decimal):
                            doc[col_name] = float(value)
                        elif isinstance(value, bytes):
                            doc[col_name] = value.decode("utf-8", errors="replace")
                        else:
                            doc[col_name] = value
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
