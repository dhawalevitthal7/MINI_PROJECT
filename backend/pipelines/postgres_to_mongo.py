"""
Pipeline: PostgreSQL → MongoDB
Extracts tables from PostgreSQL, transforms rows, inserts into MongoDB.
"""

import json
import datetime
import decimal
from typing import Any, Dict

from sqlalchemy import create_engine, text
import pymongo

from .base import BasePipeline, extract_sql_schema, safe_json


class PostgresToMongoPipeline(BasePipeline):
    source_type = "postgresql"
    target_type = "mongodb"

    def test_source_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            engine = create_engine(config["connection_url"])
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return {"success": True, "message": "PostgreSQL connection successful"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def test_target_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            client = pymongo.MongoClient(config["connection_url"], serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
            return {"success": True, "message": "MongoDB connection successful"}
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
        client = pymongo.MongoClient(target_config["connection_url"])
        db = client[target_config.get("database", "migrated_db")]

        results = {"tables_migrated": [], "errors": [], "total_rows": 0}
        mappings = plan.get("collections", plan.get("tables", []))

        for i, mapping in enumerate(mappings):
            source_table = mapping["source"]
            target_coll = mapping["target"]

            try:
                with engine.connect() as conn:
                    rows = conn.execute(text(f'SELECT * FROM "{source_table}"'))
                    columns = list(rows.keys())
                    data = rows.fetchall()

                field_maps = mapping.get("field_mappings", [])
                src_to_tgt = {}
                for fm in field_maps:
                    s_field = fm.get("source_field") or fm.get("source") or fm.get("source_column")
                    t_field = fm.get("target_field") or fm.get("target") or fm.get("target_column")
                    if s_field and t_field:
                        src_to_tgt[s_field] = t_field

                # Transform rows
                documents = []
                for row in data:
                    doc = {}
                    for col_name, value in zip(columns, row):
                        # If field mappings exist but this column isn't mapped, skip it!
                        if field_maps and col_name not in src_to_tgt:
                            continue

                        # Apply AI-approved field renaming
                        final_col = src_to_tgt.get(col_name, col_name)
                        
                        if isinstance(value, (datetime.datetime, datetime.date)):
                            doc[final_col] = value.isoformat()
                        elif isinstance(value, decimal.Decimal):
                            doc[final_col] = float(value)
                        elif isinstance(value, bytes):
                            doc[final_col] = value.decode("utf-8", errors="replace")
                        else:
                            doc[final_col] = value
                    documents.append(doc)

                if documents:
                    coll = db[target_coll]
                    coll.drop()
                    coll.insert_many(documents)

                results["tables_migrated"].append({
                    "source": source_table,
                    "target": target_coll,
                    "rows": len(documents),
                })
                results["total_rows"] += len(documents)

                if on_progress:
                    on_progress(i + 1, len(mappings), source_table)

            except Exception as e:
                results["errors"].append({"table": source_table, "error": str(e)})

        engine.dispose()
        client.close()
        return results
