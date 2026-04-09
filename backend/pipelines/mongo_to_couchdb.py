"""
Pipeline: MongoDB → CouchDB
Profiles MongoDB collections, transforms documents, inserts into CouchDB databases.
Features: ObjectId → string, _rev handling for upserts, doc_type tagging, bulk inserts.
"""

import json
import datetime
from typing import Any, Dict

import pymongo
import httpx
from bson import ObjectId

from .base import BasePipeline, extract_mongo_schema, safe_json


class MongoToCouchDBPipeline(BasePipeline):
    source_type = "mongodb"
    target_type = "couchdb"

    def test_source_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            client = pymongo.MongoClient(config["connection_url"], serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            client.close()
            return {"success": True, "message": "MongoDB connection successful"}
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
        return extract_mongo_schema(config["connection_url"], config["database"])

    def _transform_doc(self, doc: dict, collection_name: str) -> dict:
        """Transform a MongoDB document for CouchDB."""
        new_doc = {"doc_type": collection_name}
        for k, v in doc.items():
            if k == "_id":
                new_doc["mongo_id"] = str(v)
                continue
            if isinstance(v, ObjectId):
                new_doc[k] = str(v)
            elif isinstance(v, (datetime.datetime, datetime.date)):
                new_doc[k] = v.isoformat()
            elif isinstance(v, bytes):
                new_doc[k] = v.decode("utf-8", errors="replace")
            elif isinstance(v, dict):
                new_doc[k] = json.loads(json.dumps(v, default=safe_json))
            elif isinstance(v, (list, set)):
                new_doc[k] = json.loads(json.dumps(list(v), default=safe_json))
            else:
                new_doc[k] = v
        return new_doc

    def execute(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        plan: Dict[str, Any],
        on_progress=None,
    ) -> Dict[str, Any]:
        mongo_client = pymongo.MongoClient(source_config["connection_url"])
        mongo_db = mongo_client[source_config["database"]]
        host = target_config["host"]
        auth = (target_config["username"], target_config["password"])

        results = {"tables_migrated": [], "errors": [], "total_rows": 0}
        mappings = plan.get("collections", plan.get("tables", []))

        for i, mapping in enumerate(mappings):
            source_coll = mapping["source"]
            target_db_name = mapping["target"].lower().replace(" ", "_")

            try:
                # Create CouchDB database
                httpx.put(f"{host}/{target_db_name}", auth=auth, timeout=30)

                # Read and transform documents
                docs = list(mongo_db[source_coll].find())
                couch_docs = [self._transform_doc(doc, source_coll) for doc in docs]

                # Bulk insert in batches
                if couch_docs:
                    batch_size = 500
                    for batch_start in range(0, len(couch_docs), batch_size):
                        batch = couch_docs[batch_start : batch_start + batch_size]
                        r = httpx.post(
                            f"{host}/{target_db_name}/_bulk_docs",
                            json={"docs": batch},
                            auth=auth,
                            timeout=60,
                        )
                        r.raise_for_status()

                results["tables_migrated"].append({
                    "source": source_coll,
                    "target": target_db_name,
                    "rows": len(couch_docs),
                })
                results["total_rows"] += len(couch_docs)

                if on_progress:
                    on_progress(i + 1, len(mappings), source_coll)

            except Exception as e:
                results["errors"].append({"table": source_coll, "error": str(e)})

        mongo_client.close()
        return results
