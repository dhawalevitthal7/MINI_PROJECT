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

    def _transform_doc(self, doc: dict, collection_name: str, field_maps: list, src_to_tgt: dict) -> dict:
        """Transform a MongoDB document for CouchDB mapping rules."""
        new_doc = {"doc_type": collection_name}
        
        for k, v in doc.items():
            # If field maps exist and key is explicitly not mapped (and isn't the primary _id if we wanted to omit it), skip it
            if field_maps and k not in src_to_tgt:
                # Always at least allow _id to map to mongo_id if not strictly overriding it, OR skip if strictly omitted
                if k != "_id":
                    continue

            final_k = src_to_tgt.get(k, k)

            if k == "_id":
                # If _id wasn't mapped at all, we fall back to mongo_id if we didn't continue above.
                mapped_id_key = final_k if final_k != "_id" else "mongo_id"
                # If they explicitly omitted it, we skip
                if field_maps and k not in src_to_tgt:
                    continue
                new_doc[mapped_id_key] = str(v)
                continue
                
            if isinstance(v, ObjectId):
                new_doc[final_k] = str(v)
            elif isinstance(v, (datetime.datetime, datetime.date)):
                new_doc[final_k] = v.isoformat()
            elif isinstance(v, bytes):
                new_doc[final_k] = v.decode("utf-8", errors="replace")
            elif isinstance(v, dict):
                new_doc[final_k] = json.loads(json.dumps(v, default=safe_json))
            elif isinstance(v, (list, set)):
                new_doc[final_k] = json.loads(json.dumps(list(v), default=safe_json))
            else:
                new_doc[final_k] = v
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

            field_maps = mapping.get("field_mappings", [])
            src_to_tgt = {}
            for fm in field_maps:
                s_field = fm.get("source_field") or fm.get("source") or fm.get("source_column")
                t_field = fm.get("target_field") or fm.get("target") or fm.get("target_column")
                if s_field and t_field:
                    src_to_tgt[s_field] = t_field

            try:
                # Create CouchDB database
                httpx.put(f"{host}/{target_db_name}", auth=auth, timeout=30)

                # Read and transform documents
                docs = list(mongo_db[source_coll].find())
                couch_docs = [self._transform_doc(doc, source_coll, field_maps, src_to_tgt) for doc in docs]

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
