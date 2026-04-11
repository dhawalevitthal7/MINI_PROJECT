"""
NoSQL Database Manager — MongoDB and CouchDB Support
Handles schema extraction, query execution, and data retrieval for NoSQL databases.
"""

import json
import re
from typing import Dict, Any, List, Optional
from fastapi import HTTPException


class MongoDBManager:
    """Manages MongoDB connections, schema extraction, and query execution."""

    @staticmethod
    def get_client(connection_url: str):
        import pymongo
        try:
            client = pymongo.MongoClient(connection_url, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return client
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"MongoDB Connection Error: {str(e)}")

    @staticmethod
    def get_database_name(connection_url: str) -> str:
        """Extract database name from MongoDB connection URL."""
        import urllib.parse
        parsed = urllib.parse.urlparse(connection_url)
        db_name = parsed.path.strip("/")
        if "?" in db_name:
            db_name = db_name.split("?")[0]
        return db_name if db_name else "test"

    @staticmethod
    def get_collections(connection_url: str, database: str) -> List[str]:
        """List all collections in a MongoDB database."""
        client = MongoDBManager.get_client(connection_url)
        try:
            db = client[database]
            return db.list_collection_names()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"MongoDB Error: {str(e)}")
        finally:
            client.close()

    @staticmethod
    def fetch_schema(connection_url: str, database: str, sample_size: int = 50) -> str:
        """Extract schema from MongoDB as a detailed string for LLM context.
        Includes fields, types, indexes, sample values, and _id patterns.
        """
        client = MongoDBManager.get_client(connection_url)
        try:
            db = client[database]
            output = []
            output.append(f"MongoDB Database: {database}")

            for coll_name in db.list_collection_names():
                coll = db[coll_name]
                doc_count = coll.estimated_document_count()
                sample = list(coll.find().limit(sample_size))

                field_types: Dict[str, set] = {}
                field_sample_values: Dict[str, list] = {}
                for doc in sample:
                    for key, value in doc.items():
                        if key == "_id":
                            continue
                        if key not in field_types:
                            field_types[key] = set()
                            field_sample_values[key] = []
                        field_types[key].add(type(value).__name__)
                        if len(field_sample_values[key]) < 3 and value is not None:
                            try:
                                field_sample_values[key].append(str(value)[:80])
                            except Exception:
                                pass

                output.append(f"\nCollection: {coll_name} (documents: {doc_count})")

                # Fields with types and sample values
                for k, v in field_types.items():
                    type_str = "/".join(v)
                    samples = field_sample_values.get(k, [])
                    sample_str = f' [examples: {", ".join(samples[:3])}]' if samples else ""
                    output.append(f"  Field: {k} ({type_str}){sample_str}")

                # _id pattern
                if sample:
                    id_val = sample[0].get("_id")
                    if id_val is not None:
                        output.append(f"  _id type: {type(id_val).__name__} (serves as primary key)")

                # Indexes
                try:
                    indexes = coll.index_information()
                    for idx_name, idx_info in indexes.items():
                        if idx_name == "_id_":
                            continue
                        idx_keys = ", ".join([f"{k[0]} ({'asc' if k[1] == 1 else 'desc'})" for k in idx_info.get("key", [])])
                        unique_str = " UNIQUE" if idx_info.get("unique") else ""
                        output.append(f"  Index: {idx_name} ({idx_keys}){unique_str}")
                except Exception:
                    pass

                # Sample document
                if sample:
                    sample_doc = MongoDBManager._serialize_docs([sample[0]])[0]
                    # Remove _id for brevity
                    sample_doc.pop("_id", None)
                    try:
                        doc_str = json.dumps(sample_doc, default=str)
                        if len(doc_str) > 500:
                            doc_str = doc_str[:500] + "..."
                        output.append(f"  Sample Document: {doc_str}")
                    except Exception:
                        pass

            return "\n".join(output)
        except Exception as e:
            print(f"[ERROR] MongoDB Schema Fetch Error: {e}")
            return ""
        finally:
            client.close()

    @staticmethod
    def get_structured_schema(connection_url: str, database: str, sample_size: int = 50) -> Dict[str, Any]:
        """Get structured schema data for UI display — includes indexes, _id info."""
        client = MongoDBManager.get_client(connection_url)
        try:
            db = client[database]
            schema_data = {}
            for coll_name in db.list_collection_names():
                coll = db[coll_name]
                doc_count = coll.estimated_document_count()
                sample = list(coll.find().limit(sample_size))

                field_types: Dict[str, set] = {}
                for doc in sample:
                    for key, value in doc.items():
                        if key == "_id":
                            continue
                        if key not in field_types:
                            field_types[key] = set()
                        field_types[key].add(type(value).__name__)

                # Indexes
                idx_list = []
                unique_fields = []
                try:
                    indexes = coll.index_information()
                    for idx_name, idx_info in indexes.items():
                        idx_keys = [k[0] for k in idx_info.get("key", [])]
                        is_unique = idx_info.get("unique", False)
                        idx_list.append({
                            "name": idx_name,
                            "columns": idx_keys,
                            "unique": is_unique,
                        })
                        if is_unique and idx_name != "_id_":
                            unique_fields.extend(idx_keys)
                except Exception:
                    pass

                # _id pattern
                id_type = "ObjectId"
                if sample:
                    id_val = sample[0].get("_id")
                    if id_val is not None:
                        id_type = type(id_val).__name__

                schema_data[coll_name] = {
                    "type": "collection",
                    "document_count": doc_count,
                    "fields": [
                        {"name": k, "type": "/".join(v)} for k, v in field_types.items()
                    ],
                    "primary_keys": ["_id"],
                    "id_type": id_type,
                    "unique_fields": unique_fields,
                    "indexes": idx_list,
                }
            return schema_data
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"MongoDB Error: {str(e)}")
        finally:
            client.close()

    @staticmethod
    def get_collection_details(connection_url: str, database: str, collection_name: str) -> Dict[str, Any]:
        """Get details of a specific collection including indexes."""
        client = MongoDBManager.get_client(connection_url)
        try:
            db = client[database]
            coll = db[collection_name]
            doc_count = coll.estimated_document_count()

            # Get sample docs
            first_10 = list(coll.find({}, {"_id": 0}).limit(10))
            # Get last 10 by sorting by _id descending
            last_10 = list(coll.find({}, {"_id": 0}).sort("_id", -1).limit(10))
            last_10.reverse()

            # Infer fields from sample
            sample = list(coll.find().limit(50))
            field_names = set()
            for doc in sample:
                for key in doc.keys():
                    if key != "_id":
                        field_names.add(key)

            # Indexes
            idx_list = []
            unique_fields = []
            try:
                indexes = coll.index_information()
                for idx_name, idx_info in indexes.items():
                    idx_keys = [k[0] for k in idx_info.get("key", [])]
                    is_unique = idx_info.get("unique", False)
                    idx_list.append({
                        "name": idx_name,
                        "columns": idx_keys,
                        "unique": is_unique,
                    })
                    if is_unique and idx_name != "_id_":
                        unique_fields.extend(idx_keys)
            except Exception:
                pass

            # _id type
            id_type = "ObjectId"
            if sample:
                id_val = sample[0].get("_id")
                if id_val is not None:
                    id_type = type(id_val).__name__

            return {
                "table_name": collection_name,
                "row_count": doc_count,
                "columns": list(field_names),
                "primary_keys": ["_id"],
                "id_type": id_type,
                "unique_fields": unique_fields,
                "indexes": idx_list,
                "foreign_keys": [],
                "first_10": MongoDBManager._serialize_docs(first_10),
                "last_10": MongoDBManager._serialize_docs(last_10),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MongoDB Error: {str(e)}")
        finally:
            client.close()

    @staticmethod
    def get_collection_data(connection_url: str, database: str, collection_name: str, page: int = 1, limit: int = 100) -> Dict[str, Any]:
        """Get paginated data from a collection."""
        client = MongoDBManager.get_client(connection_url)
        try:
            db = client[database]
            coll = db[collection_name]
            total_rows = coll.estimated_document_count()
            total_pages = max(1, (total_rows + limit - 1) // limit)
            skip = (page - 1) * limit

            docs = list(coll.find({}, {"_id": 0}).skip(skip).limit(limit))

            return {
                "data": MongoDBManager._serialize_docs(docs),
                "total_rows": total_rows,
                "page": page,
                "total_pages": total_pages,
            }
        except Exception as e:
            return {"data": [], "total_rows": 0, "page": 0, "total_pages": 0, "error": str(e)}
        finally:
            client.close()

    @staticmethod
    def execute_query(connection_url: str, database: str, query_str: str) -> List[Dict]:
        """Execute a MongoDB aggregation pipeline or find query generated by AI."""
        client = MongoDBManager.get_client(connection_url)
        try:
            db = client[database]
            # Parse the AI-generated query
            query_obj = json.loads(query_str)

            collection_name = query_obj.get("collection", "")
            if not collection_name:
                raise ValueError("Query must specify a 'collection' field")

            coll = db[collection_name]

            if "pipeline" in query_obj:
                # Aggregation pipeline
                results = list(coll.aggregate(query_obj["pipeline"]))
            elif "filter" in query_obj:
                # Find query
                projection = query_obj.get("projection", {"_id": 0})
                sort = query_obj.get("sort")
                limit = query_obj.get("limit", 100)
                cursor = coll.find(query_obj["filter"], projection)
                if sort:
                    cursor = cursor.sort(list(sort.items()) if isinstance(sort, dict) else sort)
                cursor = cursor.limit(limit)
                results = list(cursor)
            else:
                # Try as raw aggregation pipeline (list)
                if isinstance(query_obj, list):
                    results = list(coll.aggregate(query_obj))
                else:
                    raise ValueError("Unrecognized query format. Use 'pipeline' or 'filter'.")

            return MongoDBManager._serialize_docs(results)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid MongoDB query JSON")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"MongoDB Query Error: {str(e)}")
        finally:
            client.close()

    @staticmethod
    def _serialize_docs(docs: List[Dict]) -> List[Dict]:
        """Make MongoDB documents JSON-serializable."""
        import datetime
        import decimal
        from bson import ObjectId

        def serialize(obj):
            if isinstance(obj, ObjectId):
                return str(obj)
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            if isinstance(obj, bytes):
                return "<binary>"
            if isinstance(obj, dict):
                return {k: serialize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [serialize(i) for i in obj]
            return obj

        return [serialize(doc) for doc in docs]


class CouchDBManager:
    """Manages CouchDB connections, schema extraction, and query execution."""

    @staticmethod
    def _auth(connection_url: str):
        """Parse CouchDB URL to get host, user, password."""
        # Expected format: http://user:password@host:port
        import urllib.parse
        parsed = urllib.parse.urlparse(connection_url)
        host = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            host += f":{parsed.port}"
        return host, parsed.username or "admin", parsed.password or "admin"

    @staticmethod
    def test_connection(connection_url: str) -> bool:
        import httpx
        host, user, password = CouchDBManager._auth(connection_url)
        try:
            r = httpx.get(f"{host}/", auth=(user, password), timeout=10)
            r.raise_for_status()
            return True
        except Exception:
            return False

    @staticmethod
    def get_databases(connection_url: str) -> List[str]:
        """List all non-system databases."""
        import httpx
        host, user, password = CouchDBManager._auth(connection_url)
        try:
            r = httpx.get(f"{host}/_all_dbs", auth=(user, password), timeout=30)
            r.raise_for_status()
            return [db for db in r.json() if not db.startswith("_")]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"CouchDB Error: {str(e)}")

    @staticmethod
    def fetch_schema(connection_url: str) -> str:
        """Extract schema from CouchDB as a detailed string for LLM context.
        Includes fields, types, sample values, document structure.
        """
        import httpx
        host, user, password = CouchDBManager._auth(connection_url)
        try:
            r = httpx.get(f"{host}/_all_dbs", auth=(user, password), timeout=30)
            r.raise_for_status()
            all_dbs = [db for db in r.json() if not db.startswith("_")]

            output = []
            output.append("CouchDB Instance")

            for db_name in all_dbs:
                info_r = httpx.get(f"{host}/{db_name}", auth=(user, password), timeout=30)
                info = info_r.json()
                doc_count = info.get("doc_count", 0)

                # Sample docs to infer fields
                docs_r = httpx.get(
                    f"{host}/{db_name}/_all_docs",
                    params={"include_docs": "true", "limit": 50},
                    auth=(user, password),
                    timeout=30,
                )
                docs_data = docs_r.json()
                sample_docs = [row["doc"] for row in docs_data.get("rows", []) if "doc" in row]

                field_types: Dict[str, set] = {}
                field_sample_values: Dict[str, list] = {}
                for doc in sample_docs:
                    for key, value in doc.items():
                        if key.startswith("_"):
                            continue
                        if key not in field_types:
                            field_types[key] = set()
                            field_sample_values[key] = []
                        field_types[key].add(type(value).__name__)
                        if len(field_sample_values[key]) < 3 and value is not None:
                            try:
                                field_sample_values[key].append(str(value)[:80])
                            except Exception:
                                pass

                output.append(f"\nDatabase: {db_name} (documents: {doc_count})")
                output.append(f"  Primary Key: _id (string, auto-generated or user-defined)")

                # Fields with types and sample values
                for k, v in field_types.items():
                    type_str = "/".join(v)
                    samples = field_sample_values.get(k, [])
                    sample_str = f' [examples: {", ".join(samples[:3])}]' if samples else ""
                    output.append(f"  Field: {k} ({type_str}){sample_str}")

                # Sample document
                if sample_docs:
                    sample_doc = {k: v for k, v in sample_docs[0].items() if not k.startswith("_")}
                    try:
                        doc_str = json.dumps(sample_doc, default=str)
                        if len(doc_str) > 500:
                            doc_str = doc_str[:500] + "..."
                        output.append(f"  Sample Document: {doc_str}")
                    except Exception:
                        pass

                # Design docs / indexes
                try:
                    design_r = httpx.get(
                        f"{host}/{db_name}/_all_docs",
                        params={"startkey": '"_design/"', "endkey": '"_design0"', "include_docs": "true"},
                        auth=(user, password),
                        timeout=15,
                    )
                    design_data = design_r.json()
                    for row in design_data.get("rows", []):
                        ddoc = row.get("doc", {})
                        views = ddoc.get("views", {})
                        for view_name in views:
                            output.append(f"  View: {row.get('id', '')} / {view_name}")
                except Exception:
                    pass

            return "\n".join(output)
        except Exception as e:
            print(f"[ERROR] CouchDB Schema Fetch Error: {e}")
            return ""

    @staticmethod
    def get_structured_schema(connection_url: str) -> Dict[str, Any]:
        """Get structured schema data for UI display — includes _id info."""
        import httpx
        host, user, password = CouchDBManager._auth(connection_url)
        try:
            r = httpx.get(f"{host}/_all_dbs", auth=(user, password), timeout=30)
            r.raise_for_status()
            all_dbs = [db for db in r.json() if not db.startswith("_")]

            schema_data = {}
            for db_name in all_dbs:
                info_r = httpx.get(f"{host}/{db_name}", auth=(user, password), timeout=30)
                info = info_r.json()
                doc_count = info.get("doc_count", 0)

                docs_r = httpx.get(
                    f"{host}/{db_name}/_all_docs",
                    params={"include_docs": "true", "limit": 50},
                    auth=(user, password),
                    timeout=30,
                )
                docs_data = docs_r.json()
                sample_docs = [row["doc"] for row in docs_data.get("rows", []) if "doc" in row]

                field_types: Dict[str, set] = {}
                for doc in sample_docs:
                    for key, value in doc.items():
                        if key.startswith("_"):
                            continue
                        if key not in field_types:
                            field_types[key] = set()
                        field_types[key].add(type(value).__name__)

                schema_data[db_name] = {
                    "type": "database",
                    "document_count": doc_count,
                    "fields": [
                        {"name": k, "type": "/".join(v)} for k, v in field_types.items()
                    ],
                    "primary_keys": ["_id"],
                    "id_type": "string",
                    "indexes": [],
                }
            return schema_data
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"CouchDB Error: {str(e)}")

    @staticmethod
    def get_database_details(connection_url: str, db_name: str) -> Dict[str, Any]:
        """Get details of a specific CouchDB database."""
        import httpx
        host, user, password = CouchDBManager._auth(connection_url)
        try:
            info_r = httpx.get(f"{host}/{db_name}", auth=(user, password), timeout=30)
            info = info_r.json()
            doc_count = info.get("doc_count", 0)

            # First 10 docs
            first_r = httpx.get(
                f"{host}/{db_name}/_all_docs",
                params={"include_docs": "true", "limit": 10},
                auth=(user, password),
                timeout=30,
            )
            first_data = first_r.json()
            first_10 = []
            for row in first_data.get("rows", []):
                doc = row.get("doc", {})
                cleaned = {k: v for k, v in doc.items() if not k.startswith("_")}
                first_10.append(cleaned)

            # Last 10 docs
            last_r = httpx.get(
                f"{host}/{db_name}/_all_docs",
                params={"include_docs": "true", "limit": 10, "descending": "true"},
                auth=(user, password),
                timeout=30,
            )
            last_data = last_r.json()
            last_10 = []
            for row in last_data.get("rows", []):
                doc = row.get("doc", {})
                cleaned = {k: v for k, v in doc.items() if not k.startswith("_")}
                last_10.append(cleaned)
            last_10.reverse()

            # Infer columns
            all_keys = set()
            for doc in first_10 + last_10:
                all_keys.update(doc.keys())

            return {
                "table_name": db_name,
                "row_count": doc_count,
                "columns": list(all_keys),
                "primary_keys": ["_id"],
                "id_type": "string",
                "indexes": [],
                "foreign_keys": [],
                "first_10": first_10,
                "last_10": last_10,
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"CouchDB Error: {str(e)}")

    @staticmethod
    def get_database_data(connection_url: str, db_name: str, page: int = 1, limit: int = 100) -> Dict[str, Any]:
        """Get paginated data from a CouchDB database."""
        import httpx
        host, user, password = CouchDBManager._auth(connection_url)
        try:
            info_r = httpx.get(f"{host}/{db_name}", auth=(user, password), timeout=30)
            info = info_r.json()
            total_rows = info.get("doc_count", 0)
            total_pages = max(1, (total_rows + limit - 1) // limit)
            skip = (page - 1) * limit

            docs_r = httpx.get(
                f"{host}/{db_name}/_all_docs",
                params={"include_docs": "true", "limit": limit, "skip": skip},
                auth=(user, password),
                timeout=30,
            )
            docs_data = docs_r.json()
            data = []
            for row in docs_data.get("rows", []):
                doc = row.get("doc", {})
                cleaned = {k: v for k, v in doc.items() if not k.startswith("_")}
                data.append(cleaned)

            return {
                "data": data,
                "total_rows": total_rows,
                "page": page,
                "total_pages": total_pages,
            }
        except Exception as e:
            return {"data": [], "total_rows": 0, "page": 0, "total_pages": 0, "error": str(e)}

    @staticmethod
    def execute_query(connection_url: str, db_name: str, query_str: str) -> List[Dict]:
        """Execute a CouchDB Mango query generated by AI."""
        import httpx
        host, user, password = CouchDBManager._auth(connection_url)
        try:
            query_obj = json.loads(query_str)

            target_db = query_obj.get("database", db_name)

            mango_query = {
                "selector": query_obj.get("selector", {}),
                "limit": query_obj.get("limit", 100),
            }
            if "fields" in query_obj:
                mango_query["fields"] = query_obj["fields"]
            if "sort" in query_obj:
                mango_query["sort"] = query_obj["sort"]

            r = httpx.post(
                f"{host}/{target_db}/_find",
                json=mango_query,
                auth=(user, password),
                timeout=30,
            )
            r.raise_for_status()
            result = r.json()
            docs = result.get("docs", [])
            # Clean system fields
            cleaned = []
            for doc in docs:
                cleaned.append({k: v for k, v in doc.items() if not k.startswith("_")})
            return cleaned
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid CouchDB query JSON")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"CouchDB Query Error: {str(e)}")
