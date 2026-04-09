import json
import logging
import decimal
import uuid
import datetime
from typing import Any, Dict, List, Optional

import pymongo
from bson import ObjectId
import sqlalchemy
from sqlalchemy import create_engine, text, Column, Table, MetaData
from sqlalchemy import String, Integer, Float, Boolean, DateTime, Text

class MigrationMode:
    REPLACE = "replace"
    APPEND  = "append"
    UPSERT  = "upsert"

class MongoToSqlETLEngine:
    BATCH_SIZE = 500
    MIN_MYSQL_JSON_VERSION = (5, 7)
    _MAX_SAFE_VARCHAR = 255

    TYPE_MAP = {
        "VARCHAR(64)":  lambda: String(64),
        "VARCHAR(255)": lambda: String(255),
        "VARCHAR(100)": lambda: String(100),
        "VARCHAR(20)":  lambda: String(20),
        "TEXT":         lambda: Text(),
        "BIGINT":       lambda: sqlalchemy.BigInteger(),
        "INT":          lambda: sqlalchemy.Integer(),
        "INTEGER":      lambda: sqlalchemy.Integer(),
        "DOUBLE":       lambda: sqlalchemy.Float(),
        "FLOAT":        lambda: sqlalchemy.Float(),
        "BOOLEAN":      lambda: sqlalchemy.Boolean(),
        "DATETIME":     lambda: sqlalchemy.DateTime(),
        "DATE":         lambda: sqlalchemy.Date(),
        "JSON":         None,
        "BLOB":         lambda: sqlalchemy.LargeBinary(),
    }

    def __init__(self, mongo_url: str, mongo_db_name: str, sql_url: str, mode: str = MigrationMode.UPSERT):
        self.mongo_client = pymongo.MongoClient(mongo_url)
        self.mongo_db = self.mongo_client[mongo_db_name]
        
        # Dialect fixes
        if sql_url.startswith("postgresql://"):
            sql_url = sql_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        elif sql_url.startswith("mysql://"):
            sql_url = sql_url.replace("mysql://", "mysql+pymysql://", 1)
            
        self.mysql_engine = create_engine(sql_url, echo=False)
        self.metadata = MetaData()
        self.mode = mode
        
        self.TYPE_MAP["JSON"] = self._resolve_json_type()
        
    def _detect_mysql_version(self) -> tuple:
        try:
            with self.mysql_engine.connect() as conn:
                row = conn.execute(text("SELECT VERSION()")).fetchone()
            version_str = row[0]
            if version_str.upper().startswith("POSTGRESQL"):
                return (99, 0)
            numeric_part = version_str.split("-")[0].split(".")
            return (int(numeric_part[0]), int(numeric_part[1]))
        except Exception:
            return (0, 0)

    def _resolve_json_type(self):
        version = self._detect_mysql_version()
        if version >= self.MIN_MYSQL_JSON_VERSION:
            return lambda: sqlalchemy.JSON()
        return lambda: Text()

    def _get_sa_type(self, type_str: str):
        upper = type_str.upper().strip()
        if upper in self.TYPE_MAP:
            return self.TYPE_MAP[upper]()
        if upper.startswith("VARCHAR"):
            try:
                length = int(upper.replace("VARCHAR(", "").replace(")", ""))
                if length > self._MAX_SAFE_VARCHAR:
                    return Text()
                return String(length)
            except Exception:
                return Text()
        return Text()

    def _create_table(self, table_name: str, job: Dict):
        columns = []
        col_map = job.get("column_mapping", {})
        flatten_rules = {f["mongo_field"]: f["fields"] for f in job.get("flatten", [])}

        for mongo_field, mapping in col_map.items():
            if mapping is None:
                continue
            mysql_col  = mapping["mysql_col"]
            mysql_type = mapping.get("mysql_type", "TEXT")
            is_pk      = mapping.get("primary_key", False)
            nullable   = mapping.get("nullable", True)
            columns.append(Column(
                mysql_col,
                self._get_sa_type(mysql_type),
                primary_key=is_pk,
                nullable=(not is_pk) and nullable
            ))

        for mongo_field, subfields in flatten_rules.items():
            for subname, subtype in subfields.items():
                columns.append(Column(
                    f"{mongo_field}_{subname}",
                    self._get_sa_type(subtype),
                    nullable=True
                ))

        if not columns:
            return None

        tbl = Table(table_name, self.metadata, *columns, extend_existing=True)

        with self.mysql_engine.begin() as conn:
            if self.mode == MigrationMode.REPLACE:
                conn.execute(text(f"DROP TABLE IF EXISTS {self._quote(table_name)}"))
                self.metadata.create_all(self.mysql_engine)
            else:
                self.metadata.create_all(self.mysql_engine, checkfirst=True)

        return tbl

    def _serialize_value(self, val: Any, mysql_type: str) -> Any:
        if val is None:
            return None
        upper_type = mysql_type.upper()
        if isinstance(val, ObjectId):
            return str(val)
        if isinstance(val, uuid.UUID):
            return str(val)
        if isinstance(val, decimal.Decimal):
            return float(val)
        if isinstance(val, datetime.datetime):
            return val.replace(tzinfo=None)
        if isinstance(val, datetime.date):
            return val
        if isinstance(val, bytes):
            if "BLOB" in upper_type:
                return val
            try:
                return val.decode("utf-8")
            except Exception:
                return None
        if isinstance(val, (list, dict)):
            if "JSON" in upper_type:
                return json.dumps(val, default=str)
            return json.dumps(val, default=str)[:65535]
        if isinstance(val, bool):
            return val if self.mysql_engine.dialect.name != "mysql" else int(val)
        return val

    def _transform_document(self, doc: Dict, job: Dict) -> Optional[Dict]:
        col_map = job.get("column_mapping", {})
        flatten_rules = {f["mongo_field"]: f["fields"] for f in job.get("flatten", [])}
        row = {}

        for mongo_field, mapping in col_map.items():
            if mapping is None:
                continue
            mysql_col  = mapping["mysql_col"]
            mysql_type = mapping.get("mysql_type", "TEXT")
            raw_val    = doc.get(mongo_field)

            if mongo_field in flatten_rules:
                if isinstance(raw_val, dict):
                    for subname, subtype in flatten_rules[mongo_field].items():
                        row[f"{mongo_field}_{subname}"] = self._serialize_value(raw_val.get(subname), subtype)
                continue

            row[mysql_col] = self._serialize_value(raw_val, mysql_type)

        for mongo_field, subfields in flatten_rules.items():
            if mongo_field in col_map:
                continue
            raw_val = doc.get(mongo_field)
            if isinstance(raw_val, dict):
                for subname, subtype in subfields.items():
                    row[f"{mongo_field}_{subname}"] = self._serialize_value(raw_val.get(subname), subtype)

        return row if row else None

    def _pk_column(self, job: Dict) -> Optional[str]:
        for mapping in job.get("column_mapping", {}).values():
            if mapping and mapping.get("primary_key"):
                return mapping["mysql_col"]
        return None

    def _quote(self, name: str) -> str:
        dialect = self.mysql_engine.dialect.name
        if dialect == "mysql":
            return f"`{name}`"
        return f'"{name}"'

    def _flush_batch(self, tbl: Table, batch: List[Dict], pk_col: Optional[str]) -> tuple:
        if not batch:
            return 0, 0

        dialect = self.mysql_engine.dialect.name

        def _build_upsert_sql(sample_row: Dict) -> text:
            keys        = list(sample_row.keys())
            non_pk_cols = [k for k in keys if k != pk_col]
            q           = self._quote
            tbl_q       = q(tbl.name)
            col_list    = ", ".join(q(k) for k in keys)
            val_list    = ", ".join(f":{k}" for k in keys)

            if dialect == "mysql":
                if non_pk_cols:
                    update_list = ", ".join(f"{q(k)}=VALUES({q(k)})" for k in non_pk_cols)
                    return text(f"INSERT INTO {tbl_q} ({col_list}) VALUES ({val_list}) ON DUPLICATE KEY UPDATE {update_list}")
                else:
                    return text(f"INSERT INTO {tbl_q} ({col_list}) VALUES ({val_list}) ON DUPLICATE KEY UPDATE {q(keys[0])}=VALUES({q(keys[0])})")
            else:
                if non_pk_cols:
                    update_list = ", ".join(f"{q(k)}=EXCLUDED.{q(k)}" for k in non_pk_cols)
                    return text(f"INSERT INTO {tbl_q} ({col_list}) VALUES ({val_list}) ON CONFLICT ({q(pk_col)}) DO UPDATE SET {update_list}")
                else:
                    return text(f"INSERT INTO {tbl_q} ({col_list}) VALUES ({val_list}) ON CONFLICT DO NOTHING")

        def _insert_one(conn, row: Dict) -> bool:
            try:
                if self.mode == MigrationMode.UPSERT and pk_col:
                    conn.execute(_build_upsert_sql(row), [row])
                else:
                    conn.execute(tbl.insert(), [row])
                return True
            except Exception as row_err:
                return False

        try:
            with self.mysql_engine.begin() as conn:
                if self.mode == MigrationMode.UPSERT and pk_col:
                    conn.execute(_build_upsert_sql(batch[0]), batch)
                else:
                    conn.execute(tbl.insert(), batch)
            return len(batch), 0
        except Exception:
            pass

        inserted = skipped = 0
        for row in batch:
            with self.mysql_engine.begin() as conn:
                if _insert_one(conn, row):
                    inserted += 1
                else:
                    skipped += 1

        return inserted, skipped

    def execute_plan(self, plan: List[Dict], on_progress=None) -> Dict:
        results = {"tables_migrated": [], "errors": [], "total_rows": 0}

        for i, mapping in enumerate(plan):
            source_coll = mapping["source"]
            target_table = mapping["target"]
            
            job = {
                "mongo_collection": source_coll,
                "mysql_table": target_table,
                "column_mapping": {}
            }
            
            for f in mapping.get("field_mappings", []):
                source_field = f.get("source_field")
                target_field = f.get("target_field")
                f_type = f.get("type", "TEXT")
                # Ensure primary keys are caught (often _id or id)
                is_pk = source_field == "_id" or target_field == "id" or f.get("is_primary_key", False)
                
                job["column_mapping"][source_field] = {
                    "mysql_col": target_field,
                    "mysql_type": f_type.upper(),
                    "primary_key": is_pk,
                    "nullable": not is_pk
                }

            pk_col = self._pk_column(job)
            
            try:
                tbl = self._create_table(target_table, job)
                if tbl is None:
                    continue

                collection = self.mongo_db[source_coll]
                total = collection.estimated_document_count()
                cursor = collection.find({}).batch_size(1000)

                batch = []
                inserted = skipped = 0

                for doc in cursor:
                    row = self._transform_document(doc, job)
                    if row:
                        batch.append(row)
                    if len(batch) >= self.BATCH_SIZE:
                        ok, skip = self._flush_batch(tbl, batch, pk_col)
                        inserted += ok
                        skipped += skip
                        batch = []

                ok, skip = self._flush_batch(tbl, batch, pk_col)
                inserted += ok
                skipped += skip
                
                results["tables_migrated"].append({
                    "source": source_coll,
                    "target": target_table,
                    "rows": inserted
                })
                results["total_rows"] += inserted
                
                if on_progress:
                    on_progress(i + 1, len(plan), source_coll)

            except Exception as e:
                results["errors"].append({"table": source_coll, "error": str(e)})

        return results
