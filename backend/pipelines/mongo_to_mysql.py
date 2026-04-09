"""
Pipeline: MongoDB → MySQL
Uses the shared robust ETL engine for MongoDB -> SQL migrations.
"""

from typing import Any, Dict
import pymongo
from sqlalchemy import create_engine, text

from .base import BasePipeline, extract_mongo_schema
from .mongo_sql_etl import MongoToSqlETLEngine, MigrationMode

class MongoToMySQLPipeline(BasePipeline):
    source_type = "mongodb"
    target_type = "mysql"

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
            engine = create_engine(config["connection_url"])
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            return {"success": True, "message": "MySQL connection successful"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def extract_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return extract_mongo_schema(config["connection_url"], config["database"])

    def execute(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        plan: Dict[str, Any],
        on_progress=None,
    ) -> Dict[str, Any]:
        
        mongo_url = source_config["connection_url"]
        mongo_db = source_config["database"]
        mysql_url = target_config["connection_url"]
        
        mappings = plan.get("tables", plan.get("collections", []))
        
        engine = MongoToSqlETLEngine(
            mongo_url=mongo_url,
            mongo_db_name=mongo_db,
            sql_url=mysql_url,
            mode=MigrationMode.UPSERT
        )
        
        return engine.execute_plan(mappings, on_progress)
