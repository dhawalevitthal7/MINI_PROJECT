from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


# ─── Single DB Models (backward compat) ─────────────────────────────────────

class DBConnectionRequest(BaseModel):
    db_url: str = Field(..., description="Connection String")


class UserRequest(BaseModel):
    db_url: str = Field(..., description="Connection String")
    query: str = Field(..., description="Natural language question")
    safe_mode: bool = Field(True, description="True = SELECT only.")


class AnalysisResponse(BaseModel):
    sql_query: str
    message: Optional[str] = "Query executed successfully."
    data_preview: Optional[List[Dict[str, Any]]] = None
    graphs_base64: List[str] = []
    csv_base64: Optional[str] = None
    error: Optional[str] = None


class TableDetailsResponse(BaseModel):
    table_name: str
    row_count: int
    columns: List[str]
    first_10: List[Dict[str, Any]]
    last_10: List[Dict[str, Any]]


class DashboardChart(BaseModel):
    title: str
    description: str
    graph_base64: str


class DashboardResponse(BaseModel):
    charts: List[DashboardChart]
    error: Optional[str] = None


class OptimizeRequest(BaseModel):
    db_url: str = Field(..., description="Connection String")
    query: str = Field(..., description="The SQL query to analyze")


class OptimizeResponse(BaseModel):
    original_query: str
    optimized_query: str
    explanation: str
    difference_score: int = Field(..., description="0-100 score of how much changed")


class PaginationRequest(DBConnectionRequest):
    page: int = 1
    limit: int = 100


class PaginationResponse(BaseModel):
    data: List[Dict[str, Any]]
    total_rows: int
    page: int
    total_pages: int
    error: Optional[str] = None


# ─── Dual DB Models ──────────────────────────────────────────────────────────

class DualDBConnectionRequest(BaseModel):
    source_url: str = Field(..., description="Source DB connection string")
    target_url: str = Field(..., description="Target DB connection string")
    source_db_name: Optional[str] = Field(None, description="Database name for NoSQL sources")
    target_db_name: Optional[str] = Field(None, description="Database name for NoSQL targets")


class DualDBSchemaResponse(BaseModel):
    source_dialect: str
    target_dialect: str
    source_tables: List[str]
    target_tables: List[str]
    source_schema: Optional[Dict[str, Any]] = None
    target_schema: Optional[Dict[str, Any]] = None
    diff: Optional[Dict[str, Any]] = None


class DualQueryRequest(BaseModel):
    source_url: str
    target_url: str
    source_db_name: Optional[str] = None
    target_db_name: Optional[str] = None
    query: str = Field(..., description="Natural language question")
    safe_mode: bool = True


class SingleDBQueryResult(BaseModel):
    dialect: str
    query_text: str
    query_language: str
    data_preview: Optional[List[Dict[str, Any]]] = None
    row_count: int = 0
    message: Optional[str] = None
    error: Optional[str] = None
    csv_base64: Optional[str] = None
    graphs_base64: List[str] = []


class DualQueryResponse(BaseModel):
    natural_language: str
    source_result: Optional[SingleDBQueryResult] = None
    target_result: Optional[SingleDBQueryResult] = None
    error: Optional[str] = None


class DualTableDetailRequest(BaseModel):
    db_url: str
    db_name: Optional[str] = None
    page: int = 1
    limit: int = 100
