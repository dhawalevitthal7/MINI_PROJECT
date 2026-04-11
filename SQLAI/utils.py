import hashlib


def get_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def get_dialect_name(db_url: str) -> str:
    """Detect the database dialect from a connection URL."""
    url_lower = db_url.lower()
    if "postgres" in url_lower:
        return "postgresql"
    if "mysql" in url_lower:
        return "mysql"
    if "oracle" in url_lower:
        return "oracle"
    if "mongodb" in url_lower or "mongo" in url_lower:
        return "mongodb"
    if "couchdb" in url_lower or ":5984" in url_lower:
        return "couchdb"
    return "sql"


def is_sql_db(dialect: str) -> bool:
    """Check if a dialect is a SQL-based database."""
    return dialect in ("postgresql", "mysql", "oracle", "sql")


def is_nosql_db(dialect: str) -> bool:
    """Check if a dialect is a NoSQL database."""
    return dialect in ("mongodb", "couchdb")


def get_query_language(dialect: str) -> str:
    """Return the query language label for a given dialect."""
    mapping = {
        "postgresql": "SQL",
        "mysql": "SQL",
        "oracle": "SQL",
        "sql": "SQL",
        "mongodb": "MongoDB Aggregation",
        "couchdb": "CouchDB Mango",
    }
    return mapping.get(dialect, "SQL")
