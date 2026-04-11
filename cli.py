"""
QueryVista CLI Wrapper
Single Entry Point to run all migration pipelines + SQLAI dual-DB agent.
"""
import os
import sys
import json
import time
import subprocess
import threading
import webbrowser
import urllib.parse

from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from pipelines.mysql_to_mongo import MySQLToMongoPipeline
from pipelines.mysql_to_couchdb import MySQLToCouchDBPipeline
from pipelines.postgres_to_mongo import PostgresToMongoPipeline
from pipelines.postgres_to_couchdb import PostgresToCouchDBPipeline
from pipelines.mongo_to_mysql import MongoToMySQLPipeline
from pipelines.mongo_to_couchdb import MongoToCouchDBPipeline
from pipelines.couchdb_to_mysql import CouchDBToMySQLPipeline
from pipelines.couchdb_to_postgres import CouchDBToPostgresPipeline
from pipelines.base import generate_migration_plan

load_dotenv()

PIPELINES = {
    1: ("mysql", "mongodb", MySQLToMongoPipeline()),
    2: ("mysql", "couchdb", MySQLToCouchDBPipeline()),
    3: ("postgresql", "mongodb", PostgresToMongoPipeline()),
    4: ("postgresql", "couchdb", PostgresToCouchDBPipeline()),
    5: ("mongodb", "mysql", MongoToMySQLPipeline()),
    6: ("mongodb", "couchdb", MongoToCouchDBPipeline()),
    7: ("couchdb", "mysql", CouchDBToMySQLPipeline()),
    8: ("couchdb", "postgresql", CouchDBToPostgresPipeline()),
}

SQLAI_DIR = os.path.join(os.path.dirname(__file__), "SQLAI")


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def _build_sqlai_url(source_url, target_url, source_db_name=None, target_db_name=None, port=8000):
    """Build the SQLAI frontend URL with pre-filled connection parameters."""
    params = {
        "source_url": source_url,
        "target_url": target_url,
    }
    if source_db_name:
        params["source_db"] = source_db_name
    if target_db_name:
        params["target_db"] = target_db_name
    query_string = urllib.parse.urlencode(params)
    return f"http://localhost:{port}/?{query_string}"


def _build_connection_url(db_type, config):
    """Convert a pipeline config dict into a connection URL string for SQLAI."""
    if db_type in ("mysql", "postgresql"):
        return config.get("connection_url", "")
    elif db_type == "mongodb":
        url = config.get("connection_url", "")
        db_name = config.get("database", "")
        # Append db name to mongo URL path if not already present
        if db_name and db_name not in url:
            if "?" in url:
                base, qs = url.split("?", 1)
                return f"{base.rstrip('/')}/{db_name}?{qs}"
            return f"{url.rstrip('/')}/{db_name}"
        return url
    elif db_type == "couchdb":
        host = config.get("host", "http://localhost:5984")
        user = config.get("username", "admin")
        pw = config.get("password", "admin")
        # Build URL: http://user:pass@host:port
        parsed = urllib.parse.urlparse(host)
        return f"{parsed.scheme}://{user}:{pw}@{parsed.hostname}:{parsed.port or 5984}"
    return ""


def launch_sqlai(source_url, target_url, source_db_name=None, target_db_name=None, port=8000):
    """Launch the SQLAI dual-DB agent server and open browser."""
    print("\n" + "=" * 60)
    print("  🤖 Launching QueryVista SQLAI — Dual-DB AI Agent")
    print("=" * 60)
    print(f"  Source DB : {source_url[:80]}...")
    print(f"  Target DB : {target_url[:80]}...")
    print(f"  Server    : http://localhost:{port}")
    print("=" * 60)

    # Determine python executable
    python_exe = sys.executable

    # Start uvicorn server
    server_proc = subprocess.Popen(
        [python_exe, "-m", "uvicorn", "app2:app", "--host", "0.0.0.0", "--port", str(port), "--reload"],
        cwd=SQLAI_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Wait for server to start
    print("\n⏳ Starting SQLAI server...")
    time.sleep(3)

    # Build URL with pre-populated credentials
    url = _build_sqlai_url(source_url, target_url, source_db_name, target_db_name, port)
    print(f"\n🌐 Opening browser: {url[:100]}...")
    webbrowser.open(url)

    print("\n✅ SQLAI is running! Use the browser to explore both databases.")
    print("   Press Ctrl+C or type 'quit' to stop the server.\n")

    try:
        # Stream server output
        while True:
            line = server_proc.stdout.readline()
            if line:
                print(f"  [SQLAI] {line.decode('utf-8', errors='replace').strip()}")
            if server_proc.poll() is not None:
                break
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping SQLAI server...")
        server_proc.terminate()
        server_proc.wait(timeout=5)
        print("   Server stopped.")


def launch_sqlai_standalone():
    """Launch SQLAI standalone from main menu — user enters URLs manually."""
    clear_screen()
    print("=" * 60)
    print("  🤖 QueryVista SQLAI — Standalone Launch")
    print("=" * 60)
    print("\nEnter connection URLs for both databases.\n")

    source_url = input("Source DB URL: ").strip()
    target_url = input("Target DB URL: ").strip()

    if not source_url or not target_url:
        print("Both URLs are required.")
        input("Press enter to return...")
        return

    source_db_name = None
    target_db_name = None

    # Ask for db name if NoSQL
    if any(x in source_url.lower() for x in ["mongo", "couch"]):
        source_db_name = input("Source Database Name (for NoSQL, leave blank if in URL): ").strip() or None
    if any(x in target_url.lower() for x in ["mongo", "couch"]):
        target_db_name = input("Target Database Name (for NoSQL, leave blank if in URL): ").strip() or None

    launch_sqlai(source_url, target_url, source_db_name, target_db_name)


def get_db_config(role, db_type):
    print(f"\n--- Configure {role.title()} Database ({db_type.upper()}) ---")
    config = {}
    if db_type in ("mysql", "postgresql"):
        url = input(f"Enter Connection URL for {db_type} (leave blank for defaults in README): ").strip()
        if not url:
            if db_type == "mysql":
                url = "mysql+pymysql://user1:pass123@localhost:3310/testdb"
            elif db_type == "postgresql":
                url = os.getenv("SQL_URL", "postgresql+psycopg2://postgres:password@localhost:5432/testdb")
        config["connection_url"] = url
    elif db_type == "mongodb":
        url = input("Enter MongoDB URL (leave blank to use MONGO_URL from .env): ").strip()
        if not url:
            url = os.getenv("MONGO_URL")
        db_name = input("Enter Database Name: ").strip()
        config["connection_url"] = url
        config["database"] = db_name
    elif db_type == "couchdb":
        host = input("Enter CouchDB Host (default: http://localhost:5984): ").strip() or "http://localhost:5984"
        user = input("Enter Username (default: admin): ").strip() or "admin"
        pw = input("Enter Password (default: admin123): ").strip() or "admin123"
        config["host"] = host
        config["username"] = user
        config["password"] = pw
    return config


def print_progress(current, total, context):
    print(f" > Progress: [{current}/{total}] tables migrated. Current: {context}", end="\r")


def main():
    while True:
        clear_screen()
        print("=" * 60)
        print("          🚀 QueryVista - AI DB Migration CLI")
        print("=" * 60)
        print("Select a migration pipeline:\n")

        for k, v in PIPELINES.items():
            print(f" {k}. {v[0].upper()} -> {v[1].upper()}")

        print("\n 9. 🤖 Launch SQLAI (Dual-DB AI Agent)")
        print(" 0. Exit")

        choice = input("\nEnter your choice (0-9): ").strip()
        if choice == "0":
            print("Goodbye!")
            break

        if choice == "9":
            launch_sqlai_standalone()
            continue

        if not choice.isdigit() or int(choice) not in PIPELINES:
            print("Invalid choice, press enter to try again.")
            input()
            continue

        source_type, target_type, pipeline = PIPELINES[int(choice)]
        print(f"\nSelected Pipeline: {source_type.upper()} -> {target_type.upper()}")

        # 1. Configuration
        source_config = get_db_config("source", source_type)
        target_config = get_db_config("target", target_type)

        # 2. Test Connection
        print("\nTesting Source Connection...")
        source_res = pipeline.test_source_connection(source_config)
        if not source_res["success"]:
            print(f"Source connection failed: {source_res['message']}")
            input("Press enter to return to menu...")
            continue
        print("Source connection OK.")

        print("Testing Target Connection...")
        target_res = pipeline.test_target_connection(target_config)
        if not target_res["success"]:
            print(f"Target connection failed: {target_res['message']}")
            input("Press enter to return to menu...")
            continue
        print("Target connection OK.")

        # 3. Extract Schema
        print(f"\nExtracting schema from {source_type.upper()}...")
        try:
            schema = pipeline.extract_schema(source_config)
            print(f"Successfully extracted {len(schema)} entities.")
        except Exception as e:
            print(f"Error extracting schema: {e}")
            input("Press enter to return to menu...")
            continue

        # 4. Generate AI Plan
        print(f"\nCalling Azure OpenAI (GPT-4o) to architect the migration from {source_type} to {target_type}...")
        try:
            plan = generate_migration_plan(source_type, target_type, json.dumps(schema, default=str))
            print("\n" + "=" * 40 + " AI MIGRATION PLAN " + "=" * 40)
            print(json.dumps(plan, indent=2))
            print("=" * 99)
        except Exception as e:
            print(f"Error generating plan: {e}")
            input("Press enter to return to menu...")
            continue

        # 5. Review (HITL)
        print("\nReview the plan above.")
        while True:
            action = input("Options: (1) Approve & Execute, (2) Update Plan, (3) Cancel. Choose (1/2/3): ").strip()
            if action == "2":
                feedback = input("Enter your feedback for the AI: ")
                print("\nUpdating plan based on your feedback...")
                try:
                    plan = generate_migration_plan(
                        source_type, target_type,
                        json.dumps(schema, default=str),
                        feedback=feedback,
                        existing_plan=json.dumps(plan),
                    )
                    print("\n" + "=" * 40 + " UPDATED MIGRATION PLAN " + "=" * 40)
                    print(json.dumps(plan, indent=2))
                    print("=" * 99)
                except Exception as e:
                    print(f"Error updating plan: {e}")
            elif action in ("1", "3"):
                break

        if action == "3":
            print("\nMigration cancelled.")
            input("Press enter to return to menu...")
            continue

        # 6. Execute Migration
        print("\nExecuting migration. Please wait...")
        start_time = time.time()
        migration_success = False
        try:
            result = pipeline.execute(source_config, target_config, plan, on_progress=print_progress)
            print("\n\n" + "=" * 60)
            print("✅ Migration Completed Successfully!")
            print(f"   Time taken: {time.time() - start_time:.2f} seconds")
            print(f"   Tables migrated: {len(result.get('tables_migrated', []))}")
            print(f"   Total rows inserted: {result.get('total_rows', 0)}")
            if result.get("errors"):
                print("   Errors encountered:")
                for err in result["errors"]:
                    print(f"    - Table {err['table']}: {err['error']}")
            print("=" * 60)
            migration_success = True
        except Exception as e:
            print(f"\n❌ Error during execution: {e}")

        # 7. Post-Migration: Launch SQLAI
        if migration_success:
            print("\n" + "─" * 60)
            print("  Migration complete! You can now explore both databases")
            print("  with the SQLAI Dual-DB AI Agent.")
            print("─" * 60)
            launch_choice = input("\n🤖 Launch SQLAI to explore both DBs? (y/n): ").strip().lower()
            if launch_choice in ("y", "yes", ""):
                source_url = _build_connection_url(source_type, source_config)
                target_url = _build_connection_url(target_type, target_config)
                source_db_name = source_config.get("database")
                target_db_name = target_config.get("database")
                launch_sqlai(source_url, target_url, source_db_name, target_db_name)
        else:
            input("\nPress enter to return to the main menu...")


if __name__ == "__main__":
    main()

