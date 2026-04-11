import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


class Settings:
    AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT", "")
    AZURE_API_KEY = os.getenv("AZURE_API_KEY", "")
    AZURE_API_VERSION = os.getenv("AZURE_API_VERSION", "2024-12-01-preview")
    DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME", "gpt-4o")
    # Set CACHE_DB_URL in a repo-root `.env` file (never commit credentials).
    CACHE_DB_URL = os.getenv("CACHE_DB_URL", "")


settings = Settings()
