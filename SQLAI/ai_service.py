"""
AI Service — Azure OpenAI Integration
Handles all LLM calls for query generation, schema analysis, and visualization.
"""

import json
import os
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


class AIService:
    def __init__(self):
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_ENDPOINT", ""),
            api_key=os.getenv("AZURE_API_KEY", ""),
            api_version=os.getenv("AZURE_API_VERSION", "2024-12-01-preview"),
        )
        self.model_name = os.getenv("DEPLOYMENT_NAME", "gpt-4o")

    def ai_call(self, system_instruction: str, user_content: str) -> str:
        """Generic Azure OpenAI chat completion call."""
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.2,
                max_tokens=4096,
            )
            text = response.choices[0].message.content.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                text = text.split("\n", 1)[-1] if "\n" in text else text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()
            return text
        except Exception as e:
            print(f"[ERROR] Azure OpenAI API Error: {str(e)}")
            return ""

    # Alias for backward compatibility
    def gemini_call(self, system_instruction: str, user_content: str) -> str:
        return self.ai_call(system_instruction, user_content)

    def validate_sql_safety(self, sql_query: str, safe_mode: bool) -> bool:
        if not sql_query:
            return False
        if not safe_mode:
            return True
        forbidden = ["insert", "update", "delete", "drop", "alter", "truncate", "grant", "revoke"]
        return not any(word in sql_query.lower() for word in forbidden)

    def fix_sql(self, original_sql: str, error_message: str, schema_str: str, dialect: str) -> str:
        system_instruction = f"You are a {dialect.upper()} Expert. Fix the provided query based on the error message."
        user_content = (
            f"Schema: {schema_str}\nOriginal Query: {original_sql}\n"
            f"Error: {error_message}\nProvide ONLY the corrected raw query. No markdown."
        )
        return self.ai_call(system_instruction, user_content)

    def fix_nosql_query(self, original_query: str, error_message: str, schema_str: str, dialect: str) -> str:
        """Fix a failed MongoDB/CouchDB query using AI."""
        if dialect == "mongodb":
            system_instruction = f"""You are a MongoDB Query Expert. Fix the provided query based on the error message.
Schema: {schema_str}
Rules:
- Return a JSON object with keys: "collection" (string), and either "pipeline" (array) or "filter" (object with optional "projection", "sort", "limit").
- Return ONLY valid JSON. No markdown, no explanation."""
        elif dialect == "couchdb":
            system_instruction = f"""You are a CouchDB Mango Query Expert. Fix the provided query based on the error message.
Schema: {schema_str}
Rules:
- Return a JSON object with keys: "database" (string), "selector" (Mango selector), and optionally "fields", "sort", "limit".
- Return ONLY valid JSON. No markdown, no explanation."""
        else:
            return ""

        user_content = (
            f"Original Query: {original_query}\n"
            f"Error: {error_message}\n"
            f"Fix the query and return ONLY the corrected JSON."
        )
        return self.ai_call(system_instruction, user_content)

