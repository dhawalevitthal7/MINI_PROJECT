import os
import json
import uuid
import subprocess
import traceback
from typing import Dict, Any

from .base import get_ai_client, get_pipeline_logger, DEPLOYMENT

logger = get_pipeline_logger("DynamicExecutor")


def execute_dynamic_migration(
    source_type: str,
    target_type: str,
    source_config: Dict[str, Any],
    target_config: Dict[str, Any],
    plan: Dict[str, Any],
    on_progress: Any = None,
) -> Dict[str, Any]:
    """Generates a python script via LLM based on the plan and executes it."""
    
    script_id = str(uuid.uuid4())
    script_path = os.path.join(os.path.dirname(__file__), f"__dynamic_{script_id}.py")
    
    logger.info(f"Generating dynamic python execution script to {script_path}")

    client = get_ai_client()

    system_prompt = f"""You are an elite data engineer. 
You must generate a completely standalone, robust, executable Python 3 script that performs a data migration from {source_type.upper()} to {target_type.upper()}.

Here is the exact mapping blueprint you must follow:
{json.dumps(plan, indent=2)}

Source connection URL: {source_config.get('connection_url', '')}
Target connection URL: {target_config.get('connection_url', '')}
Target Database / Host (if NoSQL): {target_config.get('database', target_config.get('host', ''))}
Username: {target_config.get('username', '')}
Password: {target_config.get('password', '')}

REQUIREMENTS:
1. Connect to both databases.
2. Iterate through the mapped tables/collections.
3. Extract data from the source.
4. Rename fields EXACTLY as specified in the blueprint's "field_mappings".
5. Handle timestamp, datetime, Decimal string conversions safely.
6. Insert the formatted records into the target database.
7. Print a final JSON line at the very end of the execution EXACTLY in this format: 
   FINAL_RESULT: {{"tables_migrated": [{{"source": "X", "target": "Y", "rows": 100}}], "errors": [], "total_rows": 100}}
8. Return ONLY valid python code. Start your response with `import` and do not wrap it in markdown block quotes (no ```python).
"""

    messages = [{"role": "system", "content": system_prompt}]

    response = client.chat.completions.create(
        model=DEPLOYMENT,
        messages=messages,
        temperature=0.1,
        max_tokens=4000,
    )

    code = response.choices[0].message.content.strip()
    if code.startswith("```"):
        import re
        code = re.sub(r"^```(?:python)?\s*", "", code)
        code = re.sub(r"\s*```$", "", code)

    # Write the script
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(code)

    logger.info(f"Executing dynamic script {script_path}...")
    
    final_result = {"tables_migrated": [], "errors": [], "total_rows": 0, "dynamic_script": True}

    try:
        if on_progress:
            on_progress(1, 1, "Executing dynamic AI Python script...")
            
        process = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        output = process.stdout
        stderr = process.stderr

        logger.info(output)
        if process.returncode != 0:
            logger.error(f"Dynamic script failed: {stderr}")
            final_result["errors"].append({"table": "DynamicScript", "error": stderr})
        
        # Parse final result
        for line in output.splitlines():
            if line.startswith("FINAL_RESULT:"):
                try:
                    res_json = json.loads(line.replace("FINAL_RESULT:", "").strip())
                    final_result["tables_migrated"] = res_json.get("tables_migrated", [])
                    final_result["total_rows"] = res_json.get("total_rows", 0)
                except Exception as e:
                    logger.error(f"Failed to parse FINAL_RESULT: {e}")

    except Exception as e:
        logger.error(f"Dynamic script execution error: {str(e)}")
        final_result["errors"].append({"table": "DynamicExecution", "error": str(e), "trace": traceback.format_exc()})
    finally:
        # Cleanup script
        if os.path.exists(script_path):
            os.remove(script_path)

    return final_result
