import sqlite3
import yaml
from pathlib import Path
from config import DB_PATH

SCHEMA_PATH = Path(__file__).parent / "schema" / "system_schema.yaml"

def generate_schema_yaml():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cur.fetchall()]

    schema = {"tables": []}

    for table in tables:
        cur.execute(f"PRAGMA table_info({table})")
        cols = cur.fetchall()
        columns = []
        for cid, name, col_type, notnull, dflt_value, pk in cols:
            columns.append({
                "name": name,
                "type": col_type or "TEXT",
                "description": ""  # you can fill manually later if you want
            })
        schema["tables"].append({
            "name": table,
            "description": "",
            "columns": columns,
        })

    conn.close()

    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SCHEMA_PATH, "w") as f:
        yaml.dump(schema, f, sort_keys=False)

    print(f"Written schema to {SCHEMA_PATH}")

if __name__ == "__main__":
    generate_schema_yaml()


