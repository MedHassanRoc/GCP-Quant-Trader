import json, os
from google.cloud import bigquery

PROJECT = os.environ["PROJECT_ID"]
DATASET = os.environ["DATASET"] + "_dq"
TABLE   = "freshness_log"

client = bigquery.Client(project=PROJECT)
table_id = f"{PROJECT}.{DATASET}.{TABLE}"

# Create table if missing
schema = [
    bigquery.SchemaField("source_name","STRING"),
    bigquery.SchemaField("max_loaded_at","TIMESTAMP"),
    bigquery.SchemaField("generated_at","TIMESTAMP"),
    bigquery.SchemaField("status","STRING"),
    bigquery.SchemaField("max_loaded_at_time_ago_in_s","INT64"),
    bigquery.SchemaField("criteria", "JSON"),
]
client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)

with open("target/sources.json","r",encoding="utf-8") as f:
    payload = json.load(f)

rows = []
gen_ts = payload["generated_at"]
for src in payload["results"]:
    rows.append({
        "source_name": src["unique_id"],
        "max_loaded_at": src.get("max_loaded_at"),
        "generated_at": gen_ts,
        "status": src["status"],
        "max_loaded_at_time_ago_in_s": src.get("max_loaded_at_time_ago_in_s"),
        "criteria": json.dumps(src.get("criteria", {})),
    })

errors = client.insert_rows_json(table_id, rows)
if errors: raise RuntimeError(errors)
print(f"Loaded {len(rows)} rows into {table_id}")
